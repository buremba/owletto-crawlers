/**
 * HackerNews Crawler
 *
 * Uses Algolia HN Search API for efficient story and comment crawling.
 * No authentication required, free to use.
 *
 * Refactored to use ApiPaginatedCrawler for reusable pagination logic.
 */

import { type Static, Type } from '@owletto/sdk';
import TurndownService from 'npm:turndown@7.2.2';
import { logger } from '@owletto/sdk';
import { ApiPaginatedCrawler } from '@owletto/sdk';
import type {
  Checkpoint,
  Content,
  CrawlerOptions,
  CrawlResult,
  Env,
  ParentSourceDefinition,
  SearchResult,
} from '@owletto/sdk';
import { calculateEngagementScore } from '@owletto/sdk';
import { httpClient } from '@owletto/sdk';
import type { PageFetchResult, PaginatedCheckpoint } from '@owletto/sdk';

/**
 * HackerNews-specific options schema
 */
export const HackerNewsOptionsSchema = Type.Object(
  {
    search_query: Type.String({
      description: 'Search term to find HN stories and comments (brand/product name)',
      minLength: 1,
    }),
    content_type: Type.Union(
      [
        Type.Literal('story'),
        Type.Literal('comment'),
        Type.Literal('ask_hn'),
        Type.Literal('show_hn'),
      ],
      {
        description:
          'Content type: ONE type per crawler for reliable checkpointing. Create separate crawlers for different types.',
      }
    ),
    story_ids: Type.Optional(
      Type.Array(Type.Number(), {
        description: 'Track specific story IDs (optional, only used with story content types)',
        minItems: 1,
      })
    ),
    lookback_days: Type.Optional(
      Type.Number({
        description:
          'Number of days to look back for historical data. Default: 365 (1 year). Maximum: 730 (2 years).',
        minimum: 1,
        maximum: 730,
        default: 365,
      })
    ),
  },
  {
    description: 'HackerNews crawler options - each crawler tracks ONE content type',
    $id: 'HackerNewsOptions',
  }
);

export type HackerNewsOptions = Static<typeof HackerNewsOptionsSchema>;

/**
 * HackerNews-specific checkpoint structure
 */
export interface HackerNewsCheckpoint extends PaginatedCheckpoint {
  last_created_at?: number; // Unix timestamp of last item processed
  processed_story_ids?: number[]; // Track processed stories (for story types only)
}

/**
 * Algolia HN API response hit structure
 */
interface AlgoliaHit {
  objectID: string;
  created_at: string;
  created_at_i: number; // Unix timestamp
  author: string;
  title?: string; // Stories have title
  story_text?: string; // Story content
  comment_text?: string; // Comment content
  url?: string; // External URL for stories
  points?: number; // Story points
  num_comments?: number; // Number of comments
  story_id?: number; // Parent story ID for comments
  parent_id?: number; // Parent comment ID for nested comments
  _tags: string[]; // ['story', 'author_xyz', 'story_123', etc]
}

/**
 * Algolia HN API response structure
 */
interface AlgoliaResponse {
  hits: AlgoliaHit[];
  nbHits: number;
  page: number;
  nbPages: number;
  hitsPerPage: number;
}

/**
 * HackerNews crawler implementation using ApiPaginatedCrawler
 */
export class HackerNewsCrawler extends ApiPaginatedCrawler<
  AlgoliaHit,
  AlgoliaResponse,
  HackerNewsCheckpoint
> {
  readonly type = 'hackernews';
  readonly displayName = 'Hacker News';
  readonly crawlerType = 'search' as const;
  readonly optionsSchema = HackerNewsOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.6,
    inverse_rating_weight: 0.0,
    content_length_weight: 0.4,
    platform_weight: 1.0,
  };
  readonly defaultScoringFormula = `
    PERCENT_RANK() OVER (PARTITION BY f.source_id ORDER BY f.score) * 100 * 0.6 +
    LEAST(f.content_length / 20.0, 100) * 0.4
  `;

  private readonly BASE_URL = 'https://hn.algolia.com/api/v1';
  private readonly ENGAGEMENT_THRESHOLD = 50;
  private readonly CONTENT_FETCH_TIMEOUT = 5000;
  private turndownService: TurndownService;

  constructor() {
    super();
    this.turndownService = new TurndownService({
      headingStyle: 'atx',
      codeBlockStyle: 'fenced',
    });
  }

  protected getPaginationConfig() {
    return {
      maxPages: 50,
      pageSize: 100,
      rateLimitMs: 1000, // 1 request per second
      incrementalCheckpoint: false,
    };
  }

  getRateLimit() {
    return {
      requests_per_minute: 60,
      recommended_interval_ms: 1000,
    };
  }

  validateOptions(options: HackerNewsOptions): string | null {
    const schemaError = this.validateWithSchema(options);
    if (schemaError) return schemaError;

    if (options.search_query.trim().length === 0) {
      return 'search_query cannot be empty or whitespace only';
    }
    return null;
  }

  urlFromOptions(options: HackerNewsOptions): string {
    return `https://hn.algolia.com/?query=${encodeURIComponent(options.search_query)}`;
  }

  displayLabelFromOptions(options: HackerNewsOptions): string {
    return `search ${options.content_type} "${options.search_query}"`;
  }

  getParentSourceDefinitions(options: HackerNewsOptions): ParentSourceDefinition[] {
    if (options.content_type !== 'comment') {
      return [];
    }

    const parentOptions: HackerNewsOptions = {
      search_query: options.search_query,
      content_type: 'story',
      lookback_days: options.lookback_days,
    };

    return [
      {
        type: this.type,
        options: parentOptions,
        description: 'Stories',
      },
    ];
  }

  /**
   * Get pagination token from checkpoint (override for HN-specific structure)
   */
  protected getPaginationToken(checkpoint: HackerNewsCheckpoint | null): string | null {
    // HN uses page numbers, so we use last_created_at as the "token" for incremental mode
    // If we have processed_story_ids but no pagination_token, we're in incremental mode
    return checkpoint?.pagination_token ?? null;
  }

  /**
   * Build URL for fetching a page from Algolia HN API
   */
  protected buildPageUrl(cursor: string | null, options: CrawlerOptions): string {
    const hnOptions = options as HackerNewsOptions;
    const lookbackDays = hnOptions.lookback_days || 365;
    const lookbackTimestamp = Math.floor((Date.now() - lookbackDays * 24 * 60 * 60 * 1000) / 1000);

    // Determine tag based on content_type
    let tag = 'story';
    if (hnOptions.content_type === 'comment') {
      tag = 'comment';
    } else if (hnOptions.content_type === 'ask_hn') {
      tag = 'ask_hn';
    } else if (hnOptions.content_type === 'show_hn') {
      tag = 'show_hn';
    }

    const numericFilters = [`created_at_i>${lookbackTimestamp}`];
    const page = cursor ? parseInt(cursor, 10) : 0;

    return `${this.BASE_URL}/search?query=${encodeURIComponent(
      hnOptions.search_query
    )}&tags=${tag}&hitsPerPage=100&page=${page}&numericFilters=${encodeURIComponent(numericFilters.join(','))}`;
  }

  /**
   * Parse Algolia API response
   */
  protected parseResponse(
    response: AlgoliaResponse,
    _options: CrawlerOptions
  ): PageFetchResult<AlgoliaHit> {
    const hasNextPage = response.page < response.nbPages - 1 && response.hits.length > 0;

    return {
      items: response.hits,
      nextToken: hasNextPage ? String(response.page + 1) : null,
      rawCount: response.hits.length,
    };
  }

  /**
   * Get item date from Algolia hit
   */
  protected getItemDate(item: AlgoliaHit): Date {
    return new Date(item.created_at_i * 1000);
  }

  /**
   * Get parent ID for comments
   */
  protected getParentId(item: AlgoliaHit): string | null {
    if (item.parent_id && item.story_id && item.parent_id !== item.story_id) {
      return `hn_comment_${item.parent_id}`;
    }
    if (item.story_id) {
      return `hn_story_${item.story_id}`;
    }
    return null;
  }

  /**
   * Transform Algolia hit to Content format
   */
  protected transformItem(item: AlgoliaHit, options: CrawlerOptions): Content {
    const hnOptions = options as HackerNewsOptions;

    if (hnOptions.content_type === 'comment') {
      return this.transformComment(item);
    }
    return this.transformStorySync(item);
  }

  /**
   * Transform comment to Content (synchronous)
   */
  private transformComment(comment: AlgoliaHit): Content {
    const commentId = parseInt(comment.objectID, 10);
    const engagementData = { score: 0 };

    return {
      external_id: `hn_comment_${commentId}`,
      content: comment.comment_text || '',
      author: comment.author,
      url: `https://news.ycombinator.com/item?id=${commentId}`,
      published_at: this.getItemDate(comment),
      score: calculateEngagementScore('hackernews', engagementData),
      metadata: {
        ...engagementData,
        type: 'comment',
        comment_id: commentId,
        parent_type: 'story',
        story_id: comment.story_id,
        parent_id: comment.parent_id,
        created_at_i: comment.created_at_i,
        tags: comment._tags,
      },
    };
  }

  /**
   * Transform story to Content (synchronous version - no external fetch)
   * External content fetching is done in post-processing
   */
  private transformStorySync(story: AlgoliaHit): Content {
    const storyId = parseInt(story.objectID, 10);

    const isAskHN = story._tags.includes('ask_hn');
    const isShowHN = story._tags.includes('show_hn');
    const isPoll = story._tags.includes('poll');

    let storyType = 'story';
    if (isAskHN) storyType = 'ask_hn';
    else if (isShowHN) storyType = 'show_hn';
    else if (isPoll) storyType = 'poll';

    const engagementData = {
      score: story.points || 0,
      reply_count: story.num_comments || 0,
    };

    return {
      external_id: `hn_story_${storyId}`,
      title: story.title || '',
      content: (story.story_text || '').trim(),
      author: story.author,
      url: `https://news.ycombinator.com/item?id=${storyId}`,
      published_at: this.getItemDate(story),
      score: calculateEngagementScore('hackernews', engagementData),
      metadata: {
        ...engagementData,
        type: 'story',
        story_type: storyType,
        tags: story._tags,
        external_url: story.url,
        created_at_i: story.created_at_i,
      },
    };
  }

  /**
   * Create checkpoint with HN-specific fields
   */
  protected createCheckpoint(
    existing: HackerNewsCheckpoint | null,
    latestContent: Content | null,
    nextToken: string | null,
    itemsProcessed: number
  ): HackerNewsCheckpoint {
    const base = super.createCheckpoint(existing, latestContent, nextToken, itemsProcessed);

    return {
      ...base,
      last_created_at: latestContent?.metadata?.created_at_i ?? existing?.last_created_at,
      processed_story_ids: existing?.processed_story_ids || [],
    };
  }

  /**
   * Main pull method - uses base class pagination
   */
  async pull(
    options: HackerNewsOptions,
    checkpoint: HackerNewsCheckpoint | null,
    env: Env,
    updateCheckpointFn?: (checkpoint: Checkpoint) => Promise<void>
  ): Promise<CrawlResult> {
    // Handle specific story IDs case (bypass pagination)
    if (options.story_ids && options.story_ids.length > 0 && options.content_type !== 'comment') {
      return this.pullSpecificStories(options, checkpoint, env);
    }

    // Use base class pagination
    const {
      contents,
      checkpoint: newCheckpoint,
      parentMap,
      nextCrawlRecommendedAt,
    } = await this.paginate(options, checkpoint, env, updateCheckpointFn);

    // Deduplicate and sort
    const seenIds = new Set<string>();
    const uniqueContents = this.deduplicate(contents, seenIds);
    uniqueContents.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

    // Fetch external content for high-engagement stories (async enhancement)
    if (options.content_type !== 'comment') {
      await this.enrichStoriesWithExternalContent(uniqueContents);
    }

    // Update checkpoint with processed story IDs
    const finalCheckpoint: HackerNewsCheckpoint = {
      ...newCheckpoint,
      processed_story_ids:
        options.content_type !== 'comment'
          ? [
              ...(checkpoint?.processed_story_ids || []),
              ...uniqueContents.map((c) => parseInt(c.external_id.replace('hn_story_', ''), 10)),
            ]
          : checkpoint?.processed_story_ids || [],
    };

    return {
      contents: uniqueContents,
      checkpoint: finalCheckpoint,
      metadata: {
        items_found: contents.length,
        items_skipped: contents.length - uniqueContents.length,
        parent_map: parentMap ? Object.fromEntries(parentMap) : undefined,
        next_crawl_recommended_at: nextCrawlRecommendedAt,
      },
    };
  }

  /**
   * Pull specific stories by ID (bypasses pagination)
   */
  private async pullSpecificStories(
    options: HackerNewsOptions,
    checkpoint: HackerNewsCheckpoint | null,
    _env: Env
  ): Promise<CrawlResult> {
    const storyIds = options.story_ids!;
    const contents: Content[] = [];

    for (const id of storyIds) {
      try {
        const url = `${this.BASE_URL}/items/${id}`;
        const story = await httpClient.get(url).json<AlgoliaHit>();
        if (story) {
          contents.push(this.transformStorySync(story));
        }
      } catch (error) {
        logger.error({ error, storyId: id }, 'Failed to fetch HN story');
      }
    }

    await this.enrichStoriesWithExternalContent(contents);

    const newCheckpoint: HackerNewsCheckpoint = {
      updated_at: new Date(),
      last_timestamp: contents[0]?.published_at ?? checkpoint?.last_timestamp,
      total_items_processed: (checkpoint?.total_items_processed || 0) + contents.length,
      processed_story_ids: [...(checkpoint?.processed_story_ids || []), ...storyIds],
    };

    return {
      contents,
      checkpoint: newCheckpoint,
      metadata: {
        items_found: contents.length,
        items_skipped: 0,
      },
    };
  }

  /**
   * Fetch external content for high-engagement stories
   */
  private async enrichStoriesWithExternalContent(contents: Content[]): Promise<void> {
    for (const content of contents) {
      const externalUrl = content.metadata?.external_url as string | undefined;
      const points = content.metadata?.score as number | undefined;

      // Only fetch for high-engagement stories with external URLs and no content
      if (
        !content.content &&
        externalUrl &&
        points &&
        points >= this.ENGAGEMENT_THRESHOLD &&
        !externalUrl.includes('news.ycombinator.com')
      ) {
        const externalContent = await this.fetchExternalContent(externalUrl);
        if (externalContent) {
          content.content = externalContent;
          content.metadata = {
            ...content.metadata,
            fetched_content: true,
            original_url: externalUrl,
          };
          logger.info(
            { storyId: content.external_id, url: externalUrl },
            'Fetched external content for HN story'
          );
        }
        // Rate limit external fetches
        await this.sleep(2000);
      }
    }
  }

  /**
   * Fetch and convert external URL content to markdown
   */
  private async fetchExternalContent(url: string): Promise<string | null> {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.CONTENT_FETCH_TIMEOUT);

      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; HNBot/1.0)',
          Accept: 'text/html,application/xhtml+xml',
        },
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        logger.warn({ url, status: response.status }, 'Failed to fetch external content');
        return null;
      }

      const contentType = response.headers.get('content-type') || '';
      if (!contentType.includes('text/html')) {
        return null;
      }

      const html = await response.text();

      // Clean HTML
      const cleanHtml = html
        .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
        .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
        .replace(/<nav\b[^<]*(?:(?!<\/nav>)<[^<]*)*<\/nav>/gi, '')
        .replace(/<header\b[^<]*(?:(?!<\/header>)<[^<]*)*<\/header>/gi, '')
        .replace(/<footer\b[^<]*(?:(?!<\/footer>)<[^<]*)*<\/footer>/gi, '');

      const markdown = this.turndownService.turndown(cleanHtml);
      const trimmed = markdown.trim().substring(0, 2000);

      return trimmed.length > 100 ? trimmed : null;
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        logger.warn({ url }, 'Content fetch timeout');
      } else {
        logger.warn({ error, url }, 'Error fetching external content');
      }
      return null;
    }
  }

  /**
   * Search HackerNews for stories matching a search term
   */
  async search(searchTerm: string, _env: Env): Promise<SearchResult[]> {
    try {
      const results: SearchResult[] = [];

      // Check for stories
      const storiesUrl = `${this.BASE_URL}/search?query=${encodeURIComponent(
        searchTerm
      )}&tags=story&hitsPerPage=3`;
      const storiesData = await httpClient.get(storiesUrl).json<AlgoliaResponse>();

      if (storiesData.nbHits > 0) {
        const topTitles = storiesData.hits
          .slice(0, 2)
          .map((h) => h.title)
          .join(', ');
        results.push({
          url: `https://hn.algolia.com/?query=${encodeURIComponent(searchTerm)}&tags=story`,
          title: `Stories about "${searchTerm}"`,
          description: `Found ${storiesData.nbHits} stories. Recent: ${topTitles}`,
          metadata: {
            search_query: searchTerm,
            content_type: 'story',
          },
        });
      }

      // Check for comments
      const commentsUrl = `${this.BASE_URL}/search?query=${encodeURIComponent(
        searchTerm
      )}&tags=comment&hitsPerPage=3`;
      const commentsData = await httpClient.get(commentsUrl).json<AlgoliaResponse>();

      if (commentsData.nbHits > 0) {
        results.push({
          url: `https://hn.algolia.com/?query=${encodeURIComponent(searchTerm)}&tags=comment`,
          title: `Comments about "${searchTerm}"`,
          description: `Found ${commentsData.nbHits} comments.`,
          metadata: {
            search_query: searchTerm,
            content_type: 'comment',
          },
        });
      }

      return results;
    } catch (error) {
      logger.error({ error }, 'HackerNews search error:');
      return [];
    }
  }
}
