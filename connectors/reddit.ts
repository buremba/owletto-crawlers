/**
 * Reddit Crawler
 *
 * Crawls posts and comments from subreddits or performs Reddit-wide searches.
 * Supports both authenticated (OAuth) and unauthenticated (public JSON API) modes.
 *
 * Refactored to use ApiPaginatedCrawler for reusable pagination logic.
 */

import { type Static, Type } from '@owletto/sdk';
import type { KyInstance } from '@owletto/sdk';
import { HTTPError } from '@owletto/sdk';
import { logger } from '@owletto/sdk';
import { withHttpRetry } from '@owletto/sdk';
import { ApiPaginatedCrawler } from '@owletto/sdk';
import type {
  Checkpoint,
  Content,
  CrawlerOptions,
  CrawlResult,
  Env,
  ParentSourceDefinition,
  SearchResult,
  SessionState,
} from '@owletto/sdk';
import { calculateEngagementScore, RateLimitError } from '@owletto/sdk';
import { createAuthenticatedClient, httpClient } from '@owletto/sdk';
import type { PageFetchResult, PaginatedCheckpoint } from '@owletto/sdk';

/**
 * Reddit-specific environment variables
 */
interface RedditEnv extends Env {
  REDDIT_CLIENT_ID?: string;
  REDDIT_CLIENT_SECRET?: string;
  REDDIT_USER_AGENT?: string;
}

/**
 * Reddit-specific options schema
 */
export const RedditOptionsSchema = Type.Object(
  {
    subreddit: Type.Optional(
      Type.String({
        description:
          'Subreddit name without r/ prefix (e.g., "spotify"). Can be combined with search_terms for subreddit-scoped search.',
        minLength: 1,
      })
    ),
    search_terms: Type.Optional(
      Type.Array(Type.String(), {
        description:
          'Search terms to query. If subreddit is provided, searches within that subreddit. Otherwise searches all of Reddit.',
      })
    ),
    content_type: Type.Union([Type.Literal('posts'), Type.Literal('comments')], {
      description: 'Content type: "posts" or "comments". Each crawler does ONE thing only.',
    }),
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
    description:
      'Reddit crawler options - requires either subreddit or search_terms, and content_type',
    $id: 'RedditOptions',
  }
);

export type RedditOptions = Static<typeof RedditOptionsSchema>;

/**
 * Reddit-specific checkpoint structure
 */
export interface RedditCheckpoint extends PaginatedCheckpoint {
  last_id?: string;
  last_fullname?: string;
  last_after_token?: string | null;
  subreddit_state?: {
    [subreddit: string]: {
      last_id: string;
      last_timestamp: Date;
    };
  };
}

/**
 * Reddit post data structure from API
 */
interface RedditPost {
  id: string;
  title: string;
  selftext: string;
  author: string;
  permalink: string;
  created_utc: number;
  score: number;
  ups: number;
  downs: number;
  num_comments: number;
  crosspost_parent?: string;
  subreddit?: string;
}

/**
 * Reddit comment data structure from API
 */
interface RedditComment {
  id: string;
  body: string;
  author: string;
  permalink: string;
  created_utc: number;
  score: number;
  ups: number;
  downs: number;
  parent_id: string;
  link_id: string;
  depth: number;
}

/**
 * Reddit API response structure
 */
interface RedditAPIResponse {
  data: {
    children: Array<{
      kind: string;
      data: RedditPost | RedditComment;
    }>;
    after?: string;
  };
}

/**
 * Union type for Reddit items
 */
type RedditItem = RedditPost | RedditComment;

/**
 * Reddit crawler implementation using ApiPaginatedCrawler
 */
export class RedditCrawler extends ApiPaginatedCrawler<
  RedditItem,
  RedditAPIResponse,
  RedditCheckpoint
> {
  readonly type = 'reddit';
  readonly displayName = 'Reddit';
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = RedditOptionsSchema;
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
  readonly authSchema = {
    methods: [
      {
        type: 'oauth' as const,
        provider: 'reddit',
        requiredScopes: ['read', 'history'],
        authorizationUrl: 'https://www.reddit.com/api/v1/authorize',
        tokenUrl: 'https://www.reddit.com/api/v1/access_token',
        clientIdKey: 'REDDIT_CLIENT_ID',
        clientSecretKey: 'REDDIT_CLIENT_SECRET',
        required: false,
        scope: 'source' as const,
        description:
          'Optional Reddit OAuth for authenticated access and improved rate limits. Without credentials, crawler uses public JSON endpoints.',
      },
      {
        type: 'env_keys' as const,
        required: false,
        scope: 'source' as const,
        fields: [
          {
            key: 'REDDIT_CLIENT_ID',
            label: 'Reddit Client ID',
            description: 'OAuth app client ID from https://www.reddit.com/prefs/apps',
          },
          {
            key: 'REDDIT_CLIENT_SECRET',
            label: 'Reddit Client Secret',
            description: 'OAuth app client secret from https://www.reddit.com/prefs/apps',
            secret: true,
          },
        ],
        description:
          'Optional OAuth app credentials for authenticated Reddit API access. Leave empty to run in public mode.',
      },
    ],
  };

  // OAuth token cache
  private accessToken: string | null = null;
  private tokenExpiresAt: number = 0;
  private tokenRefreshPromise: Promise<string | null> | null = null;

  // Current crawl context
  private currentOptions: RedditOptions | null = null;
  private baseUrl: string = 'https://www.reddit.com';

  protected getPaginationConfig() {
    return {
      maxPages: 50,
      pageSize: 100,
      rateLimitMs: 1000, // 60 req/min limit
      incrementalCheckpoint: true, // Reddit uses incremental checkpoints
    };
  }

  getRateLimit() {
    return {
      requests_per_minute: 60,
      recommended_interval_ms: 1000,
    };
  }

  validateOptions(options: RedditOptions): string | null {
    const schemaError = this.validateWithSchema(options);
    if (schemaError) return schemaError;

    if (!options.subreddit && !options.search_terms) {
      return 'Either subreddit or search_terms required';
    }
    if (options.search_terms && options.search_terms.length === 0) {
      return 'search_terms cannot be empty';
    }
    if (options.content_type === 'comments' && options.search_terms) {
      return 'search_terms is not supported with content_type=comments. Reddit API limitation.';
    }

    return null;
  }

  urlFromOptions(options: RedditOptions): string {
    if (options.subreddit) {
      if (options.search_terms && options.search_terms.length > 0) {
        const searchQuery = options.search_terms.join(' OR ');
        return `https://reddit.com/r/${options.subreddit}/search?q=${encodeURIComponent(searchQuery)}&restrict_sr=1`;
      }
      return `https://reddit.com/r/${options.subreddit}`;
    }
    if (options.search_terms && options.search_terms.length > 0) {
      return `https://reddit.com/search?q=${encodeURIComponent(options.search_terms[0])}`;
    }
    return '';
  }

  displayLabelFromOptions(options: RedditOptions): string {
    let label = '';

    if (options.subreddit) {
      label = `r/${options.subreddit}'s ${options.content_type}`;
      if (options.search_terms && options.search_terms.length > 0) {
        const searchTerms = options.search_terms
          .map((term) => `"${term.charAt(0).toUpperCase()}${term.slice(1)}"`)
          .join(', ');
        label += `, search ${searchTerms}`;
      }
    } else if (options.search_terms && options.search_terms.length > 0) {
      const searchTerms = options.search_terms
        .map((term) => `"${term.charAt(0).toUpperCase()}${term.slice(1)}"`)
        .join(', ');
      label = `Reddit ${options.content_type}, search ${searchTerms}`;
    } else {
      label = `Reddit ${options.content_type}`;
    }

    return label;
  }

  getParentSourceDefinitions(options: RedditOptions): ParentSourceDefinition[] {
    if (options.content_type !== 'comments') {
      return [];
    }

    const parentOptions: RedditOptions = {
      ...options,
      content_type: 'posts',
    };

    return [
      {
        type: this.type,
        options: parentOptions,
        description: 'Posts',
      },
    ];
  }

  /**
   * Get OAuth access token (cached if valid)
   */
  private async getAccessToken(env: RedditEnv): Promise<string | null> {
    if (!env.REDDIT_CLIENT_ID || !env.REDDIT_CLIENT_SECRET) {
      return null;
    }

    if (this.accessToken && Date.now() < this.tokenExpiresAt - 300000) {
      return this.accessToken;
    }

    if (this.tokenRefreshPromise) {
      return this.tokenRefreshPromise;
    }

    this.tokenRefreshPromise = this.refreshToken(env);
    try {
      return await this.tokenRefreshPromise;
    } finally {
      this.tokenRefreshPromise = null;
    }
  }

  private async refreshToken(env: RedditEnv): Promise<string | null> {
    try {
      const auth = btoa(`${env.REDDIT_CLIENT_ID}:${env.REDDIT_CLIENT_SECRET}`);
      const response = await httpClient.post('https://www.reddit.com/api/v1/access_token', {
        headers: {
          Authorization: `Basic ${auth}`,
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': env.REDDIT_USER_AGENT || 'UserContent-MCP/0.1.0',
        },
        body: 'grant_type=client_credentials',
      });

      const data = await response.json<{ access_token: string; expires_in: number }>();
      this.accessToken = data.access_token;
      this.tokenExpiresAt = Date.now() + data.expires_in * 1000;
      return this.accessToken;
    } catch (error) {
      logger.error({ error }, 'Failed to get Reddit access token');
      return null;
    }
  }

  /**
   * Get configured HTTP client (authenticated or public)
   */
  protected async getClientAndBaseUrl(
    env: RedditEnv
  ): Promise<{ client: KyInstance; baseUrl: string }> {
    const token = await this.getAccessToken(env);
    if (token) {
      return {
        client: createAuthenticatedClient(`Bearer ${token}`, {
          'User-Agent': env.REDDIT_USER_AGENT || 'UserContent-MCP/0.1.0',
        }),
        baseUrl: 'https://oauth.reddit.com',
      };
    }
    return {
      client: httpClient,
      baseUrl: 'https://www.reddit.com',
    };
  }

  /**
   * Get pagination token from checkpoint (Reddit uses after token)
   */
  protected getPaginationToken(checkpoint: RedditCheckpoint | null): string | null {
    return checkpoint?.last_after_token ?? checkpoint?.pagination_token ?? null;
  }

  /**
   * Build URL for fetching a page based on content_type and options
   */
  protected buildPageUrl(cursor: string | null, options: CrawlerOptions): string {
    const redditOptions = options as RedditOptions;
    const { subreddit, content_type, search_terms } = redditOptions;
    const afterParam = cursor ? `&after=${cursor}` : '';

    if (content_type === 'comments') {
      return `${this.baseUrl}/r/${subreddit}/comments.json?limit=100${afterParam}`;
    }

    // Posts mode
    if (subreddit && !search_terms) {
      return `${this.baseUrl}/r/${subreddit}/new.json?limit=100${afterParam}`;
    }

    if (search_terms && search_terms.length > 0) {
      const query = encodeURIComponent(search_terms.join(' OR '));
      if (subreddit) {
        return `${this.baseUrl}/r/${subreddit}/search.json?q=${query}&restrict_sr=on&sort=new&limit=100${afterParam}`;
      }
      return `${this.baseUrl}/search.json?q=${query}&sort=new&limit=100${afterParam}`;
    }

    return `${this.baseUrl}/r/${subreddit}/new.json?limit=100${afterParam}`;
  }

  /**
   * Parse Reddit API response
   */
  protected parseResponse(
    response: RedditAPIResponse,
    _options: CrawlerOptions
  ): PageFetchResult<RedditItem> {
    return {
      items: response.data.children.map((c) => c.data),
      nextToken: response.data.after ?? null,
      rawCount: response.data.children.length,
    };
  }

  /**
   * Get item date from Reddit item
   */
  protected getItemDate(item: RedditItem): Date {
    return new Date(item.created_utc * 1000);
  }

  /**
   * Filter out deleted/removed items and crossposts
   */
  protected filterItem(item: RedditItem, options: CrawlerOptions): boolean {
    const redditOptions = options as RedditOptions;

    if (redditOptions.content_type === 'posts') {
      const post = item as RedditPost;
      if (post.crosspost_parent) return false;
      if (post.author === '[deleted]') return false;
      if (post.selftext === '[removed]' || post.selftext === '[deleted]') return false;

      // If we have search_terms with subreddit, also filter by search terms
      if (
        redditOptions.subreddit &&
        redditOptions.search_terms &&
        redditOptions.search_terms.length > 0
      ) {
        const searchTermsLower = redditOptions.search_terms.map((t) => t.toLowerCase());
        const titleLower = post.title?.toLowerCase() || '';
        const contentLower = post.selftext?.toLowerCase() || '';
        return searchTermsLower.some(
          (term) => titleLower.includes(term) || contentLower.includes(term)
        );
      }

      return true;
    }

    // Comments
    const comment = item as RedditComment;
    if (comment.author === '[deleted]') return false;
    if (comment.body === '[removed]' || comment.body === '[deleted]') return false;
    return true;
  }

  /**
   * Get parent ID for comments
   */
  protected getParentId(item: RedditItem): string | null {
    const redditOptions = this.currentOptions;
    if (!redditOptions || redditOptions.content_type !== 'comments') return null;

    const comment = item as RedditComment;
    const parentId = comment.parent_id;

    if (parentId?.startsWith('t1_')) {
      // Nested comment: parent is another comment
      return `comment_${parentId.replace('t1_', '')}`;
    }
    if (parentId?.startsWith('t3_')) {
      // Top-level comment: parent is the post
      return parentId.replace('t3_', '');
    }

    return null;
  }

  /**
   * Transform Reddit item to Content format
   */
  protected transformItem(item: RedditItem, options: CrawlerOptions): Content {
    const redditOptions = options as RedditOptions;

    if (redditOptions.content_type === 'comments') {
      const comment = item as RedditComment;
      const postId = comment.link_id.replace('t3_', '');
      return this.transformComment(comment, postId);
    }

    return this.transformPost(item as RedditPost);
  }

  private transformPost(post: RedditPost): Content {
    const engagementData = {
      score: post.score,
      upvotes: post.ups,
      downvotes: post.downs,
      reply_count: post.num_comments,
    };
    return {
      external_id: post.id,
      title: post.title,
      content: (post.selftext || '').trim(),
      author: post.author,
      url: `https://reddit.com${post.permalink}`,
      published_at: new Date(post.created_utc * 1000),
      score: calculateEngagementScore('reddit', engagementData),
      metadata: engagementData,
    };
  }

  private transformComment(comment: RedditComment, postId: string): Content {
    const engagementData = {
      score: comment.score,
      upvotes: comment.ups,
      downvotes: comment.downs,
    };
    return {
      external_id: `comment_${comment.id}`,
      content: comment.body,
      author: comment.author,
      url: `https://reddit.com${comment.permalink}`,
      published_at: new Date(comment.created_utc * 1000),
      score: calculateEngagementScore('reddit', engagementData),
      metadata: {
        ...engagementData,
        post_id: postId,
        parent_id: comment.parent_id,
        depth: comment.depth,
      },
    };
  }

  /**
   * Create checkpoint with Reddit-specific fields
   */
  protected createCheckpoint(
    existing: RedditCheckpoint | null,
    latestContent: Content | null,
    nextToken: string | null,
    itemsProcessed: number
  ): RedditCheckpoint {
    return {
      updated_at: new Date(),
      last_timestamp: latestContent?.published_at ?? existing?.last_timestamp,
      last_after_token: nextToken,
      pagination_token: nextToken, // Also set for base class compatibility
      total_items_processed: (existing?.total_items_processed || 0) + itemsProcessed,
    };
  }

  /**
   * Override fetchPage to set up client with correct base URL
   */
  protected async fetchPage(
    cursor: string | null,
    options: CrawlerOptions,
    env: Env
  ): Promise<PageFetchResult<RedditItem>> {
    const redditEnv = env as RedditEnv;

    // Get authenticated client and base URL
    const { client, baseUrl } = await this.getClientAndBaseUrl(redditEnv);
    this.baseUrl = baseUrl;

    const url = this.buildPageUrl(cursor, options);

    try {
      const response = await withHttpRetry(async () => client.get(url).json<RedditAPIResponse>(), {
        operation: 'Reddit API fetch',
        context: { url, cursor },
      });

      return this.parseResponse(response, options);
    } catch (error) {
      if (error instanceof HTTPError) {
        this.handleHttpError(error, url);
      }
      throw error;
    }
  }

  /**
   * Main pull method - uses base class pagination
   */
  async pull(
    options: RedditOptions,
    checkpoint: RedditCheckpoint | null,
    env: Env,
    _sessionState?: SessionState | null,
    updateCheckpointFn?: (checkpoint: Checkpoint) => Promise<void>
  ): Promise<CrawlResult> {
    // Store current context
    this.currentOptions = options;

    // Use base class pagination
    const { contents, parentMap, nextCrawlRecommendedAt } = await this.paginate(
      options,
      checkpoint,
      env,
      updateCheckpointFn
    );

    // Deduplicate and sort
    const seenIds = new Set<string>();
    const uniqueContents = this.deduplicate(contents, seenIds);
    uniqueContents.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

    // Reddit saves checkpoints incrementally, so we return null to avoid overwriting
    return {
      contents: uniqueContents,
      checkpoint: null, // Checkpoint already saved by incremental updates
      metadata: {
        items_found: contents.length,
        items_skipped: contents.length - uniqueContents.length,
        parent_map: parentMap ? Object.fromEntries(parentMap) : undefined,
        next_crawl_recommended_at: nextCrawlRecommendedAt,
      },
    };
  }

  /**
   * Handle HTTP errors with Reddit-specific messages
   */
  protected handleHttpError(error: HTTPError, url: string): never {
    const status = error.response.status;
    logger.error({ status, url, platform: 'reddit' }, `[RedditCrawler] HTTP ${status} error`);

    switch (status) {
      case 429:
        throw new RateLimitError(
          'Reddit rate limit exceeded. Please wait 60 seconds before retrying.'
        );
      case 404:
        throw new Error('Subreddit or resource not found. Please check the subreddit name.');
      case 403:
        throw new Error('Access forbidden. The subreddit may be private or banned.');
      case 500:
      case 502:
      case 503:
        throw new Error(`Reddit server error (${status}). Please retry after 5 minutes.`);
      default:
        super.handleHTTPError(status, url, 'Reddit');
    }
  }

  /**
   * Search Reddit for subreddits
   */
  async search(searchTerm: string, _env: Env): Promise<SearchResult[]> {
    const results: SearchResult[] = [];

    try {
      const subredditSearchUrl = `https://www.reddit.com/subreddits/search.json?q=${encodeURIComponent(
        searchTerm
      )}&limit=3`;

      const subredditData = await withHttpRetry(
        async () => httpClient.get(subredditSearchUrl).json<RedditAPIResponse>(),
        {
          operation: 'Reddit subreddit search',
          context: { searchTerm },
        }
      );

      for (const sub of subredditData.data.children.slice(0, 3)) {
        const displayName = (sub.data as any).display_name;
        const url = (sub.data as any).url || `/r/${displayName}`;

        if (!displayName) {
          logger.warn({ data: sub.data }, 'Reddit search returned subreddit without display_name');
          continue;
        }

        const description = (
          (sub.data as any).public_description ||
          (sub.data as any).selftext ||
          'Reddit community'
        ).substring(0, 200);

        results.push({
          url: `https://reddit.com${url}`,
          title: `r/${displayName} (posts)`,
          description: `${description} - Posts only`,
          metadata: {
            subreddit: displayName,
            content_type: 'posts' as const,
          },
        });

        results.push({
          url: `https://reddit.com${url}`,
          title: `r/${displayName} (comments)`,
          description: `${description} - Comments only`,
          metadata: {
            subreddit: displayName,
            content_type: 'comments' as const,
          },
        });
      }
    } catch (error) {
      logger.error({ error }, 'Reddit search error:');
    }

    return results;
  }
}
