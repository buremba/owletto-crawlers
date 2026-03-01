/**
 * Reddit Connector (V1 runtime)
 *
 * Fetches posts and comments from Reddit subreddits or search queries.
 * Supports both authenticated (OAuth) and unauthenticated (public JSON API) modes.
 * Single-file source compatible with Owletto connector compiler.
 */

import {
  ConnectorRuntime,
  type ActionContext,
  type ActionResult,
  type ConnectorDefinition,
  type EventEnvelope,
  type SyncContext,
  type SyncResult,
  calculateEngagementScore,
} from '@owletto/sdk';

// ---------------------------------------------------------------------------
// Reddit API types
// ---------------------------------------------------------------------------

interface RedditPost {
  name: string;
  id: string;
  title: string;
  selftext: string;
  author: string;
  permalink: string;
  created_utc: number;
  score: number;
  ups: number;
  num_comments: number;
  upvote_ratio: number;
  is_self: boolean;
  domain: string;
  subreddit: string;
  crosspost_parent?: string;
}

interface RedditComment {
  name: string;
  id: string;
  body: string;
  author: string;
  permalink: string;
  created_utc: number;
  score: number;
  ups: number;
  parent_id: string;
  link_id: string;
  subreddit: string;
}

interface RedditListingResponse {
  data: {
    children: Array<{
      kind: string;
      data: RedditPost & RedditComment;
    }>;
    after: string | null;
  };
}

interface RedditCheckpoint {
  last_timestamp?: string;
  pagination_token?: string;
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

export default class RedditConnector extends ConnectorRuntime {
  readonly definition: ConnectorDefinition = {
    key: 'reddit',
    name: 'Reddit',
    description: 'Fetches posts and comments from Reddit subreddits or search queries.',
    version: '1.0.0',
    authSchema: {
      methods: [
        {
          type: 'oauth',
          provider: 'reddit',
          requiredScopes: ['read', 'history'],
        },
        {
          type: 'none',
        },
      ],
    },
    feeds: {
      content: {
        key: 'content',
        name: 'Reddit Content',
        description: 'Fetch posts or comments from subreddits or search queries.',
        configSchema: {
          type: 'object',
          properties: {
            subreddit: {
              type: 'string',
              description: 'Subreddit name without r/ prefix (e.g., "programming").',
            },
            search_terms: {
              type: 'string',
              description: 'Search terms to query across Reddit.',
            },
            content_type: {
              type: 'string',
              enum: ['post', 'comment'],
              description: 'Content type to fetch: "post" or "comment".',
            },
            lookback_days: {
              type: 'integer',
              minimum: 1,
              maximum: 730,
              default: 365,
              description: 'Number of days to look back for historical data.',
            },
          },
        },
      },
    },
    optionsSchema: {
      type: 'object',
      properties: {
        subreddit: {
          type: 'string',
          description: 'Subreddit name without r/ prefix (e.g., "programming").',
        },
        search_terms: {
          type: 'string',
          description: 'Search terms to query across Reddit.',
        },
        content_type: {
          type: 'string',
          enum: ['post', 'comment'],
          description: 'Content type to fetch: "post" or "comment".',
        },
        lookback_days: {
          type: 'integer',
          minimum: 1,
          maximum: 730,
          default: 365,
          description: 'Number of days to look back for historical data.',
        },
      },
    },
  };

  private readonly MAX_PAGES = 10;
  private readonly RATE_LIMIT_MS = 1000;
  private readonly USER_AGENT = 'Owletto-Connector/1.0.0';

  // -------------------------------------------------------------------------
  // sync
  // -------------------------------------------------------------------------

  async sync(ctx: SyncContext): Promise<SyncResult> {
    const subreddit = ctx.config.subreddit as string | undefined;
    const searchTerms = ctx.config.search_terms as string | undefined;
    const contentType = (ctx.config.content_type as string) ?? 'post';
    const lookbackDays = (ctx.config.lookback_days as number) ?? 365;

    const accessToken = ctx.credentials?.accessToken;
    const baseUrl = accessToken ? 'https://oauth.reddit.com' : 'https://www.reddit.com';

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - lookbackDays);

    const events: EventEnvelope[] = [];
    let after: string | null = null;
    let page = 0;
    let reachedCutoff = false;

    while (page < this.MAX_PAGES && !reachedCutoff) {
      const url = this.buildFetchUrl({
        baseUrl,
        subreddit,
        searchTerms,
        contentType,
        after,
        isOAuth: !!accessToken,
      });

      const headers: Record<string, string> = {
        'User-Agent': this.USER_AGENT,
      };
      if (accessToken) {
        headers.Authorization = `Bearer ${accessToken}`;
      }

      const response = await fetch(url, { headers });
      if (!response.ok) {
        const status = response.status;
        if (status === 429) {
          throw new Error('Reddit rate limit exceeded. Please wait before retrying.');
        }
        if (status === 404) {
          throw new Error('Subreddit or resource not found. Please check the subreddit name.');
        }
        if (status === 403) {
          throw new Error('Access forbidden. The subreddit may be private or banned.');
        }
        throw new Error(`Reddit API error (${status}): ${await response.text()}`);
      }

      const listing = (await response.json()) as RedditListingResponse;
      const children = listing.data.children;

      if (children.length === 0) break;

      for (const child of children) {
        const itemData = child.data;
        const itemDate = new Date(itemData.created_utc * 1000);

        if (itemDate < cutoffDate) {
          reachedCutoff = true;
          break;
        }

        // Filter deleted/removed items
        if (itemData.author === '[deleted]') continue;

        if (contentType === 'post') {
          const post = itemData as RedditPost;
          if (post.crosspost_parent) continue;
          if (post.selftext === '[removed]' || post.selftext === '[deleted]') continue;

          events.push(this.transformPost(post));
        } else {
          const comment = itemData as RedditComment;
          if (comment.body === '[removed]' || comment.body === '[deleted]') continue;

          events.push(this.transformComment(comment));
        }
      }

      after = listing.data.after;
      if (!after) break;

      page++;

      if (page < this.MAX_PAGES && !reachedCutoff) {
        await this.sleep(this.RATE_LIMIT_MS);
      }
    }

    const checkpoint: RedditCheckpoint = {
      last_timestamp: new Date().toISOString(),
      pagination_token: after ?? undefined,
    };

    return {
      events,
      checkpoint: checkpoint as Record<string, unknown>,
      metadata: {
        items_found: events.length,
      },
    };
  }

  // -------------------------------------------------------------------------
  // execute
  // -------------------------------------------------------------------------

  async execute(_ctx: ActionContext): Promise<ActionResult> {
    return { success: false, error: 'Actions not supported' };
  }

  // -------------------------------------------------------------------------
  // URL building
  // -------------------------------------------------------------------------

  private buildFetchUrl(params: {
    baseUrl: string;
    subreddit?: string;
    searchTerms?: string;
    contentType: string;
    after: string | null;
    isOAuth: boolean;
  }): string {
    const { baseUrl, subreddit, searchTerms, contentType, after, isOAuth } = params;
    const jsonSuffix = isOAuth ? '' : '.json';
    const afterParam = after ? `&after=${after}` : '';

    if (contentType === 'comment') {
      // Comments from a subreddit
      if (subreddit) {
        return `${baseUrl}/r/${subreddit}/comments${jsonSuffix}?limit=100${afterParam}`;
      }
      // Comments aren't searchable via Reddit search API, fall back to r/all
      return `${baseUrl}/r/all/comments${jsonSuffix}?limit=100${afterParam}`;
    }

    // Posts mode
    if (searchTerms) {
      const query = encodeURIComponent(searchTerms);
      if (subreddit) {
        return `${baseUrl}/r/${subreddit}/search${jsonSuffix}?q=${query}&restrict_sr=on&sort=relevance&t=year&limit=100${afterParam}`;
      }
      return `${baseUrl}/search${jsonSuffix}?q=${query}&sort=relevance&t=year&limit=100${afterParam}`;
    }

    // Subreddit listing
    if (subreddit) {
      return `${baseUrl}/r/${subreddit}/new${jsonSuffix}?t=year&limit=100${afterParam}`;
    }

    // Fallback to r/all
    return `${baseUrl}/r/all/new${jsonSuffix}?t=year&limit=100${afterParam}`;
  }

  // -------------------------------------------------------------------------
  // Transform helpers
  // -------------------------------------------------------------------------

  private transformPost(post: RedditPost): EventEnvelope {
    const engagementScore = calculateEngagementScore('reddit', {
      score: post.score,
      reply_count: post.num_comments,
      upvotes: post.ups,
    });

    return {
      external_id: `reddit_post_${post.name}`,
      title: post.title,
      content: (post.selftext ?? '').trim(),
      author: post.author,
      url: `https://reddit.com${post.permalink}`,
      published_at: new Date(post.created_utc * 1000),
      kind: 'post',
      score: engagementScore,
      metadata: {
        subreddit: post.subreddit,
        score: post.score,
        num_comments: post.num_comments,
        upvote_ratio: post.upvote_ratio,
        is_self: post.is_self,
        domain: post.domain,
      },
    };
  }

  private transformComment(comment: RedditComment): EventEnvelope {
    const engagementScore = calculateEngagementScore('reddit', {
      score: comment.score,
      upvotes: comment.ups,
    });

    let parentExternalId: string | undefined;
    if (comment.parent_id) {
      if (comment.parent_id.startsWith('t1_')) {
        // Parent is another comment
        parentExternalId = `reddit_comment_${comment.parent_id}`;
      } else if (comment.parent_id.startsWith('t3_')) {
        // Parent is a post
        parentExternalId = `reddit_post_${comment.parent_id}`;
      }
    }

    return {
      external_id: `reddit_comment_${comment.name}`,
      content: comment.body ?? '',
      author: comment.author,
      url: `https://reddit.com${comment.permalink}`,
      published_at: new Date(comment.created_utc * 1000),
      kind: 'comment',
      score: engagementScore,
      parent_external_id: parentExternalId,
      metadata: {
        subreddit: comment.subreddit,
        score: comment.score,
      },
    };
  }

  // -------------------------------------------------------------------------
  // Utilities
  // -------------------------------------------------------------------------

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
