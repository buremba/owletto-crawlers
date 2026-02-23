/**
 * GitHub Crawler
 *
 * Crawls issues, PRs, comments, and discussions from GitHub repositories.
 * Supports both authenticated (via GITHUB_TOKEN) and unauthenticated modes.
 *
 * Refactored to use ApiPaginatedCrawler for reusable pagination logic.
 */

import { type Static, Type } from '@sinclair/typebox';
import type { KyInstance } from 'ky';
import { HTTPError } from 'ky';
import logger from '@/utils/logger';
import { ApiPaginatedCrawler } from './api-paginated';
import type {
  Checkpoint,
  Content,
  CrawlerOptions,
  CrawlResult,
  Env,
  ParentSourceDefinition,
  SearchResult,
  SessionState,
} from './base';
import { calculateEngagementScore, RateLimitError } from './base';
import { createAuthenticatedClient, createHttpClient } from './http';
import type { PageFetchResult, PaginatedCheckpoint } from './paginated';

/**
 * GitHub-specific options schema
 */
export const GitHubOptionsSchema = Type.Object(
  {
    repo_owner: Type.String({
      description: 'Repository owner (user or organization)',
      minLength: 1,
    }),
    repo_name: Type.String({
      description: 'Repository name',
      minLength: 1,
    }),
    content_type: Type.Union(
      [
        Type.Literal('issues'),
        Type.Literal('pull_requests'),
        Type.Literal('issue_comments'),
        Type.Literal('pr_comments'),
        Type.Literal('discussions'),
        Type.Literal('discussion_comments'),
      ],
      {
        description:
          'Content type: ONE type per crawler for reliable checkpointing. Create separate crawlers for different types.',
      }
    ),
    labels_filter: Type.Optional(
      Type.Array(Type.String({ minLength: 1 }), {
        description: 'Filter by specific labels (optional, applies to issues and PRs only)',
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
    description: 'GitHub crawler options - each crawler tracks ONE content type',
    $id: 'GitHubOptions',
  }
);

export type GitHubOptions = Static<typeof GitHubOptionsSchema>;

/**
 * GitHub-specific checkpoint structure
 */
export interface GitHubCheckpoint extends PaginatedCheckpoint {
  last_updated_at?: string;
  last_comment_id?: number;
  last_cursor?: string;
  rate_limit_remaining?: number;
  rate_limit_reset?: number;
}

/**
 * GitHub Issue/PR data structure
 */
interface GitHubIssue {
  id: number;
  number: number;
  title: string;
  body: string | null;
  user: { login: string };
  html_url: string;
  created_at: string;
  updated_at: string;
  state: string;
  labels: Array<{ name: string }>;
  comments: number;
  reactions?: { '+1': number; '-1': number; total_count: number };
  pull_request?: { url: string; merged_at: string | null };
}

/**
 * GitHub Comment data structure
 */
interface GitHubComment {
  id: number;
  body: string;
  user: { login: string };
  html_url: string;
  created_at: string;
  updated_at: string;
  reactions?: { '+1': number; '-1': number; total_count: number };
}

/**
 * GitHub Discussion data structure (GraphQL)
 */
interface GitHubDiscussion {
  id: string;
  number: number;
  title: string;
  body: string;
  author: { login: string };
  url: string;
  createdAt: string;
  updatedAt: string;
  category: { name: string };
  comments: { totalCount: number };
  reactions: { totalCount: number };
}

/**
 * GitHub Discussion Comment data structure (GraphQL)
 */
interface GitHubDiscussionComment {
  id: string;
  body: string;
  author: { login: string };
  url: string;
  createdAt: string;
  updatedAt: string;
  reactions: { totalCount: number };
}

/**
 * Union type for all GitHub items
 */
type GitHubItem = GitHubIssue | GitHubComment | GitHubDiscussion | GitHubDiscussionComment;

/**
 * GitHub crawler implementation using ApiPaginatedCrawler
 */
export class GitHubCrawler extends ApiPaginatedCrawler<
  GitHubItem,
  GitHubItem[] | { data?: any },
  GitHubCheckpoint
> {
  readonly type = 'github';
  readonly displayName = 'GitHub';
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = GitHubOptionsSchema;
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

  /**
   * GitHub OAuth provider for user-authenticated access
   * Enables access to private repositories and higher rate limits
   * Falls back to GITHUB_TOKEN env var if no user credential is linked
   */
  readonly oauthProvider = {
    provider: 'github',
    requiredScopes: ['repo', 'read:user'],
    description:
      'GitHub OAuth enables access to private repositories and higher rate limits (5000 req/hour)',
    required: false, // Can fall back to GITHUB_TOKEN env var
  };

  private readonly BASE_URL = 'https://api.github.com';
  private currentOptions: GitHubOptions | null = null;
  private currentSessionState: SessionState | null = null;

  protected getPaginationConfig() {
    return {
      maxPages: 50,
      pageSize: 100,
      rateLimitMs: 1000,
      incrementalCheckpoint: false,
    };
  }

  getRateLimit() {
    return {
      requests_per_minute: 60,
      requests_per_hour: 5000,
      recommended_interval_ms: 1000,
    };
  }

  validateOptions(options: GitHubOptions): string | null {
    const schemaError = this.validateWithSchema(options);
    if (schemaError) return schemaError;
    return null;
  }

  urlFromOptions(options: GitHubOptions): string {
    return `https://github.com/${options.repo_owner}/${options.repo_name}`;
  }

  displayLabelFromOptions(options: GitHubOptions): string {
    return `${options.repo_owner}/${options.repo_name}`;
  }

  getParentSourceDefinitions(options: GitHubOptions): ParentSourceDefinition[] {
    const baseOptions = {
      repo_owner: options.repo_owner,
      repo_name: options.repo_name,
      lookback_days: options.lookback_days,
    };

    if (options.content_type === 'issue_comments') {
      return [
        {
          type: this.type,
          options: {
            ...baseOptions,
            content_type: 'issues',
            labels_filter: options.labels_filter,
          },
          description: 'Issues',
        },
      ];
    }

    if (options.content_type === 'pr_comments') {
      return [
        {
          type: this.type,
          options: {
            ...baseOptions,
            content_type: 'pull_requests',
            labels_filter: options.labels_filter,
          },
          description: 'Pull Requests',
        },
      ];
    }

    if (options.content_type === 'discussion_comments') {
      return [
        {
          type: this.type,
          options: {
            ...baseOptions,
            content_type: 'discussions',
          },
          description: 'Discussions',
        },
      ];
    }

    return [];
  }

  protected getHttpClient(env: Env): KyInstance {
    const headers: Record<string, string> = {
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
    };

    // Check for OAuth credentials from user-linked account first
    const oauthToken = this.currentSessionState?.oauth?.accessToken;
    if (oauthToken) {
      logger.info('[GitHubCrawler] Using OAuth token from user credentials');
      return createAuthenticatedClient(`Bearer ${oauthToken}`, headers);
    }

    // Fall back to environment token
    if (env.GITHUB_TOKEN) {
      logger.debug('[GitHubCrawler] Using GITHUB_TOKEN from environment');
      return createAuthenticatedClient(`token ${env.GITHUB_TOKEN}`, headers);
    }

    // No authentication - limited to 60 requests per hour
    logger.warn(
      '[GitHubCrawler] No authentication token available - rate limits will be very low (60 req/hour)'
    );
    return createHttpClient({ headers });
  }

  protected buildPageUrl(cursor: string | null, options: CrawlerOptions): string {
    const ghOptions = options as GitHubOptions;
    const { repo_owner, repo_name, content_type, labels_filter } = ghOptions;

    if (cursor?.startsWith('https://')) {
      return cursor;
    }

    switch (content_type) {
      case 'issues':
      case 'pull_requests': {
        let url = `${this.BASE_URL}/repos/${repo_owner}/${repo_name}/issues?state=all&per_page=100&sort=updated&direction=desc`;
        if (labels_filter && labels_filter.length > 0) {
          url += `&labels=${labels_filter.join(',')}`;
        }
        return url;
      }

      case 'issue_comments':
        return `${this.BASE_URL}/repos/${repo_owner}/${repo_name}/issues/comments?per_page=100&sort=created&direction=desc`;

      case 'pr_comments':
        return `${this.BASE_URL}/repos/${repo_owner}/${repo_name}/pulls/comments?per_page=100&sort=created&direction=desc`;

      case 'discussions':
      case 'discussion_comments':
        return `${this.BASE_URL}/graphql`;

      default:
        return `${this.BASE_URL}/repos/${repo_owner}/${repo_name}/issues?per_page=100`;
    }
  }

  protected parseResponse(
    response: GitHubItem[] | { data?: any },
    options: CrawlerOptions
  ): PageFetchResult<GitHubItem> {
    const ghOptions = options as GitHubOptions;

    if (Array.isArray(response)) {
      return {
        items: response,
        nextToken: null,
        rawCount: response.length,
      };
    }

    if (
      ghOptions.content_type === 'discussions' ||
      ghOptions.content_type === 'discussion_comments'
    ) {
      const discussions = response.data?.repository?.discussions?.nodes || [];
      const pageInfo = response.data?.repository?.discussions?.pageInfo;

      return {
        items: discussions,
        nextToken: pageInfo?.hasNextPage ? pageInfo.endCursor : null,
        rawCount: discussions.length,
      };
    }

    return { items: [], nextToken: null };
  }

  protected getItemDate(item: GitHubItem): Date {
    if ('createdAt' in item) {
      return new Date(item.createdAt);
    }
    if ('created_at' in item) {
      return new Date(item.created_at);
    }
    return new Date();
  }

  protected filterItem(item: GitHubItem, options: CrawlerOptions): boolean {
    const ghOptions = options as GitHubOptions;

    if ('pull_request' in item) {
      if (ghOptions.content_type === 'issues') {
        return !item.pull_request;
      }
      if (ghOptions.content_type === 'pull_requests') {
        return !!item.pull_request;
      }
    }

    return true;
  }

  protected getParentId(item: GitHubItem): string | null {
    const ghOptions = this.currentOptions;
    if (!ghOptions) return null;

    if ('html_url' in item && typeof item.html_url === 'string') {
      if (ghOptions.content_type === 'issue_comments') {
        const match = item.html_url.match(/\/issues\/(\d+)#/);
        if (match) {
          return `issue_${ghOptions.repo_owner}_${ghOptions.repo_name}_${match[1]}`;
        }
      }
      if (ghOptions.content_type === 'pr_comments') {
        const match = item.html_url.match(/\/pull\/(\d+)#/);
        if (match) {
          return `pr_${ghOptions.repo_owner}_${ghOptions.repo_name}_${match[1]}`;
        }
      }
    }

    return null;
  }

  protected transformItem(item: GitHubItem, options: CrawlerOptions): Content {
    const ghOptions = options as GitHubOptions;
    const { repo_owner, repo_name, content_type } = ghOptions;

    if (content_type === 'issues' || content_type === 'pull_requests') {
      return this.transformIssueOrPR(
        item as GitHubIssue,
        repo_owner,
        repo_name,
        content_type === 'pull_requests'
      );
    }

    if (content_type === 'issue_comments' || content_type === 'pr_comments') {
      const comment = item as GitHubComment;
      const parentType = content_type === 'issue_comments' ? 'issue' : 'pr';
      const match = comment.html_url.match(/\/(issues|pull)\/(\d+)#/);
      const parentNumber = match ? parseInt(match[2], 10) : 0;
      return this.transformComment(comment, repo_owner, repo_name, parentType, parentNumber);
    }

    if (content_type === 'discussions') {
      return this.transformDiscussion(item as GitHubDiscussion, repo_owner, repo_name);
    }

    if (content_type === 'discussion_comments') {
      const discComment = item as GitHubDiscussionComment;
      return this.transformDiscussionComment(discComment, repo_owner, repo_name, 0);
    }

    return {
      external_id: `github_${(item as any).id || Date.now()}`,
      content: '',
      author: 'Unknown',
      url: '',
      published_at: new Date(),
      score: 0,
    };
  }

  private transformIssueOrPR(
    issue: GitHubIssue,
    owner: string,
    repo: string,
    isPR: boolean
  ): Content {
    const externalId = `${isPR ? 'pr' : 'issue'}_${owner}_${repo}_${issue.number}`;
    const engagementData = {
      score: issue.reactions?.total_count || 0,
      upvotes: issue.reactions?.['+1'] || 0,
      downvotes: issue.reactions?.['-1'] || 0,
      reply_count: issue.comments,
    };

    return {
      external_id: externalId,
      title: issue.title,
      content: (issue.body || '').trim(),
      author: issue.user?.login || 'Unknown',
      url: issue.html_url,
      published_at: new Date(issue.created_at),
      score: calculateEngagementScore('github', engagementData),
      metadata: {
        ...engagementData,
        type: isPR ? 'pr' : 'issue',
        number: issue.number,
        labels: issue.labels.map((l) => l.name),
        state: issue.state,
        updated_at: issue.updated_at,
        ...(isPR && {
          is_pr: true,
          merged_at: issue.pull_request?.merged_at || null,
        }),
      },
    };
  }

  private transformComment(
    comment: GitHubComment,
    owner: string,
    repo: string,
    parentType: 'issue' | 'pr',
    parentNumber: number
  ): Content {
    const externalId = `${parentType}_comment_${owner}_${repo}_${comment.id}`;
    const engagementData = {
      score: comment.reactions?.total_count || 0,
      upvotes: comment.reactions?.['+1'] || 0,
      downvotes: comment.reactions?.['-1'] || 0,
    };

    return {
      external_id: externalId,
      content: comment.body,
      author: comment.user?.login || 'Unknown',
      url: comment.html_url,
      published_at: new Date(comment.created_at),
      score: calculateEngagementScore('github', engagementData),
      metadata: {
        ...engagementData,
        type: `${parentType}_comment`,
        comment_id: comment.id,
        parent_type: parentType,
        parent_number: parentNumber,
        updated_at: comment.updated_at,
      },
    };
  }

  private transformDiscussion(discussion: GitHubDiscussion, owner: string, repo: string): Content {
    const externalId = `discussion_${owner}_${repo}_${discussion.number}`;
    const engagementData = {
      score: discussion.reactions.totalCount,
      reply_count: discussion.comments.totalCount,
    };

    return {
      external_id: externalId,
      title: discussion.title,
      content: discussion.body.trim(),
      author: discussion.author?.login || 'Unknown',
      url: discussion.url,
      published_at: new Date(discussion.createdAt),
      score: calculateEngagementScore('github', engagementData),
      metadata: {
        ...engagementData,
        type: 'discussion',
        number: discussion.number,
        category: discussion.category.name,
        updated_at: discussion.updatedAt,
        cursor: discussion.id,
      },
    };
  }

  private transformDiscussionComment(
    comment: GitHubDiscussionComment,
    owner: string,
    repo: string,
    discussionNumber: number
  ): Content {
    const externalId = `discussion_comment_${owner}_${repo}_${comment.id}`;
    const engagementData = { score: comment.reactions.totalCount };

    return {
      external_id: externalId,
      content: comment.body,
      author: comment.author?.login || 'Unknown',
      url: comment.url,
      published_at: new Date(comment.createdAt),
      score: calculateEngagementScore('github', engagementData),
      metadata: {
        ...engagementData,
        type: 'discussion_comment',
        parent_type: 'discussion',
        parent_number: discussionNumber,
        updated_at: comment.updatedAt,
        cursor: comment.id,
      },
    };
  }

  protected async fetchPage(
    cursor: string | null,
    options: CrawlerOptions,
    env: Env
  ): Promise<PageFetchResult<GitHubItem>> {
    const ghOptions = options as GitHubOptions;

    if (
      ghOptions.content_type === 'discussions' ||
      ghOptions.content_type === 'discussion_comments'
    ) {
      return this.fetchDiscussionsPage(cursor, ghOptions, env);
    }

    const client = this.getHttpClient(env);
    const url = this.buildPageUrl(cursor, options);

    try {
      const response = await client.get(url);
      const items = await response.json<GitHubItem[]>();

      const linkHeader = response.headers.get('link');
      const nextUrl = this.parseNextLinkFromHeader(linkHeader);

      return {
        items,
        nextToken: nextUrl,
        rawCount: items.length,
      };
    } catch (error) {
      if (error instanceof HTTPError) {
        this.handleHttpError(error, url);
      }
      throw error;
    }
  }

  private parseNextLinkFromHeader(linkHeader: string | null): string | null {
    if (!linkHeader) return null;

    const links = linkHeader.split(',');
    for (const link of links) {
      const match = link.match(/<([^>]+)>;\s*rel="next"/);
      if (match) {
        return match[1];
      }
    }

    return null;
  }

  private async fetchDiscussionsPage(
    cursor: string | null,
    options: GitHubOptions,
    env: Env
  ): Promise<PageFetchResult<GitHubItem>> {
    const query = `
      query($owner: String!, $repo: String!, $after: String) {
        repository(owner: $owner, name: $repo) {
          discussions(first: 100, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
            nodes {
              id
              number
              title
              body
              author { login }
              url
              createdAt
              updatedAt
              category { name }
              comments { totalCount }
              reactions { totalCount }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      }
    `;

    const variables = {
      owner: options.repo_owner,
      repo: options.repo_name,
      after: cursor || null,
    };

    try {
      const client = this.getHttpClient(env);
      const response = await client
        .post('https://api.github.com/graphql', {
          json: { query, variables },
        })
        .json<{
          data?: {
            repository?: {
              discussions?: { nodes?: GitHubDiscussion[]; pageInfo?: any };
            };
          };
        }>();

      const discussions = response.data?.repository?.discussions?.nodes || [];
      const pageInfo = response.data?.repository?.discussions?.pageInfo;

      return {
        items: discussions as unknown as GitHubItem[],
        nextToken: pageInfo?.hasNextPage ? pageInfo.endCursor : null,
        rawCount: discussions.length,
      };
    } catch (error) {
      logger.error({ error }, 'Failed to fetch GitHub discussions');
      return { items: [], nextToken: null };
    }
  }

  protected createCheckpoint(
    existing: GitHubCheckpoint | null,
    latestContent: Content | null,
    nextToken: string | null,
    itemsProcessed: number
  ): GitHubCheckpoint {
    const base = super.createCheckpoint(existing, latestContent, nextToken, itemsProcessed);

    return {
      ...base,
      last_updated_at: latestContent?.published_at?.toISOString() ?? existing?.last_updated_at,
      last_comment_id: latestContent?.metadata?.comment_id ?? existing?.last_comment_id,
      last_cursor: latestContent?.metadata?.cursor ?? existing?.last_cursor,
    };
  }

  async pull(
    options: GitHubOptions,
    checkpoint: GitHubCheckpoint | null,
    env: Env,
    sessionState?: SessionState | null,
    updateCheckpointFn?: (checkpoint: Checkpoint) => Promise<void>
  ): Promise<CrawlResult> {
    this.currentOptions = options;
    this.currentSessionState = sessionState ?? null;

    const {
      contents,
      checkpoint: newCheckpoint,
      parentMap,
      nextCrawlRecommendedAt,
    } = await this.paginate(options, checkpoint, env, updateCheckpointFn);

    const seenIds = new Set<string>();
    const uniqueContents = this.deduplicate(contents, seenIds);
    uniqueContents.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

    return {
      contents: uniqueContents,
      checkpoint: newCheckpoint,
      metadata: {
        items_found: contents.length,
        items_skipped: contents.length - uniqueContents.length,
        parent_map: parentMap ? Object.fromEntries(parentMap) : undefined,
        next_crawl_recommended_at: nextCrawlRecommendedAt,
      },
    };
  }

  protected handleHttpError(error: HTTPError, url: string): never {
    const status = error.response.status;

    logger.error({ status, url, platform: 'github' }, `[GitHubCrawler] HTTP ${status} error`);

    switch (status) {
      case 401:
        throw new Error(
          'GitHub authentication failed. Check your OAuth credentials or GITHUB_TOKEN.'
        );
      case 403:
        if (error.response.headers.get('x-ratelimit-remaining') === '0') {
          const resetAt = error.response.headers.get('x-ratelimit-reset');
          const retryAfterMs = resetAt ? Number(resetAt) * 1000 - Date.now() : undefined;
          throw new RateLimitError(
            'GitHub rate limit exceeded. Please wait before retrying.',
            retryAfterMs && retryAfterMs > 0 ? retryAfterMs : undefined
          );
        }
        throw new Error(
          'GitHub access forbidden. Consider linking a GitHub OAuth account or setting GITHUB_TOKEN for higher limits.'
        );
      case 404:
        throw new Error(`GitHub repository not found: ${url}`);
      case 422:
        throw new Error('Invalid GitHub API request. Check repository owner and name.');
      default:
        super.handleHTTPError(status, url, 'GitHub');
    }
  }

  async search(searchTerm: string, env: Env): Promise<SearchResult[]> {
    try {
      const url = `${this.BASE_URL}/search/repositories?q=${encodeURIComponent(
        searchTerm
      )}&per_page=3&sort=stars`;

      const client = this.getHttpClient(env);
      const response = await client.get(url).json<{
        items: Array<{
          full_name: string;
          html_url: string;
          description: string | null;
          stargazers_count: number;
          open_issues_count: number;
          has_issues: boolean;
          has_discussions: boolean;
          archived: boolean;
        }>;
      }>();

      const results: SearchResult[] = [];

      for (const repo of response.items) {
        const [owner, name] = repo.full_name.split('/');
        const baseMetadata = { repo_owner: owner, repo_name: name };

        if (repo.has_issues && repo.open_issues_count > 0) {
          results.push({
            url: `${repo.html_url}/issues`,
            title: `${repo.full_name} - Issues`,
            description: `${repo.open_issues_count} open issues`,
            metadata: { ...baseMetadata, content_type: 'issues' },
          });

          results.push({
            url: `${repo.html_url}/issues`,
            title: `${repo.full_name} - Issue Comments`,
            description: `Comments on ${repo.open_issues_count} issues. Requires 'Issues' crawler first.`,
            metadata: {
              ...baseMetadata,
              content_type: 'issue_comments',
              requires_parent: 'issues',
            },
          });
        }

        if (!repo.archived) {
          results.push({
            url: `${repo.html_url}/pulls`,
            title: `${repo.full_name} - Pull Requests`,
            description: 'Track PR content and code review discussions',
            metadata: { ...baseMetadata, content_type: 'pull_requests' },
          });

          results.push({
            url: `${repo.html_url}/pulls`,
            title: `${repo.full_name} - PR Comments`,
            description: "Comments on pull requests. Requires 'Pull Requests' crawler first.",
            metadata: {
              ...baseMetadata,
              content_type: 'pr_comments',
              requires_parent: 'pull_requests',
            },
          });
        }

        if (repo.has_discussions) {
          results.push({
            url: `${repo.html_url}/discussions`,
            title: `${repo.full_name} - Discussions`,
            description: 'Community discussions and Q&A',
            metadata: { ...baseMetadata, content_type: 'discussions' },
          });

          results.push({
            url: `${repo.html_url}/discussions`,
            title: `${repo.full_name} - Discussion Comments`,
            description: "Comments on discussions. Requires 'Discussions' crawler first.",
            metadata: {
              ...baseMetadata,
              content_type: 'discussion_comments',
              requires_parent: 'discussions',
            },
          });
        }
      }

      return results;
    } catch (error) {
      logger.error({ error }, 'GitHub search error:');
      return [];
    }
  }
}
