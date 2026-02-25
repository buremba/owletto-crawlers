/**
 * iOS App Store Crawler
 * Fetches app reviews using iTunes Search API and RSS feeds
 */

import { type Static, Type } from '@owletto/sdk';
import { logger } from '@owletto/sdk';
import type { Checkpoint, Content, CrawlResult, Env, SearchResult } from '@owletto/sdk';
import { BaseCrawler, calculateEngagementScore } from '@owletto/sdk';

// Apple blocks Workers - use native fetch with browser-like headers
const IOS_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  Accept: 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9',
  Referer: 'https://apps.apple.com/',
};

/**
 * iOS App Store-specific options schema
 */
export const IOSAppStoreOptionsSchema = Type.Object(
  {
    app_id: Type.String({
      description: 'iOS App Store ID (e.g., "324684580" for Spotify)',
      minLength: 1,
    }),
    country: Type.String({
      description: 'ISO country code (e.g., "US", "GB")',
      minLength: 2,
      maxLength: 2,
      pattern: '^[A-Z]{2}$',
    }),
    app_name: Type.Optional(
      Type.String({
        description: 'App name for fallback search if app_id lookup fails',
        minLength: 1,
      })
    ),
  },
  {
    description: 'iOS App Store crawler options',
    $id: 'IOSAppStoreOptions',
  }
);

export type IOSAppStoreOptions = Static<typeof IOSAppStoreOptionsSchema>;

interface IOSCheckpoint extends Checkpoint {
  last_review_id?: string;
  last_sort_order?: string;
}

export class IOSAppStoreCrawler extends BaseCrawler {
  readonly type = 'ios_appstore';
  readonly displayName = 'iOS App Store';
  readonly apiType = 'browser' as const; // Apple blocks CF Workers IPs
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = IOSAppStoreOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.4, // iOS App Store has limited engagement (helpful votes)
    inverse_rating_weight: 0.3, // Prioritize critical reviews
    content_length_weight: 0.3, // Detailed reviews are valuable
    platform_weight: 1.0,
  };

  // iOS App Store: Inverse rating + engagement (vote_sum) + content depth
  readonly defaultScoringFormula = `
    (5.0 - COALESCE((f.metadata->>'rating')::numeric, 3)) / 4.0 * 100 * 0.3 +
    PERCENT_RANK() OVER (PARTITION BY f.source_id ORDER BY COALESCE((f.metadata->>'vote_sum')::numeric, 0)) * 100 * 0.4 +
    LEAST(f.content_length / 20.0, 100) * 0.3
  `;

  getRateLimit() {
    return {
      requests_per_minute: 20,
      recommended_interval_ms: 3000, // 3 seconds between requests
    };
  }

  validateOptions(options: IOSAppStoreOptions): string | null {
    if (!options.app_id) {
      return 'app_id is required';
    }
    if (!options.country) {
      return 'country is required (e.g., "US")';
    }
    return null;
  }

  urlFromOptions(options: IOSAppStoreOptions): string {
    if (options.app_id && options.country) {
      return `https://apps.apple.com/${options.country.toLowerCase()}/app/id${options.app_id}`;
    }
    return '';
  }

  displayLabelFromOptions(options: IOSAppStoreOptions): string {
    return `App ID ${options.app_id} (${options.country})`;
  }

  async pull(
    options: IOSAppStoreOptions,
    checkpoint: IOSCheckpoint | null,
    _env: Env,
    _updateCheckpointFn?: (checkpoint: Checkpoint) => Promise<void>
  ): Promise<CrawlResult> {
    try {
      // Apply defaults for optional fields
      const { app_id, country = 'US' } = options;

      // Generate app URL directly (avoid lookup API that's blocked by Apple)
      const appUrl = `https://apps.apple.com/${country.toLowerCase()}/app/id${app_id}`;

      // Fetch reviews via RSS feed with pagination
      // Apple allows up to 10 pages (500 reviews max)
      const MAX_PAGES = 10;
      const allReviews: any[] = [];
      let page = 1;
      let shouldContinue = true;

      while (shouldContinue && page <= MAX_PAGES) {
        const rssUrl = `https://itunes.apple.com/${country}/rss/customerreviews/page=${page}/id=${app_id}/sortby=mostrecent/json`;

        const rssResponse = await fetch(rssUrl, { headers: IOS_HEADERS });
        if (!rssResponse.ok) {
          if (page === 1) {
            throw new Error(`RSS feed returned ${rssResponse.status}: ${rssUrl}`);
          }
          // No more pages available
          break;
        }

        let rssData: { feed?: { entry?: any[] } };
        try {
          rssData = await rssResponse.json();
        } catch (_e) {
          if (page === 1) {
            const text = await rssResponse.text();
            throw new Error(`RSS feed returned invalid JSON: ${text.substring(0, 100)}`);
          }
          break;
        }

        // Handle both array and single entry responses
        const rawEntries = rssData.feed?.entry;
        const feedEntries = Array.isArray(rawEntries) ? rawEntries : rawEntries ? [rawEntries] : [];
        if (feedEntries.length === 0) {
          break;
        }

        // Filter out the first entry on page 1 (app metadata) - only if it exists and is not a review
        const reviews = feedEntries.filter((entry: any, index: number) => {
          // Skip first entry on page 1 if it's app metadata (no rating)
          if (page === 1 && index === 0 && !entry['im:rating']) {
            return false;
          }
          return entry['im:rating'];
        });

        if (reviews.length === 0) {
          break;
        }

        // Check if we've hit the checkpoint (already seen content)
        if (checkpoint?.last_timestamp) {
          const oldestReviewDate = new Date(
            reviews[reviews.length - 1].updated?.label || Date.now()
          );
          if (oldestReviewDate <= checkpoint.last_timestamp) {
            // Add reviews newer than checkpoint and stop
            allReviews.push(
              ...reviews.filter((r: any) => new Date(r.updated?.label) > checkpoint.last_timestamp!)
            );
            shouldContinue = false;
            break;
          }
        }

        allReviews.push(...reviews);
        page++;

        // Small delay between pages to be respectful
        if (shouldContinue && page <= MAX_PAGES) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }

      const reviews = allReviews;

      // Transform to Content format
      const contents: Content[] = reviews.map((review: any) => {
        const reviewId = review.id?.label || '';
        const rating = parseInt(review['im:rating']?.label || '0', 10);
        const content = review.content?.label || '';
        const author = review.author?.name?.label || 'Anonymous';
        const publishedAt = new Date(review.updated?.label || Date.now());
        const title = review.title?.label || '';

        const engagementData = {
          rating,
        };

        return {
          external_id: reviewId,
          content: title ? `${title}\n\n${content}` : content,
          author,
          published_at: publishedAt,
          score: calculateEngagementScore('ios_appstore', engagementData),
          url: review.link?.attributes?.href || appUrl,
          metadata: {
            ...engagementData,
            version: review['im:version']?.label,
            vote_sum: parseInt(review['im:voteSum']?.label || '0', 10),
            vote_count: parseInt(review['im:voteCount']?.label || '0', 10),
          },
        };
      });

      // Filter out already seen content
      const newContents = checkpoint
        ? contents.filter((c) => c.published_at > (checkpoint.last_timestamp || new Date(0)))
        : contents;

      // Sort by published date descending
      newContents.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

      const newCheckpoint: IOSCheckpoint =
        newContents.length > 0
          ? {
              last_review_id: newContents[0].external_id,
              last_timestamp: newContents[0].published_at,
              last_sort_order: 'mostrecent',
              updated_at: new Date(),
            }
          : {
              last_review_id: checkpoint?.last_review_id || '',
              last_timestamp: checkpoint?.last_timestamp || new Date(),
              last_sort_order: checkpoint?.last_sort_order || 'mostrecent',
              updated_at: new Date(),
            };

      return {
        contents: newContents,
        checkpoint: newCheckpoint,
        metadata: {
          items_found: reviews.length,
          items_skipped: reviews.length - newContents.length,
        },
      };
    } catch (error) {
      throw new Error(
        `iOS App Store crawler failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Search iOS App Store for apps
   * Uses iTunes Search API to find apps by name
   */
  async search(searchTerm: string, _env: Env): Promise<SearchResult[]> {
    try {
      const searchUrl = `https://itunes.apple.com/search?term=${encodeURIComponent(
        searchTerm
      )}&entity=software&limit=5`;

      const response = await fetch(searchUrl, { headers: IOS_HEADERS });
      if (!response.ok) {
        logger.error({ status: response.status }, 'iOS App Store search failed');
        return [];
      }

      const data: {
        results?: Array<{
          trackId: number;
          trackName: string;
          artistName: string;
          description: string;
          trackViewUrl: string;
        }>;
      } = await response.json();

      const results: SearchResult[] = (data.results || []).slice(0, 5).map((app) => ({
        url: app.trackViewUrl,
        title: app.trackName,
        description: `${app.artistName} - ${(app.description || '').substring(0, 150)}`,
        metadata: {
          app_id: app.trackId.toString(),
          country: 'us', // Default country for search
        },
      }));

      return results;
    } catch (error) {
      logger.error({ error }, 'iOS App Store search error:');
      return [];
    }
  }
}
