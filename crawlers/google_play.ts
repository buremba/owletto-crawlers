/**
 * Google Play Store Crawler
 * Fetches app reviews using google-play-scraper npm package
 */

import { type Static, Type } from '@owletto/sdk';
import gplay from 'npm:google-play-scraper@10.1.2';
import { logger } from '@owletto/sdk';
import type { Checkpoint, Content, CrawlResult, Env, SearchResult } from '@owletto/sdk';
import { BaseCrawler, calculateEngagementScore } from '@owletto/sdk';

/**
 * Google Play Store-specific options schema
 */
export const GooglePlayOptionsSchema = Type.Object(
  {
    app_id: Type.String({
      description: 'Google Play package name (e.g., "com.spotify.music")',
      minLength: 1,
      pattern: '^[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)+$',
    }),
    country: Type.Optional(
      Type.String({
        description: 'ISO country code (default: "us")',
        minLength: 2,
        maxLength: 2,
        default: 'us',
      })
    ),
    lang: Type.Optional(
      Type.String({
        description: 'Language code (default: "en")',
        minLength: 2,
        maxLength: 5,
        default: 'en',
      })
    ),
  },
  {
    description: 'Google Play Store crawler options',
    $id: 'GooglePlayOptions',
  }
);

export type GooglePlayOptions = Static<typeof GooglePlayOptionsSchema>;

interface GooglePlayCheckpoint extends Checkpoint {
  last_review_id?: string;
  pagination_token?: string;
}

export class GooglePlayCrawler extends BaseCrawler {
  readonly type = 'google_play';
  readonly displayName = 'Google Play Store';
  readonly apiType = 'api' as const;
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = GooglePlayOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.4, // Google Play has limited engagement (helpful votes)
    inverse_rating_weight: 0.3, // Prioritize critical reviews
    content_length_weight: 0.3, // Detailed reviews are valuable
    platform_weight: 1.0,
  };

  // Google Play: Inverse rating + thumbs_up engagement + content depth
  readonly defaultScoringFormula = `
    (5.0 - COALESCE((f.metadata->>'rating')::numeric, 3)) / 4.0 * 100 * 0.3 +
    PERCENT_RANK() OVER (PARTITION BY f.source_id ORDER BY COALESCE((f.metadata->>'thumbs_up')::numeric, 0)) * 100 * 0.4 +
    LEAST(f.content_length / 20.0, 100) * 0.3
  `;

  getRateLimit() {
    return {
      requests_per_minute: 20,
      recommended_interval_ms: 3000, // 3 seconds between requests
    };
  }

  validateOptions(options: GooglePlayOptions): string | null {
    if (!options.app_id) {
      return 'app_id (package name) is required';
    }
    return null;
  }

  urlFromOptions(options: GooglePlayOptions): string {
    if (options.app_id) {
      return `https://play.google.com/store/apps/details?id=${options.app_id}`;
    }
    return '';
  }

  displayLabelFromOptions(options: GooglePlayOptions): string {
    return options.app_id || 'Google Play';
  }

  async pull(
    options: GooglePlayOptions,
    checkpoint: GooglePlayCheckpoint | null,
    _env: Env,
    _updateCheckpointFn?: (checkpoint: Checkpoint) => Promise<void>
  ): Promise<CrawlResult> {
    try {
      const { app_id, country = 'us', lang = 'en' } = options;

      // Fetch reviews using google-play-scraper
      const MAX_REVIEWS = 500;
      const allReviews: any[] = [];
      let nextPaginationToken = checkpoint?.pagination_token;

      // Fetch pages until we have enough or hit checkpoint
      while (allReviews.length < MAX_REVIEWS) {
        const reviewsResult = await gplay.reviews({
          appId: app_id,
          sort: (gplay as any).sort?.NEWEST || 1,
          num: 150,
          paginate: true,
          nextPaginationToken,
          lang,
          country,
        });

        if (!reviewsResult.data || reviewsResult.data.length === 0) {
          break;
        }

        // Check if we've hit the checkpoint
        if (checkpoint?.last_timestamp) {
          const newReviews = reviewsResult.data.filter(
            (r: any) =>
              new Date(r.date).getTime() >
              (checkpoint.last_timestamp ? checkpoint.last_timestamp.getTime() : 0)
          );
          allReviews.push(...newReviews);
          if (newReviews.length < reviewsResult.data.length) {
            break;
          }
        } else {
          allReviews.push(...reviewsResult.data);
        }

        nextPaginationToken = reviewsResult.nextPaginationToken;
        if (!nextPaginationToken) {
          break;
        }

        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      // Transform to Content format
      const contents: Content[] = allReviews.map((review) => {
        const reviewId = review.id || '';
        const rating = review.score || 0;
        const content = review.text || '';
        const author = review.userName || 'Anonymous';
        const publishedAt = review.date ? new Date(review.date) : new Date();

        const engagementData = {
          rating,
          thumbs_up: review.thumbsUp || 0,
          reply_count: review.replyDate ? 1 : 0,
        };

        return {
          external_id: reviewId,
          content,
          author,
          published_at: publishedAt,
          score: calculateEngagementScore('google_play', engagementData),
          url: `https://play.google.com/store/apps/details?id=${app_id}&reviewId=${reviewId}`,
          metadata: {
            ...engagementData,
            version: review.version,
            criteria: review.criteria,
            reply: review.replyText,
            reply_date: review.replyDate,
          },
        };
      });

      // Sort by published date descending
      contents.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

      const newCheckpoint: GooglePlayCheckpoint =
        contents.length > 0
          ? {
              last_review_id: contents[0].external_id,
              last_timestamp: contents[0].published_at,
              pagination_token: nextPaginationToken || undefined,
              updated_at: new Date(),
            }
          : {
              last_review_id: checkpoint?.last_review_id || '',
              last_timestamp: checkpoint?.last_timestamp || new Date(),
              pagination_token: checkpoint?.pagination_token,
              updated_at: new Date(),
            };

      return {
        contents,
        checkpoint: newCheckpoint,
        metadata: {
          items_found: allReviews.length,
          items_skipped: 0,
        },
      };
    } catch (error) {
      throw new Error(
        `Google Play crawler failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async search(searchTerm: string, _env: Env): Promise<SearchResult[]> {
    try {
      const results = await gplay.search({
        term: searchTerm,
        num: 5,
      });

      return results.slice(0, 5).map((app: any) => ({
        url: app.url || `https://play.google.com/store/apps/details?id=${app.appId}`,
        title: app.title || '',
        description: `${app.developer || ''} - ${(app.summary || '').substring(0, 150)}`,
        metadata: {
          app_id: app.appId,
          country: 'us',
        },
      }));
    } catch (error) {
      logger.error({ error }, 'Google Play search error:');
      return [];
    }
  }
}
