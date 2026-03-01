/**
 * Google Play Store Connector (V1 runtime)
 *
 * Syncs app reviews from the Google Play Store.
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
import gplay from 'npm:google-play-scraper@10.1.2';

interface GooglePlayCheckpoint {
  last_timestamp?: string;
  pagination_token?: string;
}

export default class GooglePlayConnector extends ConnectorRuntime {
  readonly definition: ConnectorDefinition = {
    key: 'google_play',
    name: 'Google Play Store',
    description: 'Fetches app reviews from the Google Play Store.',
    version: '1.0.0',
    authSchema: {
      methods: [{ type: 'none' }],
    },
    feeds: {
      reviews: {
        key: 'reviews',
        name: 'App Reviews',
        description: 'Fetch reviews for an Android app.',
        configSchema: {
          type: 'object',
          required: ['app_id'],
          properties: {
            app_id: {
              type: 'string',
              minLength: 1,
              description: 'Google Play package name (e.g., "com.spotify.music")',
            },
            country: {
              type: 'string',
              minLength: 2,
              maxLength: 2,
              default: 'us',
              description: 'ISO country code',
            },
            lang: {
              type: 'string',
              minLength: 2,
              maxLength: 5,
              default: 'en',
              description: 'Language code',
            },
          },
        },
      },
    },
    optionsSchema: {
      type: 'object',
      required: ['app_id'],
      properties: {
        app_id: {
          type: 'string',
          minLength: 1,
          description: 'Google Play package name (e.g., "com.spotify.music")',
        },
        country: {
          type: 'string',
          minLength: 2,
          maxLength: 2,
          default: 'us',
          description: 'ISO country code',
        },
        lang: {
          type: 'string',
          minLength: 2,
          maxLength: 5,
          default: 'en',
          description: 'Language code',
        },
      },
    },
  };

  async sync(ctx: SyncContext): Promise<SyncResult> {
    const app_id = ctx.config.app_id as string;
    const country = (ctx.config.country as string) || 'us';
    const lang = (ctx.config.lang as string) || 'en';
    const checkpoint = (ctx.checkpoint ?? {}) as GooglePlayCheckpoint;

    const MAX_REVIEWS = 500;
    const allReviews: any[] = [];
    let nextPaginationToken: string | undefined = checkpoint.pagination_token;
    const lastTimestamp = checkpoint.last_timestamp
      ? new Date(checkpoint.last_timestamp).getTime()
      : null;
    let hitCheckpoint = false;

    while (allReviews.length < MAX_REVIEWS) {
      const reviewsResult = await gplay.reviews({
        appId: app_id,
        sort: 1,
        num: 150,
        paginate: true,
        nextPaginationToken,
        lang,
        country,
      });

      if (!reviewsResult.data || reviewsResult.data.length === 0) {
        break;
      }

      if (lastTimestamp) {
        for (const review of reviewsResult.data) {
          const reviewTime = new Date(review.date).getTime();
          if (reviewTime > lastTimestamp) {
            allReviews.push(review);
          } else {
            hitCheckpoint = true;
            break;
          }
        }
        if (hitCheckpoint) {
          break;
        }
      } else {
        allReviews.push(...reviewsResult.data);
      }

      nextPaginationToken = reviewsResult.nextPaginationToken;
      if (!nextPaginationToken) {
        break;
      }

      // Rate-limit delay between pagination requests
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    // Transform to EventEnvelope format
    const events: EventEnvelope[] = allReviews.map((review) => {
      const reviewId = review.id || '';
      const rating = review.score || 0;
      const thumbsUp = review.thumbsUp || 0;
      const replyCount = review.replyDate ? 1 : 0;

      return {
        external_id: reviewId,
        content: review.text || '',
        author: review.userName || 'Anonymous',
        published_at: review.date ? new Date(review.date) : new Date(),
        score: calculateEngagementScore('google_play', {
          rating,
          helpful_count: thumbsUp,
          reply_count: replyCount,
        }),
        url: `https://play.google.com/store/apps/details?id=${app_id}&reviewId=${reviewId}`,
        metadata: {
          rating,
          thumbs_up: thumbsUp,
          version: review.version,
          reply: review.replyText,
          reply_date: review.replyDate,
        },
      };
    });

    // Sort by published date descending
    events.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

    const newCheckpoint: GooglePlayCheckpoint =
      events.length > 0
        ? {
            last_timestamp: events[0].published_at.toISOString(),
            pagination_token: nextPaginationToken,
          }
        : {
            last_timestamp: checkpoint.last_timestamp,
            pagination_token: checkpoint.pagination_token,
          };

    return {
      events,
      checkpoint: newCheckpoint as Record<string, unknown>,
      metadata: {
        items_found: allReviews.length,
        items_skipped: 0,
      },
    };
  }

  async execute(_ctx: ActionContext): Promise<ActionResult> {
    return { success: false, error: 'Actions not supported' };
  }
}
