/**
 * Google Maps Crawler
 * Fetches business reviews using Google Places API
 */

import { type Static, Type } from '@sinclair/typebox';
import logger from '@/utils/logger';
import type { Checkpoint, Content, CrawlResult, Env, SearchResult } from './base';
import { BaseCrawler, calculateEngagementScore } from './base';
import { httpClient } from './http';

/**
 * Google Maps-specific options schema
 */
export const GoogleMapsOptionsSchema = Type.Object(
  {
    place_id: Type.Optional(
      Type.String({
        description: 'Google Place ID for the business',
        minLength: 1,
      })
    ),
    business_name: Type.Optional(
      Type.String({
        description: 'Business name for search-based fallback if place_id not provided',
        minLength: 1,
      })
    ),
  },
  {
    description: 'Google Maps crawler options - requires either place_id or business_name',
    $id: 'GoogleMapsOptions',
  }
);

export type GoogleMapsOptions = Static<typeof GoogleMapsOptionsSchema>;

interface GMapsCheckpoint extends Checkpoint {
  last_review_time?: number; // Unix timestamp
}

export class GoogleMapsCrawler extends BaseCrawler {
  readonly type = 'gmaps';
  readonly displayName = 'Google Maps';
  readonly apiType = 'api' as const;
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = GoogleMapsOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.0, // No engagement metrics on Google Maps
    inverse_rating_weight: 0.5, // Prioritize critical reviews (1-star > 5-star)
    content_length_weight: 0.5, // Detailed reviews are valuable
    platform_weight: 1.0,
  };

  // Google Maps: Inverse rating + content depth (no engagement metrics available)
  readonly defaultScoringFormula = `
    (5.0 - COALESCE((f.metadata->>'rating')::numeric, 3)) / 4.0 * 100 * 0.5 +
    LEAST(f.content_length / 20.0, 100) * 0.5
  `;

  getRateLimit() {
    return {
      requests_per_minute: 20,
      recommended_interval_ms: 3000, // 3 seconds between requests
    };
  }

  validateOptions(options: GoogleMapsOptions): string | null {
    if (!options.place_id && !options.business_name) {
      return 'Either place_id or business_name required';
    }
    return null;
  }

  urlFromOptions(options: GoogleMapsOptions): string {
    if (options.place_id) {
      return `https://www.google.com/maps/place/?q=place_id:${options.place_id}`;
    }
    if (options.business_name) {
      return `https://www.google.com/maps/search/${encodeURIComponent(options.business_name)}`;
    }
    return '';
  }

  displayLabelFromOptions(options: GoogleMapsOptions): string {
    return options.business_name || options.place_id || 'Google Maps';
  }

  async pull(
    options: GoogleMapsOptions,
    checkpoint: GMapsCheckpoint | null,
    env: Env,
    _updateCheckpointFn?: (checkpoint: Checkpoint) => Promise<void>
  ): Promise<CrawlResult> {
    const apiKey = env.GOOGLE_MAPS_API_KEY;
    if (!apiKey) {
      throw new Error('GOOGLE_MAPS_API_KEY not configured in environment');
    }

    try {
      // If no place_id provided, search for the business first
      let placeId = options.place_id;
      if (!placeId && options.business_name) {
        const searchUrl = `https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=${encodeURIComponent(
          options.business_name
        )}&inputtype=textquery&fields=place_id&key=${apiKey}`;

        const searchData = await httpClient.get(searchUrl).json<{
          candidates?: Array<{ place_id: string }>;
        }>();

        if (searchData.candidates && searchData.candidates.length > 0) {
          placeId = searchData.candidates[0].place_id;
        } else {
          throw new Error(`Business not found: ${options.business_name}`);
        }
      }

      // Fetch place details including reviews
      const detailsUrl = `https://maps.googleapis.com/maps/api/place/details/json?place_id=${placeId}&fields=name,reviews,url&key=${apiKey}`;

      const data = await httpClient.get(detailsUrl).json<{
        status: string;
        result?: {
          reviews?: any[];
          url?: string;
        };
      }>();

      if (data.status !== 'OK') {
        throw new Error(`Google Places API error: ${data.status}`);
      }

      const place = data.result;
      const reviews = place?.reviews || [];

      // Transform to Content format
      const contents: Content[] = reviews.map((review: any) => {
        const engagementData = {
          rating: review.rating,
          helpful_count: 0, // Google Maps doesn't provide this
        };
        return {
          external_id: `${placeId}_${review.time}`,
          content: review.text || '',
          author: review.author_name || 'Anonymous',
          published_at: new Date(review.time * 1000), // Convert Unix timestamp to Date
          score: calculateEngagementScore('gmaps', engagementData),
          url: place?.url || `https://maps.google.com/?q=place_id:${placeId}`,
          metadata: {
            ...engagementData,
            author_url: review.author_url,
            profile_photo_url: review.profile_photo_url,
            relative_time_description: review.relative_time_description,
          },
        };
      });

      // Filter out already seen content based on checkpoint
      const newContents = checkpoint
        ? contents.filter((c) => c.published_at > (checkpoint.last_timestamp || new Date(0)))
        : contents;

      // Sort by published date descending
      newContents.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

      const newCheckpoint: GMapsCheckpoint =
        newContents.length > 0
          ? {
              last_review_time: Math.floor(newContents[0].published_at.getTime() / 1000),
              last_timestamp: newContents[0].published_at,
              updated_at: new Date(),
            }
          : {
              last_review_time: checkpoint?.last_review_time,
              last_timestamp: checkpoint?.last_timestamp || new Date(),
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
        `Google Maps crawler failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Search Google Maps for a company/brand
   * Uses Places API text search to find businesses
   */
  async search(searchTerm: string, env: Env): Promise<SearchResult[]> {
    const apiKey = env.GOOGLE_MAPS_API_KEY;
    if (!apiKey) {
      logger.warn('GOOGLE_MAPS_API_KEY not configured, skipping Google Maps search');
      return [];
    }

    try {
      const searchUrl = `https://maps.googleapis.com/maps/api/place/textsearch/json?query=${encodeURIComponent(
        searchTerm
      )}&key=${apiKey}`;

      const data = await httpClient.get(searchUrl).json<{
        status: string;
        results?: Array<{
          place_id: string;
          name: string;
          formatted_address: string;
          types?: string[];
        }>;
      }>();

      if (data.status !== 'OK') {
        logger.warn(`Google Places search failed: ${data.status}`);
        return [];
      }

      const results: SearchResult[] = (data.results || []).slice(0, 5).map((place) => ({
        url: `https://maps.google.com/?q=place_id:${place.place_id}`,
        title: place.name,
        description: place.formatted_address || place.types?.join(', ') || 'Google Maps business',
        metadata: {
          place_id: place.place_id,
        },
      }));

      return results;
    } catch (error) {
      logger.error({ error }, 'Google Maps search error:');
      return [];
    }
  }
}
