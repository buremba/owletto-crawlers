/**
 * Google Maps Connector (V1 runtime)
 *
 * Fetches business reviews using Google Places API.
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
} from '@owletto/sdk';

interface GMapsReview {
  author_name: string;
  author_url?: string;
  profile_photo_url?: string;
  rating: number;
  relative_time_description?: string;
  text: string;
  time: number;
}

interface PlaceDetailsResponse {
  status: string;
  result?: {
    name?: string;
    reviews?: GMapsReview[];
    url?: string;
  };
}

interface FindPlaceResponse {
  candidates?: Array<{ place_id: string }>;
}

interface GMapsCheckpoint {
  last_timestamp?: string;
}

const configSchema = {
  type: 'object',
  properties: {
    place_id: {
      type: 'string',
      description: 'Google Place ID',
    },
    business_name: {
      type: 'string',
      description: 'Business name for search-based fallback',
    },
  },
};

export default class GoogleMapsConnector extends ConnectorRuntime {
  readonly definition: ConnectorDefinition = {
    key: 'gmaps',
    name: 'Google Maps',
    description: 'Fetches business reviews using Google Places API.',
    version: '1.0.0',
    authSchema: {
      methods: [
        {
          type: 'env_keys',
          required: true,
          fields: [
            {
              key: 'GOOGLE_MAPS_API_KEY',
              label: 'Google Maps API Key',
              secret: true,
            },
          ],
        },
      ],
    },
    feeds: {
      reviews: {
        key: 'reviews',
        name: 'Business Reviews',
        description: 'Fetch reviews for a business on Google Maps.',
        configSchema,
      },
    },
    optionsSchema: configSchema,
  };

  async sync(ctx: SyncContext): Promise<SyncResult> {
    const apiKey = ctx.config.GOOGLE_MAPS_API_KEY as string | undefined;
    if (!apiKey) {
      throw new Error('GOOGLE_MAPS_API_KEY is required');
    }

    let placeId = ctx.config.place_id as string | undefined;
    const businessName = ctx.config.business_name as string | undefined;

    if (!placeId && !businessName) {
      throw new Error('Either place_id or business_name is required');
    }

    // If no place_id, search by business name
    if (!placeId && businessName) {
      const searchUrl = `https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=${encodeURIComponent(businessName)}&inputtype=textquery&fields=place_id&key=${apiKey}`;
      const searchResponse = await fetch(searchUrl);
      if (!searchResponse.ok) {
        throw new Error(`Google Places search failed (${searchResponse.status}): ${await searchResponse.text()}`);
      }
      const searchData = (await searchResponse.json()) as FindPlaceResponse;
      if (!searchData.candidates || searchData.candidates.length === 0) {
        throw new Error(`Business not found: ${businessName}`);
      }
      placeId = searchData.candidates[0].place_id;
    }

    // Fetch place details with reviews
    const detailsUrl = `https://maps.googleapis.com/maps/api/place/details/json?place_id=${placeId}&fields=name,reviews,url&key=${apiKey}`;
    const detailsResponse = await fetch(detailsUrl);
    if (!detailsResponse.ok) {
      throw new Error(`Google Places details failed (${detailsResponse.status}): ${await detailsResponse.text()}`);
    }
    const data = (await detailsResponse.json()) as PlaceDetailsResponse;

    if (data.status !== 'OK') {
      throw new Error(`Google Places API error: ${data.status}`);
    }

    const place = data.result;
    const reviews = place?.reviews ?? [];
    const placeUrl = place?.url ?? `https://maps.google.com/?q=place_id:${placeId}`;

    // Transform reviews to EventEnvelope[]
    let events: EventEnvelope[] = reviews.map((review) => ({
      external_id: `${placeId}_${review.time}`,
      content: review.text || '',
      author: review.author_name || 'Anonymous',
      published_at: new Date(review.time * 1000),
      url: placeUrl,
      metadata: {
        rating: review.rating,
        author_url: review.author_url,
        profile_photo_url: review.profile_photo_url,
        relative_time_description: review.relative_time_description,
      },
    }));

    // Filter by checkpoint
    const checkpoint = ctx.checkpoint as GMapsCheckpoint | null;
    if (checkpoint?.last_timestamp) {
      const lastTimestamp = new Date(checkpoint.last_timestamp);
      events = events.filter((e) => e.published_at > lastTimestamp);
    }

    // Sort descending by published_at
    events.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

    const newCheckpoint: Record<string, unknown> =
      events.length > 0
        ? { last_timestamp: events[0].published_at.toISOString() }
        : { last_timestamp: checkpoint?.last_timestamp ?? null };

    return {
      events,
      checkpoint: newCheckpoint,
      metadata: {
        items_found: reviews.length,
      },
    };
  }

  async execute(_ctx: ActionContext): Promise<ActionResult> {
    return { success: false, error: 'Actions not supported' };
  }
}
