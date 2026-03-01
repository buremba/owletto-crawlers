/**
 * Trustpilot Connector (V1 runtime)
 *
 * Scrapes business reviews from Trustpilot using Playwright.
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
  launchBrowser,
  captureErrorArtifacts,
} from '@owletto/sdk';
import type { Page } from 'playwright';

interface TrustpilotReview {
  rating: number;
  title: string;
  text: string;
  date: string;
  author: string;
}

interface TrustpilotCheckpoint {
  last_sync_at?: string;
  last_page?: number;
}

const configSchema = {
  type: 'object',
  properties: {
    business_url: {
      type: 'string',
      format: 'uri',
      description:
        'Full Trustpilot review URL (e.g., "https://www.trustpilot.com/review/spotify.com")',
    },
    business_name: {
      type: 'string',
      minLength: 1,
      description: 'Business name for search-based lookup',
    },
    lookback_days: {
      type: 'integer',
      minimum: 1,
      maximum: 730,
      default: 365,
      description:
        'Number of days to look back for historical data. Default: 365 (1 year). Maximum: 730 (2 years).',
    },
  },
};

export default class TrustpilotConnector extends ConnectorRuntime {
  readonly definition: ConnectorDefinition = {
    key: 'trustpilot',
    name: 'Trustpilot',
    description: 'Scrapes business reviews from Trustpilot.',
    version: '1.0.0',
    authSchema: {
      methods: [{ type: 'none' }],
    },
    feeds: {
      reviews: {
        key: 'reviews',
        name: 'Business Reviews',
        description: 'Scrape reviews for a business on Trustpilot.',
        configSchema,
      },
    },
    optionsSchema: configSchema,
  };

  async sync(ctx: SyncContext): Promise<SyncResult> {
    const businessUrl = ctx.config.business_url as string | undefined;
    const businessName = ctx.config.business_name as string | undefined;

    if (!businessUrl && !businessName) {
      throw new Error('Either business_url or business_name is required');
    }

    const baseUrl =
      businessUrl || `https://www.trustpilot.com/review/${businessName}`;

    const { browser, screenshotDir } = await launchBrowser({} as any, {
      stealth: true,
    });
    const page = await browser.newPage();

    try {
      await page.goto(baseUrl, {
        waitUntil: 'domcontentloaded',
        timeout: 30000,
      });

      // Handle cookie consent banner
      try {
        const cookieButton = await page.waitForSelector(
          '[data-cookie-consent-accept]',
          { timeout: 2000 },
        );
        if (cookieButton) {
          await cookieButton.click();
        }
      } catch {
        // No cookie banner found, continue
      }

      // Wait for review cards to load
      try {
        await page.waitForSelector('[data-service-review-card-paper]', {
          timeout: 10000,
        });
      } catch {
        // No reviews found on page
        await browser.close();
        return {
          events: [],
          checkpoint: {
            last_sync_at: new Date().toISOString(),
            last_page: 1,
          },
          metadata: { items_found: 0 },
        };
      }

      // Extract raw reviews from the page
      const rawReviews = await page.evaluate(() => {
        const reviewElements = Array.from(
          document.querySelectorAll('[data-service-review-card-paper]'),
        );

        return reviewElements.map((el: Element) => {
          const ratingElement = el.querySelector(
            '[data-service-review-rating]',
          );
          const titleElement = el.querySelector(
            '[data-service-review-title-typography]',
          );
          const textElement = el.querySelector(
            '[data-service-review-text-typography]',
          );
          const dateElement = el.querySelector('time');
          const authorElement = el.querySelector(
            '[data-consumer-name-typography]',
          );

          const rating = parseInt(
            ratingElement?.getAttribute('data-service-review-rating') || '0',
            10,
          );

          return {
            rating,
            title: titleElement?.textContent?.trim() || '',
            text: textElement?.textContent?.trim() || '',
            date: dateElement?.getAttribute('datetime') || '',
            author: authorElement?.textContent?.trim() || '',
          };
        });
      });

      await browser.close();

      // Filter reviews with meaningful content (more than 10 chars)
      const reviews: TrustpilotReview[] = rawReviews.filter(
        (r) => r.text && r.text.length > 10,
      );

      // Transform to EventEnvelope format
      const events: EventEnvelope[] = reviews.map((review) => {
        const content = review.title
          ? `${review.title}\n\n${review.text}`
          : review.text;

        return {
          external_id: `${review.date}-${review.author}`,
          content,
          author: review.author,
          published_at: new Date(review.date),
          score: calculateEngagementScore('trustpilot', {
            rating: review.rating,
            helpful_count: 0,
          }),
          url: baseUrl,
          metadata: {
            rating: review.rating,
            helpful_count: 0,
            title: review.title,
          },
        };
      });

      return {
        events,
        checkpoint: {
          last_sync_at: new Date().toISOString(),
          last_page: 1,
        } as Record<string, unknown>,
        metadata: {
          items_found: reviews.length,
        },
      };
    } catch (error: any) {
      await captureErrorArtifacts(
        page,
        error,
        'trustpilot-sync',
        screenshotDir,
      );
      await browser.close();
      throw error;
    }
  }

  async execute(_ctx: ActionContext): Promise<ActionResult> {
    return { success: false, error: 'Actions not supported' };
  }
}
