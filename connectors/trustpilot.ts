/**
 * Trustpilot Crawler
 * Scrapes business reviews from Trustpilot using Playwright
 */

import { type Static, Type } from '@owletto/sdk';
import type { Page } from 'playwright';
import { logger } from '@owletto/sdk';
import type { Content, CrawlerOptions, Env, SearchResult } from '@owletto/sdk';
import { calculateEngagementScore } from '@owletto/sdk';
import {
  type BrowserCrawlerConfig,
  BrowserPaginatedCrawler,
  type BrowserPaginationConfig,
} from '@owletto/sdk';
import type { PaginatedCheckpoint } from '@owletto/sdk';
import { captureErrorArtifacts, launchBrowser } from '@owletto/sdk';

/**
 * Trustpilot-specific options schema
 */
export const TrustpilotOptionsSchema = Type.Object(
  {
    business_url: Type.Optional(
      Type.String({
        description:
          'Full Trustpilot review URL (e.g., "https://www.trustpilot.com/review/spotify.com")',
        format: 'uri',
      })
    ),
    business_name: Type.Optional(
      Type.String({
        description: 'Business name for search-based lookup',
        minLength: 1,
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
    description: 'Trustpilot crawler options - requires either business_url or business_name',
    $id: 'TrustpilotOptions',
  }
);

export type TrustpilotOptions = Static<typeof TrustpilotOptionsSchema>;

interface TrustpilotCheckpoint extends PaginatedCheckpoint {
  last_review_id?: string;
  last_page?: number;
}

/**
 * Raw review data extracted from Trustpilot page
 */
interface TrustpilotReview {
  id: string;
  rating: number;
  title: string;
  text: string;
  date: string;
  author: string;
}

export class TrustpilotCrawler extends BrowserPaginatedCrawler<
  TrustpilotReview,
  TrustpilotCheckpoint
> {
  readonly type = 'trustpilot';
  readonly displayName = 'Trustpilot';
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = TrustpilotOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.4, // Trustpilot has limited engagement (helpful votes)
    inverse_rating_weight: 0.3, // Prioritize critical reviews
    content_length_weight: 0.3, // Detailed reviews are valuable
    platform_weight: 1.0,
  };

  // Trustpilot: Inverse rating + engagement + depth
  readonly defaultScoringFormula = `
    (5.0 - COALESCE((f.metadata->>'rating')::numeric, 3)) / 4.0 * 100 * 0.3 +
    PERCENT_RANK() OVER (PARTITION BY f.source_id ORDER BY f.score) * 100 * 0.4 +
    LEAST(f.content_length / 20.0, 100) * 0.3
  `;

  getRateLimit() {
    return {
      requests_per_minute: 15,
      recommended_interval_ms: 4000, // 4 seconds between requests
    };
  }

  validateOptions(options: TrustpilotOptions): string | null {
    if (!options.business_url && !options.business_name) {
      return 'Either business_url or business_name required';
    }
    return null;
  }

  urlFromOptions(options: TrustpilotOptions): string {
    return options.business_url || `https://www.trustpilot.com/review/${options.business_name}`;
  }

  displayLabelFromOptions(options: TrustpilotOptions): string {
    return `${options.business_name || options.business_url} Reviews`;
  }

  // ============ BrowserPaginatedCrawler Abstract Methods ============

  protected getBrowserConfig(): BrowserCrawlerConfig {
    return {
      stealth: true,
      waitUntil: 'domcontentloaded',
      navigationTimeout: 30000,
      cookieConsent: {
        bannerSelectors: ['[data-cookie-consent-banner]'],
        acceptSelectors: ['button[data-cookie-consent-accept]'],
        timeout: 2000,
      },
      captcha: {
        enabled: true,
        selectors: ['#captcha-container', '.g-recaptcha'],
        textPatterns: ['unusual traffic'],
      },
    };
  }

  protected getBrowserPaginationConfig(): BrowserPaginationConfig {
    return {
      maxPages: 50,
      pageSize: 20, // Trustpilot shows ~20 reviews per page
      rateLimitMs: 4000,
      incrementalCheckpoint: false,
      pagesPerRun: 1, // Single page per run
      pageDelayMs: 2000,
    };
  }

  protected getBaseUrl(options: CrawlerOptions): string {
    const opts = options as TrustpilotOptions;
    return opts.business_url || `https://www.trustpilot.com/review/${opts.business_name}`;
  }

  protected buildPageUrl(baseUrl: string, pageNumber: number): string {
    return pageNumber === 1 ? baseUrl : `${baseUrl}?page=${pageNumber}`;
  }

  protected async waitForContent(page: Page): Promise<void> {
    try {
      await page.waitForSelector('[data-service-review-card-paper]', { timeout: 10000 });
    } catch (_error) {
      // No reviews found - might be empty page
      logger.warn(`[${this.type}] No review cards found on page`);
    }
  }

  protected async extractItems(page: Page): Promise<TrustpilotReview[]> {
    return page.evaluate(() => {
      const reviewElements = Array.from(
        document.querySelectorAll('[data-service-review-card-paper]')
      );

      return reviewElements.map((el: Element) => {
        const ratingElement = el.querySelector('[data-service-review-rating]');
        const titleElement = el.querySelector('[data-service-review-title-typography]');
        const textElement = el.querySelector('[data-service-review-text-typography]');
        const dateElement = el.querySelector('time');
        const authorElement = el.querySelector('[data-consumer-name-typography]');

        // Extract rating from data-service-review-rating attribute
        const rating = parseInt(
          ratingElement?.getAttribute('data-service-review-rating') || '0',
          10
        );

        return {
          id: ratingElement?.getAttribute('data-service-review-rating') || '',
          rating,
          title: titleElement?.textContent?.trim() || '',
          text: textElement?.textContent?.trim() || '',
          date: dateElement?.getAttribute('datetime') || '',
          author: authorElement?.textContent?.trim() || '',
        };
      });
    });
  }

  protected transformItem(item: TrustpilotReview, options: CrawlerOptions): Content {
    const url = this.getBaseUrl(options);
    const engagementData = {
      rating: item.rating,
      helpful_count: 0, // Trustpilot doesn't expose this easily
    };

    // Include title in content if available
    const fullContent = item.title ? `${item.title}\n\n${item.text}` : item.text;

    return {
      external_id: `${item.date}-${item.author}`, // Use date + author as unique ID
      content: fullContent,
      author: item.author,
      published_at: new Date(item.date),
      score: calculateEngagementScore('trustpilot', engagementData),
      url: url,
      metadata: {
        rating: item.rating,
        helpful_count: 0,
        title: item.title,
      },
    };
  }

  protected getItemDate(item: TrustpilotReview): Date {
    return new Date(item.date);
  }

  protected filterItem(item: TrustpilotReview, _options: CrawlerOptions): boolean {
    // Only include reviews with actual content (more than 10 chars)
    return Boolean(item.text && item.text.length > 10);
  }

  // ============ Search Method (not part of pagination) ============

  /**
   * Search Trustpilot for a company/brand
   * Uses browser rendering to search for business profiles
   */
  async search(searchTerm: string, env: Env): Promise<SearchResult[]> {
    // Use stealth mode to avoid detection
    const { browser, screenshotDir } = await launchBrowser(env, { stealth: true });
    const page = await browser.newPage();

    try {
      const searchUrl = `https://www.trustpilot.com/search?query=${encodeURIComponent(searchTerm)}`;
      await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

      const results = await page.evaluate(() => {
        const businessCards = Array.from(
          document.querySelectorAll('[data-business-unit-card]')
        ).slice(0, 5);

        return businessCards
          .map((card: Element) => {
            const linkElement = card.querySelector('a[href*="/review/"]');
            const nameElement = card.querySelector('[data-business-unit-title]');
            const descElement = card.querySelector('[data-business-unit-description]');
            const url = linkElement
              ? `https://www.trustpilot.com${linkElement.getAttribute('href')}`
              : '';

            return {
              url,
              title: nameElement?.textContent?.trim() || '',
              description: descElement?.textContent?.trim() || 'Business on Trustpilot',
              metadata: {
                business_url: url,
              },
            };
          })
          .filter((r) => r.url && r.title);
      });

      await browser.close();
      return results;
    } catch (error: any) {
      // Capture error artifacts for debugging
      await captureErrorArtifacts(page, error, 'trustpilot-search', screenshotDir);
      logger.error({ error }, 'Trustpilot search error:');
      await browser.close();
      return [];
    }
  }
}
