/**
 * Capterra Crawler
 * Scrapes software reviews from Capterra using browser rendering with stealth mode.
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
 * Capterra-specific options schema
 */
export const CapterraOptionsSchema = Type.Object(
  {
    vendor_name: Type.Optional(
      Type.String({
        description:
          'Vendor/company name (e.g., "Spotify AB"). Optional but recommended for disambiguation.',
        minLength: 1,
      })
    ),
    product_id: Type.String({
      description: 'Capterra product ID (e.g., "12345")',
      minLength: 1,
    }),
    product_name: Type.Optional(
      Type.String({
        description:
          'Product name slug for URL (e.g., "spotify"). Optional - Capterra will redirect without it.',
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
    description: 'Capterra crawler options',
    $id: 'CapterraOptions',
  }
);

export type CapterraOptions = Static<typeof CapterraOptionsSchema>;

interface CapterraCheckpoint extends PaginatedCheckpoint {
  last_review_id?: string;
  last_page?: number;
}

/**
 * Raw review data extracted from Capterra page
 */
interface CapterraReview {
  id: string;
  rating: number;
  title: string;
  text: string;
  date: string; // ISO date string
  author: string;
  helpfulCount: number;
}

export class CapterraCrawler extends BrowserPaginatedCrawler<CapterraReview, CapterraCheckpoint> {
  readonly type = 'capterra';
  readonly displayName = 'Capterra';
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = CapterraOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.4, // Capterra has limited engagement (helpful votes)
    inverse_rating_weight: 0.3, // Prioritize critical reviews
    content_length_weight: 0.3, // Detailed reviews are valuable
    platform_weight: 1.0,
  };

  // Capterra: Inverse rating + helpful votes + content depth
  readonly defaultScoringFormula = `
    (5.0 - COALESCE((f.metadata->>'rating')::numeric, 3)) / 4.0 * 100 * 0.3 +
    PERCENT_RANK() OVER (PARTITION BY f.source_id ORDER BY COALESCE((f.metadata->>'helpful_count')::numeric, 0)) * 100 * 0.4 +
    LEAST(f.content_length / 20.0, 100) * 0.3
  `;

  getRateLimit() {
    return {
      requests_per_minute: 10,
      recommended_interval_ms: 6000, // 6 seconds between requests
    };
  }

  validateOptions(options: CapterraOptions): string | null {
    if (!options.product_id) {
      return 'product_id is required';
    }
    return null;
  }

  urlFromOptions(options: CapterraOptions): string {
    // Use product_name if available for better URL, otherwise Capterra will redirect
    const baseUrl = options.product_name
      ? `https://www.capterra.com/p/${options.product_id}/${options.product_name}`
      : `https://www.capterra.com/p/${options.product_id}`;
    return `${baseUrl}/reviews`;
  }

  displayLabelFromOptions(options: CapterraOptions): string {
    return options.product_name || `Product ID ${options.product_id}`;
  }

  // ============ BrowserPaginatedCrawler Abstract Methods ============

  protected getBrowserConfig(): BrowserCrawlerConfig {
    return {
      stealth: true,
      waitUntil: 'domcontentloaded',
      navigationTimeout: 30000,
      cookieConsent: {
        bannerSelectors: ['[data-test="cookie-accept"]', '#onetrust-accept-btn-handler'],
        acceptSelectors: ['[data-test="cookie-accept"]', '#onetrust-accept-btn-handler'],
        timeout: 2000,
      },
      captcha: {
        enabled: false, // Capterra doesn't typically use CAPTCHA
      },
    };
  }

  protected getBrowserPaginationConfig(): BrowserPaginationConfig {
    return {
      maxPages: 50,
      pageSize: 20, // Capterra shows ~20 reviews per page
      rateLimitMs: 6000,
      incrementalCheckpoint: false,
      pagesPerRun: 1, // Single page per run
      pageDelayMs: 2000,
    };
  }

  protected getBaseUrl(options: CrawlerOptions): string {
    const opts = options as CapterraOptions;
    const baseUrl = opts.product_name
      ? `https://www.capterra.com/p/${opts.product_id}/${opts.product_name}`
      : `https://www.capterra.com/p/${opts.product_id}`;
    return `${baseUrl}/reviews`;
  }

  protected buildPageUrl(baseUrl: string, pageNumber: number): string {
    return pageNumber === 1 ? baseUrl : `${baseUrl}?page=${pageNumber}`;
  }

  protected async waitForContent(page: Page): Promise<void> {
    try {
      await page.waitForSelector('[data-test="review-card"], .review-card', { timeout: 10000 });
    } catch (_error) {
      logger.warn(`[${this.type}] Review selectors not found - page structure may have changed`);
    }
  }

  protected async extractItems(page: Page): Promise<CapterraReview[]> {
    return page.evaluate(() => {
      const reviewElements = Array.from(
        document.querySelectorAll('[data-test="review-card"], .review-card')
      );

      return reviewElements.map((el: Element, index: number) => {
        // Extract rating (usually shown as stars)
        const ratingElement = el.querySelector(
          '[data-test="rating"], .rating, [aria-label*="star"]'
        );
        let rating = 0;
        if (ratingElement) {
          const ariaLabel = ratingElement.getAttribute('aria-label');
          const ratingMatch = ariaLabel?.match(/(\d+(?:\.\d+)?)/);
          rating = ratingMatch ? parseFloat(ratingMatch[1]) : 0;
        }

        // Extract review title
        const titleElement = el.querySelector('[data-test="review-title"], .review-title, h3, h4');
        const title = titleElement?.textContent?.trim() || '';

        // Extract review text
        const textElement = el.querySelector(
          '[data-test="review-body"], .review-body, .review-content, .review-text'
        );
        const text = textElement?.textContent?.trim() || '';

        // Extract date
        const dateElement = el.querySelector('[data-test="review-date"], .review-date, time');
        const dateText =
          dateElement?.textContent?.trim() || dateElement?.getAttribute('datetime') || '';

        // Parse relative dates like "2 weeks ago"
        let date = new Date();
        if (dateText) {
          const weeksMatch = dateText.match(/(\d+)\s+weeks?\s+ago/i);
          const monthsMatch = dateText.match(/(\d+)\s+months?\s+ago/i);
          const daysMatch = dateText.match(/(\d+)\s+days?\s+ago/i);

          if (weeksMatch) {
            date = new Date(Date.now() - parseInt(weeksMatch[1], 10) * 7 * 24 * 60 * 60 * 1000);
          } else if (monthsMatch) {
            date = new Date(Date.now() - parseInt(monthsMatch[1], 10) * 30 * 24 * 60 * 60 * 1000);
          } else if (daysMatch) {
            date = new Date(Date.now() - parseInt(daysMatch[1], 10) * 24 * 60 * 60 * 1000);
          } else {
            // Try parsing as date
            const parsed = new Date(dateText);
            if (!Number.isNaN(parsed.getTime())) {
              date = parsed;
            }
          }
        }

        // Extract author
        const authorElement = el.querySelector(
          '[data-test="reviewer-name"], .reviewer-name, .author'
        );
        const author = authorElement?.textContent?.trim() || 'Anonymous';

        // Extract review ID from data attributes or generate
        const reviewId =
          (el as HTMLElement).getAttribute('data-review-id') ||
          (el as HTMLElement).id ||
          `${date.toISOString()}_${index}`.replace(/[^a-zA-Z0-9]/g, '_');

        // Extract helpful count
        const helpfulElement = el.querySelector('[data-test="helpful-count"], .helpful-count');
        const helpfulCount = helpfulElement
          ? parseInt(helpfulElement.textContent?.replace(/\D/g, '') || '0', 10)
          : 0;

        return {
          id: reviewId,
          rating,
          title,
          text,
          date: date.toISOString(),
          author,
          helpfulCount,
        };
      });
    });
  }

  protected transformItem(item: CapterraReview, options: CrawlerOptions): Content {
    const url = this.getBaseUrl(options);
    const engagementData = {
      rating: item.rating,
      helpful_count: item.helpfulCount,
    };

    return {
      external_id: item.id,
      title: item.title,
      content: item.text,
      author: item.author,
      url: url,
      published_at: new Date(item.date),
      score: calculateEngagementScore('capterra', engagementData),
      metadata: engagementData,
    };
  }

  protected getItemDate(item: CapterraReview): Date {
    return new Date(item.date);
  }

  protected filterItem(item: CapterraReview, _options: CrawlerOptions): boolean {
    // Include all reviews with content
    return Boolean(item.text && item.text.length > 0);
  }

  // ============ Search Method ============

  /**
   * Search Capterra for a product
   */
  async search(searchTerm: string, env: Env): Promise<SearchResult[]> {
    const { browser, screenshotDir } = await launchBrowser(env, {
      stealth: true,
    });

    let page: Page | null = null;

    try {
      page = (await browser.newPage()) as Page;

      const searchUrl = `https://www.capterra.com/search/?query=${encodeURIComponent(searchTerm)}`;
      await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

      // Wait for search results
      await page.waitForSelector('[data-test="product-card"], .product-card', { timeout: 10000 });

      // Extract product links
      const results = await page.evaluate(() => {
        const productElements = Array.from(
          document.querySelectorAll('[data-test="product-card"], .product-card')
        );

        return productElements.slice(0, 5).map((el: Element) => {
          const linkElement = el.querySelector('a[href*="/p/"]') as HTMLAnchorElement | null;
          const titleElement = el.querySelector('[data-test="product-name"], .product-name, h3');
          const descElement = el.querySelector('[data-test="product-description"], .description');
          const vendorElement = el.querySelector(
            '[data-test="vendor-name"], .vendor-name, .company-name'
          );

          const url = linkElement?.href || '';
          const title = titleElement?.textContent?.trim() || '';
          const description = descElement?.textContent?.trim() || '';
          const vendorName = vendorElement?.textContent?.trim() || '';

          // Extract product ID and name from URL (e.g., /p/12345/Product-Name/)
          const urlMatch = url.match(/\/p\/(\d+)\/([^/]+)/);
          const productId = urlMatch ? urlMatch[1] : '';
          const productName = urlMatch ? urlMatch[2] : '';

          return {
            url: `${url.replace(/\/$/, '')}/reviews`, // Ensure we link to reviews page
            title,
            description: description.substring(0, 200),
            metadata: {
              vendor_name: vendorName,
              product_id: productId,
              product_name: productName,
            },
          };
        });
      });

      await browser.close();
      return results.filter((r) => r.url && r.metadata.product_id);
    } catch (error: any) {
      // Capture error artifacts for debugging
      if (page) {
        await captureErrorArtifacts(page, error, 'capterra-search', screenshotDir);
      }
      await browser.close();
      logger.error({ error }, '[CapterraCrawler] Search error:');
      return [];
    }
  }
}
