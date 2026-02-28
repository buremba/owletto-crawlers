/**
 * Glassdoor Crawler
 * Scrapes employee reviews using Playwright
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
 * Glassdoor-specific options schema
 */
export const GlassdoorOptionsSchema = Type.Object(
  {
    company_id: Type.Optional(
      Type.String({
        description: 'Glassdoor company ID if known',
        minLength: 1,
      })
    ),
    company_name: Type.String({
      description: 'Company name for search-based lookup',
      minLength: 1,
    }),
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
    description: 'Glassdoor crawler options',
    $id: 'GlassdoorOptions',
  }
);

export type GlassdoorOptions = Static<typeof GlassdoorOptionsSchema>;

interface GlassdoorCheckpoint extends PaginatedCheckpoint {
  last_review_id?: string;
  last_page?: number;
}

/**
 * Raw review data extracted from Glassdoor page
 */
interface GlassdoorReview {
  id: string;
  rating: number;
  title: string;
  pros: string;
  cons: string;
  date: string;
  author: string;
}

export class GlassdoorCrawler extends BrowserPaginatedCrawler<
  GlassdoorReview,
  GlassdoorCheckpoint
> {
  readonly type = 'glassdoor';
  readonly displayName = 'Glassdoor';
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = GlassdoorOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.0, // No engagement metrics on Glassdoor
    inverse_rating_weight: 0.5, // Prioritize critical reviews (1-star > 5-star)
    content_length_weight: 0.5, // Detailed reviews are valuable
    platform_weight: 1.0,
  };

  // Glassdoor: Inverse rating + content depth (no engagement metrics available)
  readonly defaultScoringFormula = `
    (5.0 - COALESCE((f.metadata->>'rating')::numeric, 3)) / 4.0 * 100 * 0.5 +
    LEAST(f.content_length / 20.0, 100) * 0.5
  `;

  getRateLimit() {
    return {
      requests_per_minute: 8,
      recommended_interval_ms: 8000, // 8 seconds between requests (very conservative)
    };
  }

  validateOptions(options: GlassdoorOptions): string | null {
    if (!options.company_name) {
      return 'company_name is required';
    }
    return null;
  }

  urlFromOptions(options: GlassdoorOptions): string {
    if (options.company_id) {
      return `https://www.glassdoor.com/Reviews/company-reviews-${options.company_id}.htm`;
    }
    if (options.company_name) {
      return `https://www.glassdoor.com/Reviews/${options.company_name}-reviews-SRCH_KE0.htm`;
    }
    return '';
  }

  displayLabelFromOptions(options: GlassdoorOptions): string {
    return options.company_name || `Company ID ${options.company_id}`;
  }

  // ============ BrowserPaginatedCrawler Abstract Methods ============

  protected getBrowserConfig(): BrowserCrawlerConfig {
    return {
      stealth: true, // Enable stealth to avoid detection
      viewport: { width: 1920, height: 1080 },
      userAgent:
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      waitUntil: 'domcontentloaded',
      navigationTimeout: 30000,
      cookieConsent: {
        bannerSelectors: ['#onetrust-accept-btn-handler'],
        acceptSelectors: ['#onetrust-accept-btn-handler'],
        timeout: 2000,
      },
      captcha: {
        enabled: true,
        selectors: ['#recaptcha-anchor', '.g-recaptcha', '[data-test="sign-in-modal"]'],
        textPatterns: ['verify you are a human'],
      },
    };
  }

  protected getBrowserPaginationConfig(): BrowserPaginationConfig {
    return {
      maxPages: 50,
      pageSize: 20, // Glassdoor shows ~20 reviews per page
      rateLimitMs: 8000,
      incrementalCheckpoint: false,
      pagesPerRun: 1, // Single page per run
      pageDelayMs: 2000,
    };
  }

  protected getBaseUrl(options: CrawlerOptions): string {
    const opts = options as GlassdoorOptions;
    if (opts.company_id) {
      return `https://www.glassdoor.com/Reviews/company-reviews-${opts.company_id}.htm`;
    }
    return `https://www.glassdoor.com/Reviews/${opts.company_name}-reviews-SRCH_KE0.htm`;
  }

  protected buildPageUrl(baseUrl: string, pageNumber: number): string {
    return pageNumber === 1 ? baseUrl : `${baseUrl}?page=${pageNumber}`;
  }

  protected async waitForContent(page: Page): Promise<void> {
    // Add human-like delay after navigation
    await this.sleep(2000);

    try {
      await page.waitForSelector(
        '[data-test="review-list-item"], .empReview, [data-test="employerReview"]',
        { timeout: 10000 }
      );
    } catch (_error) {
      logger.warn(`[${this.type}] No review cards found - page may require authentication`);
    }
  }

  protected async extractItems(page: Page): Promise<GlassdoorReview[]> {
    return page.evaluate(() => {
      // Try multiple selector strategies as Glassdoor frequently changes their HTML
      const reviewElements =
        Array.from(document.querySelectorAll('[data-test="review-list-item"]')) ||
        Array.from(document.querySelectorAll('.empReview')) ||
        Array.from(document.querySelectorAll('[data-test="employerReview"]'));

      return reviewElements.map((el: Element) => {
        // Try multiple selector patterns for each field
        const ratingEl =
          el.querySelector('[data-test="overall-rating"]') ||
          el.querySelector('.rating') ||
          el.querySelector('[class*="rating"]');

        const titleEl =
          el.querySelector('[data-test="review-title"]') ||
          el.querySelector('.reviewLink') ||
          el.querySelector('[class*="title"]');

        const prosEl =
          el.querySelector('[data-test="pros"]') ||
          el.querySelector('[data-pros]') ||
          el.querySelector('.pros');

        const consEl =
          el.querySelector('[data-test="cons"]') ||
          el.querySelector('[data-cons]') ||
          el.querySelector('.cons');

        const dateEl =
          el.querySelector('[data-test="review-date"]') ||
          el.querySelector('.date') ||
          el.querySelector('time');

        const authorEl =
          el.querySelector('[data-test="employee-info"]') ||
          el.querySelector('.authorInfo') ||
          el.querySelector('[class*="author"]');

        // Try to get review ID from various attributes
        const reviewId =
          (el as HTMLElement).getAttribute('data-review-id') ||
          (el as HTMLElement).getAttribute('id') ||
          (el as HTMLElement).getAttribute('data-id') ||
          '';

        return {
          id: reviewId,
          rating: parseFloat(ratingEl?.textContent?.trim() || '0'),
          title: titleEl?.textContent?.trim() || '',
          pros: prosEl?.textContent?.trim() || '',
          cons: consEl?.textContent?.trim() || '',
          date: dateEl?.getAttribute('datetime') || dateEl?.textContent?.trim() || '',
          author: authorEl?.textContent?.trim() || '',
        };
      });
    });
  }

  protected transformItem(item: GlassdoorReview, options: CrawlerOptions): Content {
    const baseUrl = this.getBaseUrl(options);
    const engagementData = {
      rating: item.rating,
    };

    // Combine title, pros, and cons into content
    const content = `${item.title}\n\nPros: ${item.pros}\n\nCons: ${item.cons}`;

    return {
      external_id: item.id || `glassdoor_${Date.now()}_${Math.random()}`,
      content: content,
      author: item.author,
      published_at: new Date(item.date),
      score: calculateEngagementScore('glassdoor', engagementData),
      url: `${baseUrl}#review_${item.id}`,
      metadata: {
        rating: item.rating,
        title: item.title,
        pros: item.pros,
        cons: item.cons,
      },
    };
  }

  protected getItemDate(item: GlassdoorReview): Date {
    return new Date(item.date);
  }

  protected filterItem(item: GlassdoorReview, _options: CrawlerOptions): boolean {
    // Include reviews that have either pros or cons
    return Boolean(item.pros || item.cons);
  }

  // ============ Search Method ============

  /**
   * Search Glassdoor for company profiles
   * Uses browser rendering to search for companies
   */
  async search(searchTerm: string, env: Env): Promise<SearchResult[]> {
    const { browser, screenshotDir } = await launchBrowser(env);
    const page = (await browser.newPage()) as Page;

    try {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.setExtraHTTPHeaders({
        'User-Agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      });

      const searchUrl = `https://www.glassdoor.com/Search/results.htm?keyword=${encodeURIComponent(
        searchTerm
      )}`;
      await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

      const results = await page.evaluate(() => {
        const companyCards = Array.from(
          document.querySelectorAll('[data-test="employer-card"]')
        ).slice(0, 5);

        return companyCards
          .map((card: Element) => {
            const linkElement = card.querySelector('a[href*="/Overview/"]');
            const nameElement = card.querySelector('[data-test="employer-name"]');
            const descElement = card.querySelector('[data-test="employer-description"]');
            const companyName = nameElement?.textContent?.trim() || '';

            return {
              url: linkElement
                ? `https://www.glassdoor.com${linkElement.getAttribute('href')}`
                : '',
              title: companyName,
              description: descElement?.textContent?.trim() || 'Company on Glassdoor',
              metadata: {
                company_name: companyName,
              },
            };
          })
          .filter((r) => r.url && r.title);
      });

      await browser.close();
      return results;
    } catch (error: any) {
      // Capture error artifacts for debugging
      await captureErrorArtifacts(page, error, 'glassdoor-search', screenshotDir);
      logger.error({ error }, 'Glassdoor search error:');
      await browser.close();
      return [];
    }
  }
}
