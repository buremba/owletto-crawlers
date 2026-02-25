/**
 * G2 Crawler
 * Scrapes B2B software reviews from G2.com using browser rendering with stealth mode.
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
} from './browser-paginated';
import type { PaginatedCheckpoint } from '@owletto/sdk';

/**
 * G2-specific options schema
 */
export const G2OptionsSchema = Type.Object(
  {
    product_url: Type.String({
      description:
        'Full G2 product review URL (e.g., "https://www.g2.com/products/confluence/reviews")',
      format: 'uri',
      pattern: '^https://www\\.g2\\.com/products/[^/]+/reviews',
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
    description: 'G2 crawler options - requires trusted browser session for local execution',
    $id: 'G2Options',
  }
);

export type G2Options = Static<typeof G2OptionsSchema>;

interface G2Checkpoint extends PaginatedCheckpoint {
  last_review_id?: string;
  last_page?: number;
}

/**
 * Raw review data extracted from G2 page
 */
interface G2Review {
  rating: number;
  title: string;
  text: string;
  author: string;
  jobTitle: string;
  industry: string;
  companySize: string;
  date: string;
  badges: string[];
  reviewUrl: string;
  helpfulCount: number;
}

export class G2Crawler extends BrowserPaginatedCrawler<G2Review, G2Checkpoint> {
  readonly type = 'g2';
  readonly displayName = 'G2';
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = G2OptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.0, // No engagement metrics on G2
    inverse_rating_weight: 0.5, // Prioritize critical reviews (1-star > 5-star)
    content_length_weight: 0.5, // Detailed reviews are valuable
    platform_weight: 1.0,
  };

  // G2: Inverse rating (prioritize negative content) + content depth
  readonly defaultScoringFormula = `
    (5.0 - COALESCE((f.metadata->>'rating')::numeric, 3)) / 4.0 * 100 * 0.5 +
    LEAST(f.content_length / 20.0, 100) * 0.5
  `;

  getRateLimit() {
    return {
      requests_per_minute: 10, // Be conservative with G2
      recommended_interval_ms: 6000, // 6 seconds between requests
    };
  }

  validateOptions(options: G2Options): string | null {
    if (!options.product_url) {
      return 'product_url is required';
    }

    if (!options.product_url.match(/^https:\/\/www\.g2\.com\/products\/[^/]+\/reviews/)) {
      return 'product_url must be a valid G2 product review URL (e.g., https://www.g2.com/products/confluence/reviews)';
    }

    return null;
  }

  urlFromOptions(options: G2Options): string {
    return options.product_url || 'https://www.g2.com';
  }

  displayLabelFromOptions(options: G2Options): string {
    // Guard against undefined product_url (legacy crawlers)
    if (!options.product_url) {
      return 'G2 Reviews';
    }

    // Extract product name from URL
    const match = options.product_url.match(/\/products\/([^/]+)/);
    const productName = match ? match[1].replace(/-/g, ' ') : 'Unknown';
    return `${productName} Reviews`;
  }

  // ============ BrowserPaginatedCrawler Abstract Methods ============

  protected getBrowserConfig(): BrowserCrawlerConfig {
    return {
      stealth: true,
      waitUntil: 'domcontentloaded',
      navigationTimeout: 30000,
      cookieConsent: {
        bannerSelectors: ['#onetrust-accept-btn-handler'],
        acceptSelectors: ['#onetrust-accept-btn-handler'],
        timeout: 2000,
      },
      captcha: {
        enabled: false, // G2 doesn't use CAPTCHA
      },
    };
  }

  protected getBrowserPaginationConfig(): BrowserPaginationConfig {
    return {
      maxPages: 50,
      pageSize: 10, // G2 shows ~10 reviews per page
      rateLimitMs: 6000,
      incrementalCheckpoint: true, // Update checkpoint after each page
      pagesPerRun: 5, // Fetch 5 pages (50 reviews) per run
      pageDelayMs: 2000,
    };
  }

  protected getBaseUrl(options: CrawlerOptions): string {
    const opts = options as G2Options;
    return opts.product_url;
  }

  protected buildPageUrl(baseUrl: string, pageNumber: number): string {
    return pageNumber === 1 ? baseUrl : `${baseUrl}?page=${pageNumber}`;
  }

  protected async waitForContent(page: Page): Promise<void> {
    try {
      await page.waitForSelector('[itemprop="review"]', { timeout: 10000 });
    } catch (_error) {
      // No reviews found - might be empty page or blocked
      logger.warn(`[${this.type}] No review cards found on page`);
    }
  }

  protected async extractItems(page: Page): Promise<G2Review[]> {
    return page.evaluate(() => {
      const results: G2Review[] = [];
      const reviewCards = document.querySelectorAll('[itemprop="review"]');

      reviewCards.forEach((card) => {
        try {
          // Extract author name from meta tag
          const authorMeta = card.querySelector('[itemprop="author"] meta[itemprop="name"]');
          const author = authorMeta?.getAttribute('content') || 'Anonymous';

          // Extract author details from sibling divs with elv-text-subtle class
          const authorContainer = card.querySelector('[itemprop="author"]');
          const parentDiv = authorContainer?.closest('.elv-gap-2')?.parentElement;
          const detailDivs = parentDiv
            ? Array.from(parentDiv.querySelectorAll('.elv-text-subtle'))
            : [];

          // Parse author details (job title, industry, company size)
          let jobTitle = '';
          let industry = '';
          let companySize = '';

          if (detailDivs.length >= 3) {
            jobTitle = detailDivs[0]?.textContent?.trim() || '';
            industry = detailDivs[1]?.textContent?.trim() || '';
            companySize = detailDivs[2]?.textContent?.trim() || '';
          } else if (detailDivs.length === 2) {
            jobTitle = detailDivs[0]?.textContent?.trim() || '';
            companySize = detailDivs[1]?.textContent?.trim() || '';
          } else if (detailDivs.length === 1) {
            companySize = detailDivs[0]?.textContent?.trim() || '';
          }

          // Extract date from meta tag
          const dateMeta = card.querySelector('meta[itemprop="datePublished"]');
          const dateStr = dateMeta?.getAttribute('content') || '';

          // Extract rating
          const ratingMeta = card.querySelector('[itemprop="ratingValue"]');
          const rating = ratingMeta ? parseFloat(ratingMeta.getAttribute('content') || '0') : 0;

          // Extract review title
          const titleDiv = card.querySelector('[itemprop="name"] .elv-font-bold');
          const title = titleDiv?.textContent?.trim().replace(/^"|"$/g, '') || '';

          // Extract review body - use innerText to preserve visual spacing/newlines
          const reviewBodyEl = card.querySelector('[itemprop="reviewBody"]');
          const reviewBody = (reviewBodyEl as HTMLElement)?.innerText?.trim() || '';

          // Extract badges
          const badgeEls = card.querySelectorAll(
            '[class*="badge"], [class*="tag"], .elv-rounded-sm.elv-border'
          );
          const badges = Array.from(badgeEls)
            .map((el) => el.textContent?.trim())
            .filter((text): text is string => !!text && text.length < 50 && text.length > 3);

          // Extract review URL
          const linkEl = card.querySelector('a[href*="survey_responses"]');
          const href = linkEl?.getAttribute('href') || '';
          const reviewUrl = href
            ? href.startsWith('http')
              ? href
              : `https://www.g2.com${href}`
            : '';

          // Skip reviews with minimal content
          if ((reviewBody || '').length < 50) return;

          results.push({
            rating,
            title,
            text: reviewBody,
            author,
            jobTitle,
            industry,
            companySize,
            date: dateStr,
            badges: badges.slice(0, 10),
            reviewUrl,
            helpfulCount: 0,
          });
        } catch (e) {
          console.error('[G2Crawler] Error parsing review card:', e);
        }
      });

      return results;
    });
  }

  protected transformItem(item: G2Review, options: CrawlerOptions): Content {
    const baseUrl = this.getBaseUrl(options);
    const opts = options as G2Options;

    // Extract product key for external_id
    const match = opts.product_url.match(/\/products\/([^/]+)/);
    const productKey = match ? match[1] : 'unknown';

    const engagementData = {
      rating: item.rating,
      helpful_count: item.helpfulCount,
    };

    return {
      external_id: `g2-${productKey}-${item.date || 'nodate'}-${item.author.replace(/\s+/g, '-')}`,
      title: item.title,
      content: item.text,
      author: item.author,
      published_at: new Date(item.date),
      score: calculateEngagementScore('g2', engagementData),
      url: item.reviewUrl || baseUrl,
      metadata: {
        rating: item.rating,
        helpful_count: item.helpfulCount,
        job_title: item.jobTitle,
        industry: item.industry,
        company_size: item.companySize,
        badges: item.badges,
      },
    };
  }

  protected getItemDate(item: G2Review): Date {
    return item.date ? new Date(item.date) : new Date();
  }

  protected filterItem(item: G2Review, _options: CrawlerOptions): boolean {
    // Only include reviews with sufficient content (extracted in extractItems)
    return Boolean(item.text && item.text.length >= 50);
  }

  // ============ Search Method ============

  /**
   * Search G2 for a product
   * Note: Discovery is better handled via manage_sources discover action
   */
  async search(_searchTerm: string, _env: Env): Promise<SearchResult[]> {
    logger.warn('[G2Crawler] Search not implemented - use manage_sources discover action instead');
    return [];
  }
}
