/**
 * X (Twitter) Crawler
 * Fetches tweets using twitter-scraper library
 */

import { type Static, Type } from '@owletto/sdk';
import { Scraper } from 'npm:@the-convocation/twitter-scraper@0.21.1';
import { cycleTLSExit, cycleTLSFetch } from 'npm:@the-convocation/twitter-scraper@0.21.1/cycletls';
import { logger } from '@owletto/sdk';
import type { Checkpoint, Content, CrawlResult, Env, SearchResult } from '@owletto/sdk';
import { BaseCrawler, calculateEngagementScore } from '@owletto/sdk';

/**
 * X (Twitter)-specific options schema
 */
export const XOptionsSchema = Type.Object(
  {
    search_query: Type.String({
      description: 'Search query for tweets (e.g., "nodejs", "#programming", "from:user")',
      minLength: 1,
    }),
    min_engagement_for_threads: Type.Optional(
      Type.Number({
        description: 'Minimum engagement score to fetch conversation threads (default: 100)',
        minimum: 0,
        default: 100,
      })
    ),
    max_threads_to_fetch: Type.Optional(
      Type.Number({
        description:
          'Maximum number of conversation threads to fetch, ranked by engagement (default: 100)',
        minimum: 1,
        maximum: 500,
        default: 100,
      })
    ),
  },
  {
    description: 'X (Twitter) crawler options using twitter-scraper library',
    $id: 'XOptions',
  }
);

export type XOptions = Static<typeof XOptionsSchema>;

interface XCheckpoint extends Checkpoint {
  last_tweet_id?: string; // Track last seen tweet to avoid duplicates
  processed_thread_ids?: string[]; // Track which tweet threads we've already fetched replies for
}

export class XCrawler extends BaseCrawler {
  readonly type = 'x';
  readonly displayName = 'X (Twitter)';
  readonly apiType = 'browser' as const; // Uses CycleTLS which requires external worker
  readonly crawlerType = 'entity' as const; // Can track handles (entity) or search queries
  readonly optionsSchema = XOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.7, // X has very rich engagement (retweets, likes, replies)
    inverse_rating_weight: 0.0, // No rating system
    content_length_weight: 0.3, // Tweets are short by design
    platform_weight: 1.0,
  };

  // X/Twitter: Heavy engagement weight, content normalized to 280 chars
  readonly defaultScoringFormula = `
    PERCENT_RANK() OVER (PARTITION BY f.source_id ORDER BY f.score) * 100 * 0.7 +
    LEAST(f.content_length / 2.8, 100) * 0.3
  `;

  getRateLimit() {
    return {
      requests_per_minute: 15,
      requests_per_hour: 450, // Free tier limit per 15 min window
      recommended_interval_ms: 4000, // 4 seconds between requests
    };
  }

  validateOptions(options: XOptions): string | null {
    if (!options.search_query) {
      return 'search_query is required';
    }
    return null;
  }

  urlFromOptions(options: XOptions): string {
    return `https://x.com/search?q=${encodeURIComponent(options.search_query)}`;
  }

  displayLabelFromOptions(options: XOptions): string {
    return options.search_query;
  }

  async pull(
    options: XOptions,
    checkpoint: XCheckpoint | null,
    env: Env,
    _updateCheckpointFn?: (checkpoint: Checkpoint) => Promise<void>
  ): Promise<CrawlResult> {
    // Check for credentials - either cookies or username/password
    const cookies = env.X_COOKIES;
    const username = env.X_USERNAME;
    const password = env.X_PASSWORD;
    const email = env.X_EMAIL;
    const twoFactorSecret = env.X_2FA_SECRET; // Optional TOTP secret for 2FA

    // Log credential availability (without values) for debugging
    logger.info(
      {
        hasCookies: !!cookies,
        hasUsername: !!username,
        hasPassword: !!password,
        hasEmail: !!email,
        has2FASecret: !!twoFactorSecret,
      },
      'X crawler credential check'
    );

    if (!cookies && (!username || !password || !email)) {
      throw new Error(
        'X credentials not configured. Required: X_COOKIES (recommended) OR X_USERNAME + X_PASSWORD + X_EMAIL'
      );
    }

    try {
      // Use CycleTLS to bypass Cloudflare bot detection
      const scraper = new Scraper({
        fetch: cycleTLSFetch,
        experimental: {
          xClientTransactionId: true,
          xpff: true,
        },
      });

      // Try cookie-based auth first (more reliable, avoids DenyLoginSubtask)
      if (cookies) {
        logger.info('Using cookie-based authentication');
        try {
          const cookieArray = JSON.parse(cookies);
          await scraper.setCookies(cookieArray);

          // Verify cookies are valid by checking login status
          const isLoggedIn = await scraper.isLoggedIn();
          if (!isLoggedIn) {
            throw new Error('Cookies are invalid or expired');
          }
          logger.info('Cookie authentication successful');
        } catch (cookieError) {
          logger.warn(`Cookie auth failed: ${cookieError}, falling back to login`);
          if (!username || !password || !email) {
            throw new Error(
              'Cookie authentication failed and no login credentials provided. Please refresh X_COOKIES.'
            );
          }
          // Fall through to login
          await scraper.login(username, password, email, twoFactorSecret);
        }
      } else {
        // Login with username/password (may trigger DenyLoginSubtask)
        // TypeScript: we know these exist because of the initial validation above
        logger.info('Using username/password authentication');
        await scraper.login(username!, password!, email!, twoFactorSecret);

        // Export cookies for future use
        try {
          const newCookies = await scraper.getCookies();
          logger.info(
            'Login successful. To avoid future login issues, save these cookies to X_COOKIES:'
          );
          logger.info(JSON.stringify(newCookies));
        } catch (_e) {
          logger.warn('Could not export cookies for future use');
        }
      }

      const maxResults = 5000;
      const tweets: any[] = [];
      const seenIds = new Set<string>();

      // Track if we've seen the checkpoint tweet (to stop early)
      let foundCheckpoint = false;

      // Search for tweets using AsyncGenerator
      for await (const tweet of scraper.searchTweets(options.search_query, maxResults * 2)) {
        // Stop if we've reached our max results
        if (tweets.length >= maxResults) {
          break;
        }

        // Skip if we've already seen this tweet
        if (seenIds.has(tweet.id || '')) {
          continue;
        }

        // Stop if we've reached the checkpoint (already processed this tweet before)
        if (checkpoint?.last_tweet_id && tweet.id === checkpoint.last_tweet_id) {
          foundCheckpoint = true;
          break;
        }

        seenIds.add(tweet.id || '');
        tweets.push(tweet);
      }

      // Transform to Content format
      const contents: Content[] = tweets
        .filter((tweet) => tweet.id && tweet.text) // Filter out invalid tweets
        .map((tweet) => {
          const author = tweet.username || tweet.name || 'Unknown';
          const likes = tweet.likes || 0;
          const retweets = tweet.retweets || 0;
          const replies = tweet.replies || 0;

          const engagementData = {
            reply_count: replies,
            upvotes: likes,
            score: retweets * 2 + likes,
          };

          return {
            external_id: tweet.id!,
            content: tweet.text!,
            author: author.startsWith('@') ? author : `@${author}`,
            published_at: tweet.timeParsed || new Date(),
            score: calculateEngagementScore('x', engagementData),
            url: `https://twitter.com/${tweet.username || 'i'}/status/${tweet.id}`,
            metadata: {
              ...engagementData,
              retweet_count: retweets,
              quote_count: tweet.quotes || 0,
              is_retweet: tweet.isRetweet || false,
              is_reply: tweet.isReply || false,
              is_quote: tweet.isQuoted || false,
            },
          };
        });

      // Smart conversation thread collection for high-value tweets
      const minEngagement = options.min_engagement_for_threads || 100;
      const maxThreads = options.max_threads_to_fetch || 100;
      const isDeepMode = maxResults <= 20; // Deep mode for smaller collections
      const officialAccounts = ['Google', 'GoogleDeepMind', 'GeminiApp', 'GoogleAI', 'GoogleDocs'];

      // Track processed threads for incremental checkpointing (declared outside if block for checkpoint access)
      const processedThreadIds = new Set(checkpoint?.processed_thread_ids || []);

      if (isDeepMode || minEngagement > 0) {
        logger.info('Checking for high-engagement tweets to fetch conversation threads');

        // Filter tweets that qualify for thread fetching
        const candidateTweets = contents.filter((content) => {
          const isOfficial = officialAccounts.some((acc) =>
            content.author?.toLowerCase().includes(acc.toLowerCase())
          );
          const hasHighEngagement = content.score >= minEngagement;
          const hasManyReplies = (content.metadata as any).reply_count > 30;

          return isOfficial || hasHighEngagement || hasManyReplies;
        });

        // Rank by engagement score (highest first) and cap at max
        const tweetsForThreads = candidateTweets
          .sort((a, b) => b.score - a.score)
          .slice(0, maxThreads);

        logger.info(
          `Found ${candidateTweets.length} tweets qualifying for thread collection, fetching top ${tweetsForThreads.length}`
        );

        let threadsFetched = 0;

        // Fetch conversation threads for qualifying tweets
        for (const rootTweet of tweetsForThreads) {
          // Skip if already processed in a previous run
          if (processedThreadIds.has(rootTweet.external_id)) {
            logger.info(`Skipping already processed thread for tweet ${rootTweet.external_id}`);
            continue;
          }
          try {
            // Use conversation_id search to get replies
            const conversationQuery = `conversation_id:${rootTweet.external_id}`;
            const replyTweets: any[] = [];
            let replyCount = 0;
            const maxReplies = 50; // Max replies per conversation

            logger.info(`Fetching conversation thread for tweet ${rootTweet.external_id}`);

            // Fetch replies using conversation search
            for await (const reply of scraper.searchTweets(conversationQuery, maxReplies)) {
              if (replyCount >= maxReplies) break;

              // Skip the root tweet itself
              if (reply.id === rootTweet.external_id) continue;

              // Only include actual replies (not the root)
              if (reply.inReplyToStatusId || reply.isReply) {
                replyTweets.push(reply);
                replyCount++;
              }
            }

            // Transform replies to Content format with parent_external_id
            const replyContents: Content[] = replyTweets
              .filter((reply) => reply.id && reply.text)
              .map((reply) => {
                const replyAuthor = reply.username || reply.name || 'Unknown';
                const replyLikes = reply.likes || 0;
                const replyRetweets = reply.retweets || 0;
                const replyReplies = reply.replies || 0;

                const replyEngagementData = {
                  reply_count: replyReplies,
                  upvotes: replyLikes,
                  score: replyRetweets * 2 + replyLikes,
                };

                return {
                  external_id: reply.id!,
                  content: reply.text!,
                  author: replyAuthor.startsWith('@') ? replyAuthor : `@${replyAuthor}`,
                  published_at: reply.timeParsed || new Date(),
                  score: calculateEngagementScore('x', replyEngagementData),
                  url: `https://twitter.com/${reply.username || 'i'}/status/${reply.id}`,
                  parent_external_id: reply.inReplyToStatusId || rootTweet.external_id, // Link to parent
                  metadata: {
                    ...replyEngagementData,
                    retweet_count: replyRetweets,
                    quote_count: reply.quotes || 0,
                    is_retweet: reply.isRetweet || false,
                    is_reply: true,
                    is_quote: reply.isQuoted || false,
                    conversation_id: rootTweet.external_id,
                  },
                };
              });

            // Add reply contents to main contents array
            contents.push(...replyContents);
            logger.info(`Added ${replyContents.length} replies for tweet ${rootTweet.external_id}`);

            // Mark this thread as processed
            processedThreadIds.add(rootTweet.external_id);
            threadsFetched++;

            // Save checkpoint every 20 threads to enable resumability
            if (threadsFetched % 20 === 0 && _updateCheckpointFn) {
              const intermediateCheckpoint: XCheckpoint = {
                last_tweet_id:
                  contents.length > 0 ? contents[0].external_id : checkpoint?.last_tweet_id,
                last_timestamp:
                  contents.length > 0
                    ? contents[0].published_at
                    : checkpoint?.last_timestamp || new Date(),
                processed_thread_ids: Array.from(processedThreadIds),
                updated_at: new Date(),
              };
              await _updateCheckpointFn(intermediateCheckpoint);
              logger.info(`Checkpoint saved: ${threadsFetched} threads fetched so far`);
            }
          } catch (error) {
            logger.warn(
              `Failed to fetch conversation for tweet ${rootTweet.external_id}: ${error}`
            );
            // Continue with other tweets even if one fails
          }
        }
      }

      // Sort by published date descending
      contents.sort((a, b) => b.published_at.getTime() - a.published_at.getTime());

      // Get the newest tweet ID for next checkpoint
      const newestTweetId =
        contents.length > 0 ? contents[0].external_id : checkpoint?.last_tweet_id;

      const newCheckpoint: XCheckpoint =
        contents.length > 0
          ? {
              last_tweet_id: newestTweetId,
              last_timestamp: contents[0].published_at,
              processed_thread_ids: Array.from(processedThreadIds || []),
              updated_at: new Date(),
            }
          : {
              last_tweet_id: checkpoint?.last_tweet_id,
              last_timestamp: checkpoint?.last_timestamp || new Date(),
              processed_thread_ids: checkpoint?.processed_thread_ids || [],
              updated_at: new Date(),
            };

      // Clean up CycleTLS resources
      cycleTLSExit();

      return {
        contents,
        checkpoint: newCheckpoint,
        metadata: {
          items_found: contents.length,
          items_skipped: seenIds.size - contents.length,
          reached_checkpoint: foundCheckpoint,
        },
      };
    } catch (error) {
      // Ensure CycleTLS cleanup even on error
      try {
        cycleTLSExit();
      } catch (_cleanupError) {
        logger.warn('Failed to clean up CycleTLS resources');
      }

      throw new Error(
        `X crawler failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Search X/Twitter for relevant content about a topic
   * Returns search query options for monitoring tweets about the topic
   */
  async search(searchTerm: string, env: Env): Promise<SearchResult[]> {
    // Check if credentials are configured
    if (!env.X_USERNAME || !env.X_PASSWORD || !env.X_EMAIL) {
      logger.warn(
        'X credentials not configured (X_USERNAME, X_PASSWORD, X_EMAIL), skipping X search'
      );
      return [];
    }

    try {
      const searchTermLower = searchTerm.toLowerCase().replace(/\s+/g, '');

      // Return multiple search query options that users can choose from
      return [
        {
          url: `https://twitter.com/search?q=${encodeURIComponent(searchTerm)}`,
          title: `Search tweets about "${searchTerm}"`,
          description: `Monitor all tweets that mention "${searchTerm}"`,
          metadata: {
            search_query: searchTerm,
          },
        },
        {
          url: `https://twitter.com/search?q=${encodeURIComponent(`#${searchTermLower}`)}`,
          title: `Hashtag #${searchTermLower}`,
          description: `Monitor tweets with hashtag #${searchTermLower}`,
          metadata: {
            search_query: `#${searchTermLower}`,
          },
        },
        {
          url: `https://twitter.com/search?q=${encodeURIComponent(`"${searchTerm}" -filter:retweets`)}`,
          title: `Original tweets about "${searchTerm}"`,
          description: `Monitor original tweets (no retweets) mentioning "${searchTerm}"`,
          metadata: {
            search_query: `"${searchTerm}" -filter:retweets`,
          },
        },
      ];
    } catch (error) {
      logger.error({ error }, 'X search error:');
      return [];
    }
  }
}
