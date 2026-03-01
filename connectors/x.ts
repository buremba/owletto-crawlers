/**
 * X (Twitter) Connector (V1 runtime)
 *
 * Fetches tweets using twitter-scraper with CycleTLS.
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
import { Scraper } from 'npm:@the-convocation/twitter-scraper@0.21.1';
import { cycleTLSExit, cycleTLSFetch } from 'npm:@the-convocation/twitter-scraper@0.21.1/cycletls';

interface XCheckpoint {
  last_tweet_id?: string;
  last_timestamp?: Date | string;
  processed_thread_ids?: string[];
}

export default class XConnector extends ConnectorRuntime {
  readonly definition: ConnectorDefinition = {
    key: 'x',
    name: 'X (Twitter)',
    description: 'Fetches tweets using twitter-scraper with CycleTLS.',
    version: '1.0.0',
    authSchema: {
      methods: [
        {
          type: 'env_keys',
          required: true,
          fields: [
            { key: 'X_COOKIES', label: 'X Cookies JSON (recommended)', secret: true },
            { key: 'X_USERNAME', label: 'X Username' },
            { key: 'X_PASSWORD', label: 'X Password', secret: true },
            { key: 'X_EMAIL', label: 'X Email' },
            { key: 'X_2FA_SECRET', label: 'TOTP 2FA Secret (optional)', secret: true },
          ],
        },
      ],
    },
    feeds: {
      tweets: {
        key: 'tweets',
        name: 'Tweets',
        description: 'Search and sync tweets matching a query.',
        configSchema: {
          type: 'object',
          required: ['search_query'],
          properties: {
            search_query: {
              type: 'string',
              minLength: 1,
              description:
                'Search query for tweets (e.g., "nodejs", "#programming", "from:user")',
            },
            min_engagement_for_threads: {
              type: 'integer',
              minimum: 0,
              default: 100,
              description:
                'Minimum engagement score to fetch conversation threads (default: 100)',
            },
            max_threads_to_fetch: {
              type: 'integer',
              minimum: 1,
              maximum: 500,
              default: 100,
              description:
                'Maximum number of conversation threads to fetch, ranked by engagement (default: 100)',
            },
          },
        },
      },
    },
    optionsSchema: {
      type: 'object',
      required: ['search_query'],
      properties: {
        search_query: {
          type: 'string',
          minLength: 1,
          description:
            'Search query for tweets (e.g., "nodejs", "#programming", "from:user")',
        },
        min_engagement_for_threads: {
          type: 'integer',
          minimum: 0,
          default: 100,
          description:
            'Minimum engagement score to fetch conversation threads (default: 100)',
        },
        max_threads_to_fetch: {
          type: 'integer',
          minimum: 1,
          maximum: 500,
          default: 100,
          description:
            'Maximum number of conversation threads to fetch, ranked by engagement (default: 100)',
        },
      },
    },
  };

  async sync(ctx: SyncContext): Promise<SyncResult> {
    const config = ctx.config as Record<string, unknown>;
    const checkpoint = (ctx.checkpoint ?? {}) as XCheckpoint;

    // ── Credentials from env_keys (passed through ctx.config) ──
    const cookies = config.X_COOKIES as string | undefined;
    const username = config.X_USERNAME as string | undefined;
    const password = config.X_PASSWORD as string | undefined;
    const email = config.X_EMAIL as string | undefined;
    const twoFactorSecret = config.X_2FA_SECRET as string | undefined;

    if (!cookies && (!username || !password || !email)) {
      throw new Error(
        'X credentials not configured. Required: X_COOKIES (recommended) OR X_USERNAME + X_PASSWORD + X_EMAIL'
      );
    }

    // ── Feed config ──
    const searchQuery = config.search_query as string;
    if (!searchQuery) {
      throw new Error('search_query is required');
    }
    const minEngagement = (config.min_engagement_for_threads as number) ?? 100;
    const maxThreads = (config.max_threads_to_fetch as number) ?? 100;

    try {
      // ── Scraper init with CycleTLS ──
      const scraper = new Scraper({
        fetch: cycleTLSFetch,
        experimental: {
          xClientTransactionId: true,
          xpff: true,
        },
      });

      // ── Authentication ──
      if (cookies) {
        try {
          const cookieArray = JSON.parse(cookies);
          await scraper.setCookies(cookieArray);

          const isLoggedIn = await scraper.isLoggedIn();
          if (!isLoggedIn) {
            throw new Error('Cookies are invalid or expired');
          }
        } catch (cookieError) {
          if (!username || !password || !email) {
            throw new Error(
              'Cookie authentication failed and no login credentials provided. Please refresh X_COOKIES.'
            );
          }
          await scraper.login(username, password, email, twoFactorSecret);
        }
      } else {
        await scraper.login(username!, password!, email!, twoFactorSecret);
      }

      // ── Search tweets ──
      const maxResults = 5000;
      const tweets: any[] = [];
      const seenIds = new Set<string>();
      let foundCheckpoint = false;

      for await (const tweet of scraper.searchTweets(searchQuery, maxResults * 2)) {
        if (tweets.length >= maxResults) break;

        if (seenIds.has(tweet.id || '')) continue;

        if (checkpoint.last_tweet_id && tweet.id === checkpoint.last_tweet_id) {
          foundCheckpoint = true;
          break;
        }

        seenIds.add(tweet.id || '');
        tweets.push(tweet);
      }

      // ── Transform to EventEnvelope ──
      const events: EventEnvelope[] = tweets
        .filter((tweet) => tweet.id && tweet.text)
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
            kind: 'tweet',
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
          } satisfies EventEnvelope;
        });

      // ── Thread collection for high-engagement tweets ──
      const processedThreadIds = new Set<string>(checkpoint.processed_thread_ids || []);

      const candidateTweets = events.filter((ev) => {
        const meta = ev.metadata as Record<string, unknown>;
        const hasHighEngagement = (ev.score ?? 0) >= minEngagement;
        const hasManyReplies = ((meta.reply_count as number) || 0) > 30;
        return hasHighEngagement || hasManyReplies;
      });

      const tweetsForThreads = candidateTweets
        .sort((a, b) => (b.score ?? 0) - (a.score ?? 0))
        .slice(0, maxThreads);

      for (const rootTweet of tweetsForThreads) {
        if (processedThreadIds.has(rootTweet.external_id)) continue;

        try {
          const conversationQuery = `conversation_id:${rootTweet.external_id}`;
          const replyTweets: any[] = [];
          let replyCount = 0;
          const maxReplies = 50;

          for await (const reply of scraper.searchTweets(conversationQuery, maxReplies)) {
            if (replyCount >= maxReplies) break;
            if (reply.id === rootTweet.external_id) continue;

            if (reply.inReplyToStatusId || reply.isReply) {
              replyTweets.push(reply);
              replyCount++;
            }
          }

          const replyEvents: EventEnvelope[] = replyTweets
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
                kind: 'reply',
                score: calculateEngagementScore('x', replyEngagementData),
                url: `https://twitter.com/${reply.username || 'i'}/status/${reply.id}`,
                parent_external_id: reply.inReplyToStatusId || rootTweet.external_id,
                metadata: {
                  ...replyEngagementData,
                  retweet_count: replyRetweets,
                  quote_count: reply.quotes || 0,
                  is_retweet: reply.isRetweet || false,
                  is_reply: true,
                  is_quote: reply.isQuoted || false,
                  conversation_id: rootTweet.external_id,
                },
              } satisfies EventEnvelope;
            });

          events.push(...replyEvents);
          processedThreadIds.add(rootTweet.external_id);
        } catch (_threadError) {
          // Continue with other tweets even if one thread fails
        }
      }

      // ── Sort by published date descending ──
      events.sort(
        (a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime()
      );

      // ── Build checkpoint ──
      const newestTweetId =
        events.length > 0 ? events[0].external_id : checkpoint.last_tweet_id;

      const newCheckpoint: XCheckpoint =
        events.length > 0
          ? {
              last_tweet_id: newestTweetId,
              last_timestamp: events[0].published_at,
              processed_thread_ids: Array.from(processedThreadIds),
            }
          : {
              last_tweet_id: checkpoint.last_tweet_id,
              last_timestamp: checkpoint.last_timestamp || new Date(),
              processed_thread_ids: checkpoint.processed_thread_ids || [],
            };

      // ── Cleanup CycleTLS ──
      cycleTLSExit();

      return {
        events,
        checkpoint: newCheckpoint as unknown as Record<string, unknown>,
        metadata: {
          items_found: events.length,
          items_skipped: seenIds.size - tweets.filter((t) => t.id && t.text).length,
          reached_checkpoint: foundCheckpoint,
        },
      };
    } catch (error) {
      try {
        cycleTLSExit();
      } catch (_cleanupError) {
        // ignore cleanup errors
      }
      throw new Error(
        `X connector sync failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  async execute(_ctx: ActionContext): Promise<ActionResult> {
    return { success: false, error: 'Actions not supported' };
  }
}
