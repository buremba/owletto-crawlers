# Owletto Crawlers

Installable crawler plugins for [Owletto](https://github.com/buremba/owletto).

Each crawler is a standalone TypeScript file that imports from `@owletto/sdk` and can be installed into an Owletto instance via the `manage_crawler_templates` MCP tool.

## Available Crawlers

| Crawler | Type | Description |
|---------|------|-------------|
| Reddit | API | Subreddit posts and comments |
| GitHub | API | Repository issues and discussions |
| Hacker News | API | Stories and comments |
| Google Play | API | App store reviews |
| Google Maps | API | Place reviews |
| Trustpilot | Browser | Business reviews |
| G2 | Browser | Product reviews |
| Capterra | Browser | Software reviews |
| Glassdoor | Browser | Company reviews |
| iOS App Store | Browser | App reviews |
| X (Twitter) | Browser | Posts and mentions |

## Installation

Use the `manage_crawler_templates` tool with the `install` action:

```
action: install
registry_slug: reddit
```

Or install from a URL:

```
action: install
source_url: https://raw.githubusercontent.com/buremba/owletto-crawlers/main/crawlers/reddit.ts
```

## Writing Custom Crawlers

Crawlers extend base classes from `@owletto/sdk`:

- `BaseCrawler` - Simple crawlers
- `ApiPaginatedCrawler` - API-based with pagination
- `BrowserPaginatedCrawler` - Browser-based with pagination

See existing crawlers for examples.
