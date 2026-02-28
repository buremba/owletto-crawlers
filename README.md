# Owletto Connectors

Installable connector plugins for [Owletto](https://github.com/buremba/owletto).

Each connector is a standalone TypeScript file that imports from `@owletto/sdk` and can be installed into an Owletto instance via the `manage_connections` tool.

## Available Connectors

| Connector | Type | Description |
|-----------|------|-------------|
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

Use the `manage_connections` tool with the `install_connector` action:

```
action: install_connector
registry_key: reddit
```

Or install from a URL:

```
action: install_connector
source_url: https://raw.githubusercontent.com/buremba/owletto-sources/main/connectors/reddit.ts
```

## Writing Custom Connectors

Connectors extend base classes from `@owletto/sdk`. See existing connectors for examples.
