# Owletto Connectors

Installable connector plugins for [Owletto](https://github.com/buremba/owletto).

Each connector is a standalone TypeScript file that extends `ConnectorRuntime` from `@owletto/sdk` and can be installed into an Owletto instance via the `manage_connections` tool.

## Available Connectors

| Connector | Type | Description |
|-----------|------|-------------|
| Reddit | API | Subreddit posts and comments |
| GitHub | API | Repository issues, PRs, and discussions |
| Hacker News | API | Stories and comments |
| Google Play | API | App store reviews |
| Google Maps | API | Place reviews |
| iOS App Store | API | App reviews via RSS |
| Trustpilot | Browser | Business reviews |
| G2 | Browser | Product reviews |
| Capterra | Browser | Software reviews |
| Glassdoor | Browser | Company reviews |
| X (Twitter) | Browser | Posts and mentions |

## Installation

Use the `manage_connections` tool with `install_connector`:

```
action: install_connector
source_url: https://raw.githubusercontent.com/buremba/owletto-sources/main/connectors/reddit.ts
```

## Compiling Locally

To pre-compile connectors and generate a `manifest.json`:

```bash
npx @owletto/sdk compile connectors
```

This bundles each connector with esbuild and writes compiled JS + manifest to `dist/`.

## Writing Custom Connectors

Connectors extend `ConnectorRuntime` from `@owletto/sdk`. Each connector defines:

- `definition` — metadata, auth schema, feeds, and options schema
- `sync(ctx)` — fetches data and returns events + checkpoint
- `execute(ctx)` — handles write actions (optional)

See existing connectors for examples. The [GitHub connector](connectors/github.ts) demonstrates both sync and execute (read + write actions).
