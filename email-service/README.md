# Email Service

This service sends emails through the Gmail API and now provides AI assisted copy generation.

## Authentication

1. Place your Google OAuth client credentials in `credentials.json`.
2. Start the service and call `POST /auth/init` once to open the browser flow and store a `token.json`.
3. Use `GET /auth/status` to check validity and `POST /auth/reset` to remove the token if you need to reauthenticate.

## Quota limits

Email sending is rate limited using an in‑memory limiter.

| Variable | Default | Description |
|---|---|---|
| `EMAIL_RATE_PER_SECOND` | `1` | Maximum emails attempted per second. |
| `EMAIL_RATE_PER_MINUTE` | `60` | Maximum emails attempted per minute. |

Tune these environment variables to match your quota.

## AI copy generation

`POST /generate-copy` accepts a `prompt` and `contact` object, forwards them to the LLM configured by `LLM_API_URL`, `LLM_MODEL`, and authenticates with `LLM_API_KEY`. Responses are cached in a SQLite table `email_copy_cache` to avoid repeated API calls.
