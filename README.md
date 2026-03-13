# TikTok Boosting Automation

This project automates your TikTok Business API workflow:

1. Find latest campaign containing `acemate_Views`
2. Copy campaign
3. Rename copied campaign to `acemate_Views_YYYY_MM_DD`
4. Clear bid on copied ad groups (when bid is set)
5. Update copied ads to latest TikTok post `tiktok_item_id`
6. Enable campaign, ad groups, and ads

## Setup

1. Create a TikTok developer app and complete OAuth once.
2. Export your long-lived token as `TIKTOK_ACCESS_TOKEN`.
3. Optionally override advertiser/base URL/substrings via env vars.

Reference links:

- App: https://business-api.tiktok.com/portal/apps/7548665268394786817
- OAuth/token docs: https://business-api.tiktok.com/portal/docs?id=1853005424300034

Get access token (exchange `auth_code` for `access_token`):

```bash
curl --location --request POST 'https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "app_id": "YOUR_APP_ID",
    "secret": "YOUR_APP_SECRET",
    "auth_code": "AUTH_CODE_FROM_CALLBACK"
  }'
```

Refresh access token (recommended when expired):

```bash
curl --location --request POST 'https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token/' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "app_id": "YOUR_APP_ID",
    "secret": "YOUR_APP_SECRET",
    "refresh_token": "YOUR_REFRESH_TOKEN"
  }'
```

Token lifetime:

- `access_token` expires in 24 hours.
- `refresh_token` expires in 1 year.
- You can refresh the access token once every 24 hours per refresh token.

```bash
cp .env.example .env
# Fill in .env
set -a
source .env
set +a
```

## Run

Dry run (recommended first):

```bash
./automation.py --dry-run
```

Full run:

```bash
./automation.py
```

## GitHub Actions

Two workflows are included:

- `.github/workflows/tiktok_boost_manual.yml`
- `.github/workflows/tiktok_spend_guard.yml`

### Required repository secrets

- `TIKTOK_APP_SECRET` (your Business API app secret)
- `SLACK_BOT_TOKEN` (optional, only if you want Slack completion messages)

### Workflow 1: `TikTok Boost Manual`

Run manually from Actions with these inputs:

1. `video_link`:
   - TikTok URL like `https://www.tiktok.com/@acemate.ai/video/7616731647878155552`
   - or numeric `item_id`
2. `oauth_url`:
   - Must be the OAuth **callback URL after approval**, containing `code=` or `auth_code=`
   - If you paste the initial auth URL (without code), token exchange will fail by design
3. Optional `source_campaign_name` (default: `acemate_Views_2026_03_12`)
4. Optional `date_tag` (`YYYY_MM_DD`, default is UTC today)
5. Optional `spend_limit_eur` (default `10`)

What it does:

- extracts `app_id` + `auth_code` from the callback URL
- exchanges `auth_code` at `/oauth2/access_token/` using `TIKTOK_APP_SECRET`
- runs `automation.py` with the provided video link
- stores monitor state in GitHub Actions variable `TIKTOK_MONITOR_STATE` for the guard workflow

### Workflow 2: `TikTok Spend Guard`

Runs hourly (`cron: 0 * * * *`) and can also be triggered manually.

Behavior:

- reads `TIKTOK_MONITOR_STATE`
- waits until at least 6 hours have passed from the boost run
- checks spend for the created campaign via `/report/integrated/get/`
- disables the campaign via `/campaign/status/update/` when spend is `>= spend_limit_eur`
- clears `TIKTOK_MONITOR_STATE` after disable (or if stale > 30h)

Important:

- Guard workflow requires token scope for TikTok reporting (`/report/integrated/get/`).
- The stored access token is reused from the initial run (works for your 24h campaign window).

Useful flags:

- `--no-enable` to skip final enabling step
- `--date-tag 2026_03_13` to force a specific naming date
- `--campaign-name-substring acemate_Views` to change source campaign matching
- `--source-campaign-name acemate_Views_2026_03_12` to copy a specific source campaign exactly
- `--post-list-endpoints /spark_ad/post/list/,/tt_video/list/` to control fallback order
- `--latest-item-id 7400713033997159713` (or a full TikTok video URL) to force a specific TikTok post
- `--video-auth-code "<fresh_video_auth_code>"` to refresh Spark post authorization before latest-post lookup
- `--allow-ad-item-fallback` to use latest existing-ad item id when post endpoints are stale/expired
- `--slack-channel-id C0A49B277UK` to choose Slack channel for completion notification
- `--verbose` for debug logs

## Notes

- API base URL defaults to `https://business-api.tiktok.com/open_api/v1.3`.
- Advertiser ID defaults to `7441895227339718673`.
- The script filters/sorts campaigns client-side by timestamp for reliability.
- Latest post fetch uses fallback endpoints (default order: `/spark_ad/post/list/`, then `/tt_video/list/`).
- The selected creative `tiktok_item_id` comes from latest post endpoints (unless you set `--latest-item-id`).
- If you pass `--video-auth-code`, the script calls `/tt_video/authorize/` first to refresh post authorization.
- If a post endpoint omits publish/create timestamps, the script falls back to highest numeric `item_id` as latest.
- Posts with `auth_info.ad_auth_status=EXPIRED` are ignored.
- If post endpoints return only expired/unusable posts, the script fails by default (to avoid boosting old videos).
- Optional fallback: enable `--allow-ad-item-fallback` to use highest `tiktok_item_id` currently found in existing ads.
- If `SLACK_BOT_TOKEN` is set, the script posts a completion message in Slack with a Manage Campaigns link.
- Slack channel defaults to `C0A49B277UK` (override with `SLACK_CHANNEL_ID` or `--slack-channel-id`).
- If campaign creation hits "Campaign name already exists", the script reuses the existing same-name campaign.
- Optional `TIKTOK_MANAGE_CAMPAIGN_URL` lets you customize the second Slack link.
- If `/campaign/copy/` is unavailable (404), the script automatically falls back to create-based duplication:
  - create campaign from source campaign settings
  - recreate source ad groups with matching settings (including audience/targeting fields when available)
  - recreate ads in the new ad groups
- If TikTok requires stricter payload fields in your account configuration, you may need to add extra fields to `/ad/update/` or `/adgroup/update/` payloads.
