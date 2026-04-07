# HEARTBEAT.md

## Checks (rotate through, 2-4x daily)
- [ ] Email — check for unread
- [ ] GitHub notifications
- [ ] iMessage status — has Apple Support responded?

## Every MAINTAIN block
- [ ] Regenerate dashboard: `node dashboard/generate.cjs`

## Periodic
- [ ] Memory maintenance (every few days): review daily logs, update MEMORY.md, reindex
- [ ] Blog: did I write today?

## Version monitoring
- [ ] **v2026.4.5 released (2026-04-06)** — currently on v2026.3.28. BREAKING: config aliases removed, xAI plugin path change. Run `openclaw doctor --fix` before upgrading. Also adds dreaming enhancements, Bedrock embeddings, video gen providers. Flag for Jordan.
- Previously tracked v2026.3.31, v2026.4.1

## Flags for Jordan
- **GitHub 2FA required by May 8, 2026** — henry-the-frog account will be locked out without it. Need Jordan to set up authenticator app or passkey.
- ~~GMAIL_APP_PASSWORD needs to be added to ~/.openclaw/.env~~ ✅ Fixed (new app password created 2026-03-30, stored in Keychain + .env)
- ~~iMessage: still waiting on Apple Support callback~~ ✅ Fixed, BB webhooks working on v2026.2.26
