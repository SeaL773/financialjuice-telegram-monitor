# Security Policy

## Reporting a vulnerability

If you find a security issue in this project (auth handling, cookie/session
storage, translation request construction, Telegram message handling, or
anything else), please report it privately rather than opening a public
issue.

Use GitHub's built-in private reporting for this repository:
**Security tab → Report a vulnerability** (GitHub Security Advisories). This
creates a private advisory visible only to the maintainer until a fix is
ready, without requiring any personal contact details.

If private advisories are unavailable for any reason, open a regular issue
with the security details omitted and a note that you have a report to send
privately; the maintainer will follow up with a secure channel.

Please include, where possible:

- A description of the issue and its potential impact
- Steps to reproduce, or a minimal example
- The affected file(s) or component (for example `src/auth/`,
  `src/translate/translator.py`, `src/telegram/`)
- Whether the issue requires a live FinancialJuice/Telegram/translation
  credential to reproduce

There is no bug bounty program. Reports are handled on a best-effort basis by
a single maintainer, not a security team with guaranteed response times.

## Supported versions

This project does not maintain multiple release branches. Security fixes are
applied to the latest commit on the default branch only.

## Secrets handling

This repository is not affiliated with FinancialJuice, Telegram, or any
translation provider. It is a personal-use client, and the following applies
to anyone running or forking it:

- **Never commit real credentials.** `FJ_EMAIL`, `FJ_PASSWORD`,
  `TG_BOT_TOKEN`, `TG_CHAT_ID`, `TG_THREAD_ID`, `FJ_TRANSLATE_API_KEY`, and
  any deprecated `KIMI_*` alias are secrets. Keep them only in a local `.env`
  file, which is already excluded via `.gitignore`.
- **`data/cookies.json` is a live session credential**, not a cache file.
  Anyone with that file can act as your authenticated FinancialJuice session
  until it expires or is invalidated. It lives under `data/`, which is also
  gitignored, and should never be attached to issues, logs, or screenshots.
- **Rotate before you assume, don't assume before you rotate.** If a secret
  was ever committed, pasted into an issue, or logged, treat it as
  compromised: rotate the FinancialJuice password, regenerate the Telegram
  bot token, and reissue the translation provider API key, even if you
  believe it was later removed from history.
- **Scrub, don't just delete.** A later commit that removes a secret does not
  remove it from Git history. Use `git filter-repo` or the BFG Repo-Cleaner
  to rewrite history before making a repository with prior leaked secrets
  public, and rotate the exposed credentials regardless.
- **Translation payloads leave this codebase.** Enabling translation sends
  the full English headline/editorial text to whatever
  `FJ_TRANSLATE_BASE_URL` you configure. Only point this at a provider you
  are willing to share that content with.
- **Custom headers can carry secrets.** `FJ_TRANSLATE_HEADERS_JSON` is merged
  into every translation request; avoid putting sensitive values there in
  shared configuration examples, screenshots, or issue reports.

## Scope

This policy covers the code in this repository. It does not cover
FinancialJuice's own infrastructure, Telegram's platform, or any third-party
translation provider; report issues in those systems to their respective
owners.
