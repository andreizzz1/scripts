# Test cases (full bot)

This is a comprehensive test-case checklist for the Python rewrite (and also maps to the Rust bot behavior).

## Environment/config

- Missing mandatory env vars fail fast: `TELOXIDE_TOKEN`, `DATABASE_URL`, `HELP_ADMIN_CHANNEL_*`, `HELP_ADMIN_CHAT_*`, `HELP_GIT_REPO`.
- Optional env vars fall back to defaults: `TOP_LIMIT`, `GROWTH_MIN/MAX`, `GROW_SHRINK_RATIO`, `NEWCOMERS_GRACE_DAYS`, `GROWTH_DOD_BONUS_MAX`, `PVP_*`, `ANNOUNCEMENT_*`.
- Invalid numeric env values: non-int in `TOP_LIMIT`, non-float in ratios, negative limits.
- `DOD_SELECTION_MODE` parsing: `RANDOM`, `EXCLUSION`, `WEIGHTS`, invalid value.
- `DOD_RICH_EXCLUSION_RATIO` behavior: `<0`, `0`, `(0,1)`, `1`, `>1`.
- Feature toggles: `*_ENABLED` on/off and their effect on handlers.

## Database/migrations

- Fresh database: all SQL migrations in `../migrations/*.sql` apply in order successfully.
- Re-running migrations is idempotent (no errors; `schema_migrations` prevents double apply).
- Trigger behavior:
  - `Dicks` trigger blocks second grow on same date (`sqlstate=GD0E1`) unless `bonus_attempts > 0`.
  - `Dick_of_Day` trigger blocks choosing DoD twice per chat/day (`sqlstate=GD0E2`) and returns winner name in message.
  - Loan trigger sets `repaid_at` when debt becomes zero.

## /start, /help, /privacy

- `/help` returns localized help page (EN vs RU).
- `/privacy` returns localized policy page (EN vs RU).
- `/start` without args returns greeting + help; HTML escaping for user first name.
- `/start promo_*` deeplink:
  - Valid base64 code decode.
  - Invalid base64 / invalid UTF-8 handling.
  - Non-existent/expired promo behavior.

## /grow

- First grow creates user + dick row (length changes by increment).
- Second grow same day returns `commands.grow.tomorrow` (no length change).
- Bonus attempts allow extra grows within the same day.
- Newcomers grace days: during first `NEWCOMERS_GRACE_DAYS`, negative increments never occur.
- Range edges:
  - `GROWTH_MIN > 0` => always positive.
  - `GROWTH_MAX < 0` => always negative.
- Perks:
  - `help-pussies`: when current length < 0, adds `round(coef * abs(length))`.
  - `loan-payout`: when loan exists and base increment > 0, reduces growth and decreases debt.
- Response formatting:
  - “grown” vs “shrunk” translation branch.
  - Includes `time_till_next_day` suffix.
  - Includes position in top when `TOP_UNLIMITED_ENABLED=true`.

## /top + pagination callbacks

- Empty chat returns `commands.top.empty`.
- Non-empty chat:
  - Correct ordering: `length DESC, updated_at DESC, name`.
  - Current user is underlined.
  - `[+]` marker shows if user can still grow today.
- Pagination:
  - When `TOP_UNLIMITED_ENABLED=false`, no pagination keyboard.
  - When enabled, page 0 has only right arrow if more pages.
  - Page > 0 has left arrow; right arrow only if more pages.
  - Invalid callback data: non-int page / negative page.
  - Callback from inline message (when implemented) resolves chat correctly.

## /dick_of_day (/dod)

- No candidates => `commands.dod.no_candidates`.
- Candidate selection modes:
  - `RANDOM`: uniform random among active (updated within last week).
  - `EXCLUSION`: excludes top-N% by length (`DOD_RICH_EXCLUSION_RATIO`).
  - `WEIGHTS`: poor-in-priority weights formula.
- Winner gets bonus increment; `bonus_attempts` decreases accordingly.
- Second call same day returns `commands.dod.already_chosen`.
- Response includes `time_till_next_day` suffix and optional announcements (when implemented).

## /loan

- Disabled when `LOAN_PAYOUT_COEF <= 0` or `>= 1` => `errors.feature_disabled`.
- Positive length => `commands.loan.errors.positive_length`.
- Negative length => confirmation message + inline keyboard.
- Multiple loans:
  - If `MULTIPLE_LOANS_ENABLED=false` and active loan exists => returns `commands.loan.debt`.
  - If enabled, allow multiple active loans per user/chat (if schema/logic supports).
- Callback:
  - Only initiator can press buttons.
  - Confirmed with unchanged payout ratio issues loan (length reset to 0, debt created).
  - Confirmed with changed ratio => `commands.loan.callback.payout_ratio_changed`.
  - Old callback format without ratio is handled as “ratio changed”.
  - Refused: message is deleted or edited to refused text.

## /stats

- Private chat stats:
  - Chats count, max length, total length across all chats.
- Group chat stats:
  - Dick length + position.
  - PVP stats: win rate, battles, wins, max win streak, acquired/lost.
  - Notice shown when `PVP_STATS_SHOW_NOTICE=true`.
- When `PVP_STATS_SHOW=false`, handler is a no-op (no reply).

## /pvp (not yet ported in Python code)

- Command arg parsing: missing args error, invalid number, bet bounds.
- Bet requirement: initiator and acceptor have enough length (if enabled).
- Callback:
  - Reject self-fight.
  - Lock prevents double-processing on repeated clicks.
  - Winner/loser length transfer and bonus attempts update.
  - Battle stats are updated atomically.
  - Loan payout withholding message when applicable.
- Inline query:
  - Query text parses as bet; builds article with button.
  - Chosen inline result increments metrics (optional).

## /import (not yet ported in Python code)

- Admin-only restriction.
- Must be reply to non-forwarded message from supported bots.
- Parsing:
  - Correctly parses valid lines into (username, length).
  - Invalid lines are reported via `commands.import.errors.invalid_lines`.
- Import semantics:
  - Adds lengths to existing dicks; missing dicks are reported separately.
  - Prevent double import of the same source message/users (if tracked).
- Requires temporary admin rights to read message when privacy mode enabled.

## /promo (not yet ported in Python code)

- Dialog flow in private chat:
  - `/promo` asks for code, next message activates.
  - Inline switch button uses deeplink param.
- Activation rules:
  - Code exists and within date window; capacity available.
  - Prevent double activation per user.
  - When user has no dicks => `commands.promo.errors.no_dicks`.
- Growth result:
  - Applies bonus length to one or multiple chats; correct pluralization text.

## Localization and HTML safety

- Any user-provided string rendered in HTML is escaped (`name`, imported names, etc.).
- Locale fallback:
  - Unsupported language codes fall back to English.
  - Missing translation keys fail fast in dev/testing.

