# Instructor notes — OpenSlava 2026

**Not published.** This file lives outside `docs/`, so it never reaches the built site.
Keep anything attendees shouldn't read in here, not in `docs/`.

> MkDocs publishes `docs/` only. HTML comments in those `.md` files **do** survive into
> the rendered page source — don't hide instructor notes in `<!-- ... -->`.

## Before the session

- **Promo code.** The lab publishes Confluent's public codes (`CONFLUENTDEV1` for
  skipping the credit card, `KAFKA101` for $25 extra). If you have an event-specific
  code, add it to the table in `docs/lab/index.md`, Section 2.1.
  - `CONFLUENTDEV1` works on **first-time accounts only**. Anyone who has signed up
    before will be asked for a card — have a fallback code ready for them.
- **Dry run.** Work through Sections 2–4 once on a throwaway Confluent account,
  re-running `bash docs/lab/check-lab.sh` after each. All-green means the lab holds.
  Then `--e2e` for the full pipeline.
- **Region.** The setup guide tells attendees to put watsonx Orchestrate in Frankfurt (eu-de).
  If your Confluent cluster is outside Europe, every agent call crosses regions and
  Section 6 feels slower than it needs to.

## During the session

- The promo code field is behind a **"Have a promo code?" → Click Here** link at the
  **bottom** of Confluent's payment screen. Call this out before anyone starts — it's
  the single most likely place to lose the room.
- Attendees must redeem the code **before** creating a cluster, or Confluent blocks
  cluster creation until a payment method exists.
- Section 4 is the long one (20 min of Bob prompts). If you're behind, the knowledge
  base in 4.4 indexes while you talk — start it early.

## Credentials

Never commit them. `.gitignore` covers `.env`, `.env.*`, `*.key`, `*.pem` at any depth.
Attendees get placeholders via `.env.example` inside the workspace zip.
