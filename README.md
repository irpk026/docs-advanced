# Event-Driven AI Agents with watsonx Orchestrate

MkDocs source for the **Openslava 2026** hands-on lab: Confluent Cloud + Flink SQL +
IBM watsonx Orchestrate, built with IBM Bob.

## Run the site locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
mkdocs serve
```

Then open <http://127.0.0.1:8000>.

Build a static site into `site/` with `mkdocs build`.

## Layout

```text
mkdocs.yml                      # Site configuration and navigation
docs/
├── index.md                    # Workshop landing page
├── assets/                     # Shared images
├── setup/                      # Accounts, Bob IDE, workspace, ADK
└── lab/                        # The lab: event-driven AI agents
    ├── index.md                # Lab guide (Sections 0–7)
    ├── exercises.md            # Stretch exercises
    ├── exercises/              # Starter assets for the stretch exercises
    ├── check-lab.sh            # Full preflight check (downloadable)
    ├── import-all.sh           # wxO-only verification (downloadable)
    └── bobchestrate-confluent.zip   # The single workspace attendees download
INSTRUCTOR.md                   # Run-day notes — NOT published
src/advanced-bob-config/        # Source of the .bob/ workspace configuration
archive/                        # Superseded workspace bundles, kept for reference
```

## Checking an environment

`docs/lab/check-lab.sh` verifies tooling, workspace files, `.env` credentials,
Confluent topics and schemas, and the watsonx Orchestrate artifacts — each failure
naming the lab section that fixes it. Run it from an extracted
`bobchestrate-confluent/` workspace:

```bash
bash check-lab.sh          # read-only checks
bash check-lab.sh --e2e    # plus a real end-to-end pipeline test
```

It exits non-zero on failure, so it also works in CI.

## Publication boundary

MkDocs publishes `docs/` only. Anything placed there is public once the site is
live — including files MkDocs doesn't render, like the `.sh` and `.py` assets,
which are served verbatim. HTML comments in `.md` files survive into the page
source, so instructor notes belong in `INSTRUCTOR.md`, not in `<!-- ... -->`.

The attendee workspace is distributed as `docs/lab/bobchestrate-confluent.zip`.
It contains both the `.bob/` Bob IDE configuration and the pre-built
`retail-inventory-optimization/` lab code, so it is the only download in the lab.
