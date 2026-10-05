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
├── setup/                      # Part 0 — accounts, Bob IDE, ADK setup
└── lab/                        # The lab: event-driven AI agents
    ├── index.md                # Lab guide
    ├── exercises.md            # Stretch exercises
    ├── import-all.sh           # wxO setup verification script (downloadable)
    └── bobchestrate-confluent.zip   # The single workspace attendees download
src/advanced-bob-config/        # Source of the .bob/ workspace configuration
archive/                        # Superseded workspace bundles, kept for reference
```

The attendee workspace is distributed as `docs/lab/bobchestrate-confluent.zip`.
It contains both the `.bob/` Bob IDE configuration and the pre-built
`retail-inventory-optimization/` lab code, so it is the only download in the lab.
