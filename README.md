# Agentic AI, Live: Orchestrating Agents with Real-Time Data

MkDocs source for the **OpenSlava 2026** hands-on lab (Day 1, 14 October, 15:00–17:30, Room 2): Confluent Cloud + Flink SQL +
IBM watsonx Orchestrate, built with IBM Bob.

## Run the site locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
mkdocs serve
```

Then open <http://127.0.0.1:8000>.

Build a static site into `site/` with `mkdocs build`.

## Deployment Options

### Option 1: GitHub Pages

The repository is configured for GitHub Pages (`https://irpk026.github.io/docs-advanced/`). Deploy with:

```bash
source .venv/bin/activate
mkdocs gh-deploy
```

### Option 2: IBM Cloud Code Engine (Serverless Container)

Deploy as a lightweight containerized Nginx application using the included `Dockerfile` and `nginx.conf`:

1. Target your IBM Cloud resource group and region:
   ```bash
   ibmcloud target -g <resource-group> -r <region>
   ibmcloud ce project select --name <project-name>
   ```

2. Build and deploy directly from local source (uses `.ceignore` to filter build artifacts):
   ```bash
   ibmcloud ce app create \
     --name agentic-ai-live \
     --build-source . \
     --port 8080 \
     --min-scale 0 \
     --max-scale 2 \
     --cpu 0.25 \
     --memory 0.5G
   ```

3. Update an existing deployment after local edits:
   ```bash
   ibmcloud ce app update \
     --name agentic-ai-live \
     --build-source .
   ```

### Option 3: Code Engine Custom Domain Mapping

To use a custom domain (e.g. `docs.yourdomain.com`) instead of the default `*.codeengine.appdomain.cloud` URL:

1. Create a TLS certificate secret in Code Engine:
   ```bash
   ibmcloud ce secret create --name my-cert-secret \
     --format tls \
     --cert-chain-file cert.pem \
     --private-key-file key.pem
   ```

2. Map your custom domain to the application:
   ```bash
   ibmcloud ce domainmapping create \
     --name docs.yourdomain.com \
     --component agentic-ai-live \
     --tls-secret my-cert-secret
   ```

3. Add a `CNAME` record in your DNS provider pointing `docs.yourdomain.com` to `custom.<region>.codeengine.appdomain.cloud`.

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
