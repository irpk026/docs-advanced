# Before you arrive

**About 15 minutes at home saves you the slowest part of the session.** Set up IBM Bob and
install the tools before **Wednesday 14 October, 15:00**. Verification emails and
installers take the longest, so get them out of the way at home. You'll get the workshop
files and the step-by-step guide in the room.

[:material-file-pdf-box: Download the printable one-pager (PDF)](assets/agentic-ai-live-before-you-arrive.pdf){ .md-button }

---

## 1. IBMid and IBM Bob (about 5 minutes)

- [ ] **IBM Bob trial**: [bob.ibm.com/trial](https://bob.ibm.com/trial) → **Start your free
  trial**. Sign in with your IBMid, or create one when asked. Free for 30 days, no payment
  details.
- [ ] **Sign up with your email address**, not **Continue with Google** — social login
  causes access problems later in the lab. No verification code? Check spam and use the
  newest one.

!!! warning "Don't create your Confluent Cloud account yet"
    We create it together in the lab, because a promo code has to go in at exactly the
    right moment to avoid being asked for a credit card. If you already have a Confluent
    account, that's fine — bring the login.

---

## 2. Install (about 10 minutes)

- [ ] **Bob IDE**: [bob.ibm.com/download](https://bob.ibm.com/download). Open it and sign
  in with your IBMid. macOS: **mac-ARM** (M1 or newer) or **mac-Intel**. Windows:
  **x64 (User)**.
- [ ] **watsonx Orchestrate ADK extension**: in Bob IDE, open the Extensions view
  (`Cmd+Shift+X` / `Ctrl+Shift+X`), search for **watsonx Orchestrate** and click
  **Install**. Install it only — don't open it or set up a workspace with it.
- [ ] **Python 3.11, 3.12 or 3.13**: check with `python3 --version` (Mac) or
  `python --version` (Windows).
  Install: `brew install python@3.11` · `winget install Python.Python.3.11` (on Windows,
  tick **Add Python to PATH**).
- [ ] **uv**: check with `uv --version`.
  Install: `brew install uv` · `winget install astral-sh.uv`. Then open a **new**
  terminal and check again.

---

## 3. Optional: watsonx Orchestrate (about 10 minutes)

Do this at home if you have time — otherwise we do it together at the start of the session.

- [ ] **Start the trial** with the **same IBMid**:
  [ibm.com/products/watsonx-orchestrate](https://www.ibm.com/products/watsonx-orchestrate)
  → **Start your free trial**. Name the instance `openslava-lab-<yourname>` and choose
  **Frankfurt (eu-de)**. You're done when you see the watsonx Orchestrate chat and the
  **Build** menu.
- [ ] **Get your credentials**: **profile icon** → **Settings** → **API details**. Copy the
  **Service instance URL**, then **Generate API key** → **Create** → **Copy**. The key is
  shown **only once** — save both somewhere you can open on the day.

---

## Bring

- A laptop you can **install software on** — work laptops sometimes block installers, so
  try at home first
- Your charger, and access to your email for verification codes
- If you did step 3: your watsonx Orchestrate URL and API key

---

## You're ready when

- [ ] Bob IDE opens and you're signed in
- [ ] The watsonx Orchestrate ADK extension is installed in Bob IDE
- [ ] `python3 --version` shows 3.11–3.13, and `uv --version` prints a version
- [ ] *Optional:* you can log in to watsonx Orchestrate, and your instance URL and API key are saved

Stuck on any step? Don't worry — bring your laptop and we'll fix it in the first ten
minutes. Note which step failed and the error message.
