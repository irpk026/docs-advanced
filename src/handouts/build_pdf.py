#!/usr/bin/env python3
"""
Builds docs/assets/agentic-ai-live-before-you-arrive.pdf from before-you-arrive.html.

Needs Google Chrome. Run from the repo root:
    python3 src/handouts/build_pdf.py
"""

import base64
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path


HERE = Path(__file__).resolve().parent
OUT = HERE.parent.parent / "docs" / "assets" / "agentic-ai-live-before-you-arrive.pdf"
CHROME = {
    "darwin": "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "linux": "google-chrome",
    "win32": r"C:\Program Files\Google\Chrome\Application\chrome.exe",
}[sys.platform]



def embed_fonts(html: str) -> str:
    """Inlines the Google Fonts stylesheet and its font files, so Chrome never falls back."""
    link = re.search(r'<link href="(https://fonts.googleapis.com/css2[^"]+)" rel="stylesheet">', html)
    agent = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 Chrome/126 Safari/537.36"}
    css = urllib.request.urlopen(urllib.request.Request(link.group(1).replace("&amp;", "&"), headers=agent)).read().decode()

    def inline(match):
        data = urllib.request.urlopen(urllib.request.Request(match.group(1), headers=agent)).read()
        return f"url(data:font/woff2;base64,{base64.b64encode(data).decode()})"

    css = re.sub(r"url\((https://[^)]+)\)", inline, css)
    html = re.sub(r'<link rel="preconnect"[^>]*>\n?', "", html)
    return html.replace(link.group(0), f"<style>{css}</style>")


html = embed_fonts((HERE / "before-you-arrive.html").read_text(encoding="utf-8"))

with tempfile.TemporaryDirectory() as tmp:
    page = Path(tmp) / "handout.html"
    page.write_text(html, encoding="utf-8")
    subprocess.run([
        CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
        "--virtual-time-budget=5000",
        f"--print-to-pdf={OUT}", page.as_uri(),
    ], check=True, capture_output=True)

print(f"Wrote {OUT}")
