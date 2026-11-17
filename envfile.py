"""Load KEY=value lines from the repo's .env into os.environ.

Was four identical copies (scrape_patents/donors/flights/wsb), each with a
note to factor it out "once a third phase wants one". run_job.py makes five
-- it checks for a scraper's key before running it. Not python-dotenv: this
is the whole format the project uses, and one less dependency.

Real environment variables win over .env (setdefault), so an exported key
overrides the file.
"""

import os

DOTENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def load_dotenv(path=DOTENV_PATH):
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
