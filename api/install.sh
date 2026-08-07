#!/usr/bin/env bash
# Verified install sequence for the stage 4 API venv (Python 3.12).
#
# pymilvus==2.4.9 needs setuptools<81 (pkg_resources was removed in 81+)
# and a newer environs than it declares (its <=9.5.0 pin resolves to an
# environs version that ships with a marshmallow version incompatible with
# the marshmallow actually resolved here) — same interaction hit in
# enrichment/install.sh. Installing pymilvus first with setuptools<81
# pinned, then upgrading environs afterward, sidesteps it.
set -e

pip install --upgrade pip
pip install "setuptools<81" fastapi==0.115.0 "uvicorn[standard]==0.32.0" \
    httpx==0.27.2 psycopg2-binary==2.9.9 pymilvus==2.4.9 neo4j==5.28.1 \
    python-dotenv==1.0.1
pip install -U environs

echo "install.sh: done"
