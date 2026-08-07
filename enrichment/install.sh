#!/usr/bin/env bash
# Verified install sequence for the stage 3 enrichment venv (Python 3.12,
# CPU-only). Run from the enrichment/ directory after `python3 -m venv .venv`.
#
# Why a script and not a plain `pip install -r requirements.txt`: laion-clap
# declares an exact `numpy==1.23.5` dependency, which has no Python 3.12
# wheel and fails to build from source under modern setuptools (removed
# distutils / pkgutil.ImpImporter). We install laion-clap with --no-deps and
# pin its transitive deps ourselves to versions verified to work together.
set -e

pip install --upgrade pip setuptools

pip install --extra-index-url https://download.pytorch.org/whl/cpu \
    torch==2.4.1 torchvision==0.19.1

pip install --no-deps laion-clap==1.1.6

pip install \
    soundfile librosa torchlibrosa ftfy braceexpand webdataset wget wandb \
    llvmlite scipy scikit-learn pandas h5py tqdm regex progressbar \
    transformers==4.36.2

# pymilvus needs setuptools<81 (pkg_resources was removed in 81+) and a
# newer environs than it declares (its <=9.5.0 pin ships with an old
# marshmallow that's incompatible with the marshmallow actually resolved
# here); both overrides are safe in practice.
pip install "setuptools<81" pymilvus==2.4.9 neo4j==5.28.1 \
    psycopg2-binary==2.9.9 boto3==1.34.162 python-dotenv==1.0.1
pip install -U environs

echo "install.sh: done"
