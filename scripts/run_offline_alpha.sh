#!/bin/sh
# Offline only. Reads frozen historical ranges; never starts/stops collectors or changes live config.
set -eu
cd "$(dirname "$0")/.."
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1
.venv/bin/python -m dh.research.offline_alpha cache
.venv/bin/python -m dh.research.offline_alpha_models
.venv/bin/python -m dh.research.offline_alpha_diagnostics
.venv/bin/python -m dh.research.offline_alpha_report
