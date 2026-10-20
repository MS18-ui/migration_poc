"""Validation of a rich blueprint: does the YAML contain all the source logic, and do the tests capture it?

Layers (see run_validation in pipeline.py):
  1. static checks   - column/type flow, source<->YAML coverage, compatibility & security rules
  2. node scenarios  - generated test data per operation (boundaries, nulls, empties, duplicates, joins...)
  3. mutation tests  - perturb the YAML; every perturbation must be caught by some scenario
  4. synthesis       - search for data that kills surviving mutants
  5. agent           - bounded review loop over the above; its claims never count as passes
"""
from .findings import Finding, Report  # noqa: F401
