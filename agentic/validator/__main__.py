"""CLI:  python -m agentic.validator --spec pipeline_spec.yaml --source legacy_job.py [--out DIR] [--agent offline|llm]

Exit status: 0 = PASSED, 1 = FAILED (error findings), 2 = could not run, 3 = INCOMPLETE (a layer did not run,
e.g. no Spark on this host: static checks only). Run it where Spark + Java exist for the full result."""
import argparse
import sys

from ..validation import ValidationEngine
from .pipeline import Session, run_validation, write_outputs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="agentic.validator", description="Validate that a blueprint YAML captures all source logic")
    ap.add_argument("--spec", required=True, help="blueprint YAML (rich format)")
    ap.add_argument("--source", required=True, help="legacy source (.py PySpark or .hql/.sql)")
    ap.add_argument("--out", default="validation_out")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--n-random", type=int, default=10, help="random multi-row scenarios per node")
    ap.add_argument("--synth-budget", type=int, default=60, help="synthesis attempts per node with surviving mutants")
    ap.add_argument("--agent", choices=["none", "offline", "llm"], default="none",
                    help="run the review agent after the deterministic checks (offline = scripted, no network)")
    ap.add_argument("--metadata", nargs="*", default=None, metavar="PATH",
                    help="catalog metadata snapshot (DDL / YAML / folder) to re-check the YAML against "
                         "(default: metadata.snapshot in config.yaml; pass no value to skip)")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    try:
        bp = ValidationEngine.validate_yaml_structure(open(a.spec, encoding="utf-8").read())
        if not bp.is_rich:
            print("ERROR: the validator needs a rich blueprint (sources/transformations/targets).", file=sys.stderr)
            return 2
        src = open(a.source, encoding="utf-8").read()
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    from ..config import metadata_settings
    from ..metadata import load_snapshot
    mcfg = metadata_settings()
    mpaths = mcfg["snapshot"] if a.metadata is None else a.metadata
    mpaths = [mpaths] if isinstance(mpaths, str) else [x for x in (mpaths or []) if x]
    snapshot = load_snapshot(*mpaths) if mpaths else None
    say = (lambda m: None) if a.quiet else (lambda m: print("  ..", m))
    if a.agent == "none":
        report, suite, sess = run_validation(bp, src, a.source, a.out, progress=say, seed=a.seed,
                                             n_random=a.n_random, synth_budget=a.synth_budget,
                                             snapshot=snapshot, aliases=mcfg["aliases"])
    else:
        from .agent import run_review_agent
        sess = Session(bp, src, a.source, seed=a.seed, n_random=a.n_random, synth_budget=a.synth_budget, progress=say,
                       snapshot=snapshot, aliases=mcfg["aliases"])
        run_review_agent(sess, offline=(a.agent == "offline"))
        write_outputs(sess, a.out)
        report = sess.report
    print(report.to_markdown())
    print(f"\nOutputs written to {a.out}/ (validation_report.md, validation_report.json, test_suite.json)")
    return {"PASSED": 0, "FAILED": 1, "INCOMPLETE": 3}[report.result]


if __name__ == "__main__":
    sys.exit(main())

