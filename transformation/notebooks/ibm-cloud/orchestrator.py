"""
IBM Cloud - Medallion Pipeline Orchestrator.

Executes notebooks in sequence: Bronze → Silver → Gold
with error handling, timing, and status reporting.

Usage:
    python orchestrator.py                    # Run all layers
    python orchestrator.py --layer bronze     # Run only bronze
    python orchestrator.py --layer silver     # Run silver + gold
    python orchestrator.py --layer gold       # Run only gold
    python orchestrator.py --dry-run          # Validate without executing
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

NOTEBOOK_DIR = Path(__file__).parent
PIPELINE_ORDER = ["01_bronze_layer", "02_silver_layer", "03_gold_layer"]
LAYER_MAP = {
    "bronze": ["01_bronze_layer"],
    "silver": ["02_silver_layer"],
    "gold":   ["03_gold_layer"],
    "all":    PIPELINE_ORDER,
}

STATUS_FILE = NOTEBOOK_DIR / "pipeline_status.json"


def run_notebook(notebook_name: str, dry_run: bool = False) -> dict:
    """Execute a notebook via nbconvert and return status."""
    nb_path = NOTEBOOK_DIR / f"{notebook_name}.ipynb"
    if not nb_path.exists():
        return {"notebook": notebook_name, "status": "FAILED", "error": f"Not found: {nb_path}"}

    if dry_run:
        print(f"  [DRY-RUN] Would execute: {nb_path.name}")
        return {"notebook": notebook_name, "status": "DRY-RUN", "duration_s": 0}

    print(f"  Executing: {nb_path.name} ...")
    start = time.time()

    try:
        result = subprocess.run(
            [
                sys.executable, "-m", "jupyter", "nbconvert",
                "--to", "notebook",
                "--execute",
                "--inplace",
                "--ExecutePreprocessor.timeout=600",
                "--ExecutePreprocessor.kernel_name=python3",
                str(nb_path),
            ],
            capture_output=True,
            text=True,
            timeout=660,
        )
        duration = round(time.time() - start, 2)

        if result.returncode == 0:
            print(f"  ✓ {notebook_name} completed in {duration}s")
            return {"notebook": notebook_name, "status": "SUCCESS", "duration_s": duration}
        else:
            error_msg = result.stderr[-500:] if result.stderr else "Unknown error"
            print(f"  ✗ {notebook_name} FAILED after {duration}s")
            print(f"    Error: {error_msg[:200]}")
            return {"notebook": notebook_name, "status": "FAILED",
                    "duration_s": duration, "error": error_msg}

    except subprocess.TimeoutExpired:
        duration = round(time.time() - start, 2)
        print(f"  ✗ {notebook_name} TIMEOUT after {duration}s")
        return {"notebook": notebook_name, "status": "TIMEOUT", "duration_s": duration}
    except Exception as e:
        duration = round(time.time() - start, 2)
        return {"notebook": notebook_name, "status": "FAILED",
                "duration_s": duration, "error": str(e)}


def run_pipeline(layers: list[str], dry_run: bool = False) -> dict:
    """Execute the full pipeline with status tracking."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    print(f"=== Medallion Pipeline [{run_id}] ===")
    print(f"Layers: {', '.join(layers)}")
    print(f"Mode: {'DRY-RUN' if dry_run else 'EXECUTE'}")
    print()

    pipeline_start = time.time()
    results = []
    failed = False

    for notebook in layers:
        if failed:
            results.append({"notebook": notebook, "status": "SKIPPED"})
            print(f"  ⏭ {notebook} SKIPPED (previous failure)")
            continue

        result = run_notebook(notebook, dry_run)
        results.append(result)

        if result["status"] == "FAILED":
            failed = True

    total_duration = round(time.time() - pipeline_start, 2)
    overall_status = "FAILED" if failed else "SUCCESS"
    if dry_run:
        overall_status = "DRY-RUN"

    report = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": overall_status,
        "total_duration_s": total_duration,
        "layers_executed": len([r for r in results if r["status"] == "SUCCESS"]),
        "layers_total": len(layers),
        "results": results,
    }

    # Save status
    STATUS_FILE.write_text(json.dumps(report, indent=2))

    print()
    print(f"=== Pipeline {overall_status} in {total_duration}s ===")
    for r in results:
        status_icon = {"SUCCESS": "✓", "FAILED": "✗", "SKIPPED": "⏭",
                       "TIMEOUT": "⏱", "DRY-RUN": "○"}.get(r["status"], "?")
        dur = f" ({r.get('duration_s', 0)}s)" if r.get("duration_s") else ""
        print(f"  {status_icon} {r['notebook']}: {r['status']}{dur}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Medallion Pipeline Orchestrator")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"],
                        default="all", help="Which layer(s) to execute")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate pipeline without executing")
    parser.add_argument("--status", action="store_true",
                        help="Show last pipeline run status")
    args = parser.parse_args()

    if args.status:
        if STATUS_FILE.exists():
            print(STATUS_FILE.read_text())
        else:
            print("No pipeline runs found.")
        return

    notebooks = LAYER_MAP[args.layer]
    report = run_pipeline(notebooks, dry_run=args.dry_run)

    sys.exit(0 if report["status"] in ("SUCCESS", "DRY-RUN") else 1)


if __name__ == "__main__":
    main()
