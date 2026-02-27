"""
orchestrator.py

Reads a list of telecom operators from an Excel file, submits them all
as a single Batch API request, waits for completion, and saves results
to an output Excel file.

Usage:
    python orchestrator.py --input operators.xlsx --output results.xlsx

Excel input format expected:
    - Column named "Operator name" (or "operator", "name" — auto-detected)
    - Column named "Country" (or "country" — auto-detected)

Resume behaviour:
    If a batch was submitted in a previous run but not yet finished, a
    .batch_state.json file is written alongside the output file. On the
    next run the orchestrator re-attaches to that batch instead of
    submitting a new one.
    Operators that already have a result row in the output file are skipped
    regardless.
"""

import argparse
import time
import json
import os
from pathlib import Path

import anthropic
import pandas as pd
from search_agent import build_batch_request, parse_response, _empty_result


client = anthropic.Anthropic()

# Seconds between status-poll requests while the batch is processing
POLL_INTERVAL = 30

# If True, skip operators that already have a result row in the output file
RESUME_IF_OUTPUT_EXISTS = True


# ---------------------------------------------------------------------------
# Column name normalisation
# ---------------------------------------------------------------------------

OPERATOR_COL_CANDIDATES = ["operator name", "operator", "name", "company", "company name"]
COUNTRY_COL_CANDIDATES  = ["country", "country name", "nation", "market"]

def find_column(df: pd.DataFrame, candidates: list[str]) -> str:
    """Find the first matching column name (case-insensitive)."""
    lower_cols = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_cols:
            return lower_cols[candidate.lower()]
    raise ValueError(
        f"Could not find a column matching any of: {candidates}\n"
        f"Available columns: {list(df.columns)}"
    )


# ---------------------------------------------------------------------------
# Result flattening
# ---------------------------------------------------------------------------

def flatten_result(result: dict) -> dict:
    """Convert nested lists to pipe-separated strings for Excel compatibility."""
    flat = dict(result)
    for key in ["service_types", "recent_news", "flanker_brand_names", "mvno_names", "sources"]:
        if isinstance(flat.get(key), list):
            flat[key] = " | ".join(str(v) for v in flat[key]) if flat[key] else ""
    return flat


# ---------------------------------------------------------------------------
# Batch state persistence (for resume across interrupted runs)
# ---------------------------------------------------------------------------

def _state_path(output_path: str) -> str:
    return str(Path(output_path).with_suffix(".batch_state.json"))

def _load_state(output_path: str) -> dict:
    path = _state_path(output_path)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}

def _save_state(output_path: str, state: dict):
    with open(_state_path(output_path), "w") as f:
        json.dump(state, f, indent=2)

def _clear_state(output_path: str):
    path = _state_path(output_path)
    if os.path.exists(path):
        os.remove(path)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run(input_path: str, output_path: str, limit: int = None):
    print(f"\n📂 Loading operators from: {input_path}")
    df = pd.read_excel(input_path)
    print(f"   Found {len(df)} operators\n")

    # Detect columns
    operator_col = find_column(df, OPERATOR_COL_CANDIDATES)
    country_col  = find_column(df, COUNTRY_COL_CANDIDATES)
    print(f"   Using columns: '{operator_col}' and '{country_col}'\n")

    # Load existing results if resuming
    completed_names = set()
    existing_results = []

    if RESUME_IF_OUTPUT_EXISTS and os.path.exists(output_path):
        print(f"📋 Found existing output file — skipping already-completed operators.\n")
        existing_df = pd.read_excel(output_path)
        existing_results = existing_df.to_dict(orient="records")
        completed_names = set(existing_df["operator_name"].str.lower().tolist())

    # Build list of operators still pending
    pending = []
    for _, row in df.iterrows():
        name    = str(row[operator_col]).strip()
        country = str(row[country_col]).strip()
        if name.lower() not in completed_names:
            pending.append((name, country))

    if not pending:
        print("✅ All operators already completed. Nothing to do.")
        return

    if limit is not None:
        pending = pending[:limit]
        print(f"   Limiting to {len(pending)} operators (--limit {limit})\n")

    # Check whether a batch from a previous (interrupted) run is available
    state          = _load_state(output_path)
    batch_id       = state.get("batch_id")
    id_to_operator = state.get("id_to_operator", {})  # {custom_id: [name, country]}

    if batch_id:
        print(f"📋 Re-attaching to existing batch: {batch_id}\n")
    else:
        # Build and submit a new batch
        print(f"🚀 Submitting batch of {len(pending)} operators to the Batch API...\n")

        requests = []
        id_to_operator = {}
        for i, (name, country) in enumerate(pending):
            custom_id = f"op_{i}"
            id_to_operator[custom_id] = [name, country]
            requests.append(build_batch_request(name, country, custom_id))

        batch    = client.messages.batches.create(requests=requests)
        batch_id = batch.id

        _save_state(output_path, {
            "batch_id":        batch_id,
            "id_to_operator":  id_to_operator,
        })

        print(f"   Batch ID  : {batch_id}")
        print(f"   Operators : {len(pending)}")
        print(f"   Polling every {POLL_INTERVAL}s until complete...\n")

    # Poll until the batch finishes
    while True:
        batch  = client.messages.batches.retrieve(batch_id)
        counts = batch.request_counts
        print(
            f"   [{batch.processing_status}]  "
            f"succeeded={counts.succeeded}  "
            f"errored={counts.errored}  "
            f"processing={counts.processing}  "
            f"canceled={counts.canceled}"
        )
        if batch.processing_status == "ended":
            break
        time.sleep(POLL_INTERVAL)

    # Process results
    print(f"\n📊 Processing results...\n")
    results = list(existing_results)

    for result in client.messages.batches.results(batch_id):
        custom_id       = result.custom_id
        name, country   = id_to_operator.get(custom_id, [custom_id, "Unknown"])

        if result.result.type == "succeeded":
            data = parse_response(result.result.message.content, name, country)
            results.append(flatten_result(data))
            print(f"  ✅ {name}")
        else:
            error_info = getattr(result.result, "error", "Unknown error")
            results.append(flatten_result(_empty_result(name, country, error=str(error_info))))
            print(f"  ❌ {name}: {error_info}")

    _save_results(results, output_path)
    _clear_state(output_path)

    print(f"\n✅ All done! Results saved to: {output_path}")
    print(f"   Total operators processed: {len(results)}")


def _save_results(results: list[dict], output_path: str):
    """Save current results list to Excel."""
    out_df = pd.DataFrame(results)

    # Friendly column order
    preferred_order = [
        "operator_name", "country", "international_group", "data_year",
        "total_revenue", "mobile_revenue", "fixed_revenue",
        "ebitda", "ebitda_margin",
        "mobile_subscribers", "fixed_broadband_subscribers", "total_subscribers",
        "service_types",
        "has_flanker_brand", "flanker_brand_names",
        "has_mvno", "mvno_names",
        "recent_news", "sources", "error",
    ]
    ordered_cols = [c for c in preferred_order if c in out_df.columns]
    remaining    = [c for c in out_df.columns if c not in ordered_cols]
    out_df = out_df[ordered_cols + remaining]

    out_df.to_excel(output_path, index=False)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telecom Operator Research Orchestrator (Batch API)")
    parser.add_argument(
        "--input",  "-i",
        required=True,
        help="Path to input Excel file (must have 'Operator name' and 'Country' columns)",
    )
    parser.add_argument(
        "--output", "-o",
        default="telecom_results.xlsx",
        help="Path to output Excel file (default: telecom_results.xlsx)",
    )
    parser.add_argument(
        "--limit", "-n",
        type=int,
        default=None,
        help="Maximum number of operators to process (default: all)",
    )
    args = parser.parse_args()

    run(args.input, args.output, limit=args.limit)
