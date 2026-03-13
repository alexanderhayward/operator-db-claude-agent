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
from datetime import datetime, timezone
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
    for key in ["has_flanker_brand", "has_mvno"]:
        val = flat.get(key)
        if val is True:
            flat[key] = "Yes"
        elif val is False:
            flat[key] = "No"
    return flat


# ---------------------------------------------------------------------------
# Batch state persistence (for resume across interrupted runs)
# ---------------------------------------------------------------------------

def _state_path(output_path: str) -> str:
    p = Path(output_path)
    return str(p.parent / (p.stem + ".batch_state.json"))

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
        existing_df = pd.read_excel(output_path)

        # Rows with a non-empty error value will be retried
        has_error = existing_df.get("error", pd.Series(dtype=str)).notna() & \
                    (existing_df.get("error", pd.Series(dtype=str)).astype(str).str.strip() != "")

        success_df = existing_df[~has_error]
        retry_df   = existing_df[has_error]

        existing_results = success_df.to_dict(orient="records")
        completed_names  = set(success_df["operator_name"].str.lower().tolist())

        print(f"📋 Found existing output file:")
        print(f"   {len(success_df)} completed (will skip)")
        if len(retry_df) > 0:
            print(f"   {len(retry_df)} with errors (will retry): "
                  f"{', '.join(retry_df['operator_name'].tolist())}")
        print()

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
        if len(pending) < limit:
            print(f"   --limit {limit} exceeds available operators; processing all {len(pending)}\n")
        else:
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

        raw      = client.messages.batches.with_raw_response.create(requests=requests)
        batch    = raw.parse()
        batch_id = batch.id

        _save_state(output_path, {
            "batch_id":        batch_id,
            "id_to_operator":  id_to_operator,
        })

        tokens_limit     = raw.headers.get("anthropic-ratelimit-tokens-limit", "?")
        tokens_remaining = raw.headers.get("anthropic-ratelimit-tokens-remaining", "?")
        tokens_reset     = raw.headers.get("anthropic-ratelimit-tokens-reset", "?")

        print(f"   Batch ID        : {batch_id}")
        print(f"   Operators       : {len(pending)}")
        print(f"   Token limit     : {int(tokens_limit):,}" if tokens_limit != "?" else f"   Token limit     : ?")
        print(f"   Tokens remaining: {int(tokens_remaining):,}" if tokens_remaining != "?" else f"   Tokens remaining: ?")
        print(f"   Limit resets at : {tokens_reset}")
        print(f"   Polling every {POLL_INTERVAL}s until complete...\n")

    # Poll until the batch finishes — only print when counts change
    poll_start   = time.time()
    last_counts  = None

    while True:
        raw    = client.messages.batches.with_raw_response.retrieve(batch_id)
        batch  = raw.parse()
        counts = batch.request_counts
        snapshot = (counts.succeeded, counts.errored, counts.processing, counts.canceled)

        if snapshot != last_counts:
            elapsed     = int(time.time() - poll_start)
            elapsed_str = f"{elapsed // 60}m {elapsed % 60:02d}s"

            tokens_used      = raw.headers.get("anthropic-ratelimit-tokens-used", "?")
            tokens_remaining = raw.headers.get("anthropic-ratelimit-tokens-remaining", "?")
            tokens_reset     = raw.headers.get("anthropic-ratelimit-tokens-reset", "?")
            tok_used_str      = f"{int(tokens_used):,}"      if tokens_used      != "?" else "?"
            tok_remaining_str = f"{int(tokens_remaining):,}" if tokens_remaining != "?" else "?"

            print(
                f"   [{elapsed_str}]  succeeded={counts.succeeded}  "
                f"errored={counts.errored}  processing={counts.processing}  "
                f"canceled={counts.canceled}  |  "
                f"tokens used={tok_used_str}  remaining={tok_remaining_str}"
            )

            # Warn clearly if the token quota is exhausted
            if tokens_remaining not in ("?", None) and int(tokens_remaining) == 0:
                wait_str = "?"
                if tokens_reset not in ("?", None):
                    try:
                        reset_dt  = datetime.fromisoformat(tokens_reset.replace("Z", "+00:00"))
                        wait_secs = max(0, int((reset_dt - datetime.now(timezone.utc)).total_seconds()))
                        wait_str  = f"{wait_secs // 60}m {wait_secs % 60:02d}s"
                    except ValueError:
                        wait_str = tokens_reset
                print(f"   ⚠️  Token rate limit reached — quota resets in {wait_str} (at {tokens_reset})")

            last_counts = snapshot

        if batch.processing_status == "ended":
            break
        time.sleep(POLL_INTERVAL)

    # Process results
    print(f"\n📊 Processing results...\n")
    results = list(existing_results)
    total_input_tokens  = 0
    total_output_tokens = 0

    for result in client.messages.batches.results(batch_id):
        custom_id       = result.custom_id
        name, country   = id_to_operator.get(custom_id, [custom_id, "Unknown"])

        if result.result.type == "succeeded":
            usage = result.result.message.usage
            total_input_tokens  += usage.input_tokens
            total_output_tokens += usage.output_tokens
            data = parse_response(result.result.message.content, name, country)
            results.append(flatten_result(data))
            print(f"  ✅ {name}  ({usage.input_tokens:,} in / {usage.output_tokens:,} out tokens)")
        elif result.result.type == "errored":
            error_info = result.result.error
            error_type = getattr(error_info, "type",    "unknown_error")
            error_msg  = getattr(error_info, "message", str(error_info))
            if error_type == "rate_limit_error":
                print(f"  🚫 {name}: RATE LIMIT — {error_msg}")
            elif error_type == "overloaded_error":
                print(f"  🚫 {name}: API OVERLOADED — {error_msg}")
            else:
                print(f"  ❌ {name}: [{error_type}] {error_msg}")
            results.append(flatten_result(_empty_result(name, country, error=f"{error_type}: {error_msg}")))

        elif result.result.type == "canceled":
            print(f"  ⛔ {name}: canceled")
            results.append(flatten_result(_empty_result(name, country, error="canceled")))

        elif result.result.type == "expired":
            print(f"  ⏰ {name}: expired (batch was not processed within 24h)")
            results.append(flatten_result(_empty_result(name, country, error="expired")))

        else:
            print(f"  ❓ {name}: unknown result type '{result.result.type}'")
            results.append(flatten_result(_empty_result(name, country, error=f"unknown result type: {result.result.type}")))

    _save_results(results, output_path)
    _clear_state(output_path)

    total_tokens = total_input_tokens + total_output_tokens
    print(f"\n✅ All done! Results saved to: {output_path}")
    print(f"   Total operators processed: {len(results)}")
    print(f"   Total tokens used: {total_tokens:,}  ({total_input_tokens:,} input / {total_output_tokens:,} output)")


def _save_results(results: list[dict], output_path: str):
    """Save current results list to Excel."""
    out_df = pd.DataFrame(results)

    # Friendly column order
    preferred_order = [
        "operator_name", "country", "international_group", "data_year",
        "total_revenue", "total_revenue_local",
        "mobile_revenue", "mobile_revenue_local",
        "fixed_revenue", "fixed_revenue_local",
        "ebitda", "ebitda_local", "ebitda_margin",
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
# Cancel helper
# ---------------------------------------------------------------------------

def cancel(output_path: str):
    state    = _load_state(output_path)
    batch_id = state.get("batch_id")

    if not batch_id:
        print("❌ No in-progress batch found. Is the state file missing?")
        print(f"   (looked for: {_state_path(output_path)})")
        return

    print(f"⛔ Canceling batch: {batch_id} ...")
    batch = client.messages.batches.cancel(batch_id)
    print(f"   Status: {batch.processing_status}")
    print(f"   Note: requests already processing may still complete and be billed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telecom Operator Research Orchestrator (Batch API)")
    parser.add_argument(
        "--input",  "-i",
        required=False,
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
    parser.add_argument(
        "--cancel",
        action="store_true",
        help="Cancel the in-progress batch for the given --output file",
    )
    args = parser.parse_args()

    if args.cancel:
        cancel(args.output)
    else:
        if not args.input:
            parser.error("--input is required unless --cancel is specified")
        run(args.input, args.output, limit=args.limit)
