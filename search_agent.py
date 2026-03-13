"""
search_agent.py

Given an operator name and country, uses Claude with web search
to find structured information about the telecom operator.

Exposes:
  - search_operator()       — standalone single-operator call (for testing)
  - build_batch_request()   — builds a request dict for the Batch API
  - parse_response()        — parses Claude's content blocks into a result dict
"""

import anthropic
import json


client = anthropic.Anthropic()

MODEL      = "claude-sonnet-4-20250514"
MAX_TOKENS = 2000

SYSTEM_PROMPT = """You are a telecom industry research analyst.
Your job is to search the web and return accurate, structured data about telecom operators.
Always search for the most recent available data.
Be factual and concise. If information is not found, use null.
Always return valid JSON only — no prose, no markdown, no explanation."""

def build_search_prompt(operator_name: str, country: str) -> str:
    return f"""Research the telecom operator "{operator_name}" based in {country}.

Search the web and find the following information:

1. FINANCIAL DATA (latest available year):
   - Total revenue, Mobile revenue, Fixed revenue, EBITDA
   - For each, provide TWO values:
       (a) the original local-currency value as reported (e.g. "EUR 3.9B")
       (b) the USD equivalent, converted using the average exchange rate for the data year (e.g. "USD 4.2B")
   - EBITDA margin as a percentage
   - Mobile subscribers (total), Fixed broadband subscribers, Total subscribers

2. SERVICES:
   - Types of service offered: Mobile, Fixed, Satellite, Wholesale (list all that apply)

3. RECENT NEWS:
   - 2-3 bullet points summarizing the most recent notable news or press releases

4. INTERNATIONAL GROUP:
   - Is this operator part of an international telecom group (e.g. Vodafone Group, Orange Group, Telefonica, Deutsche Telekom, etc.)?
   - If yes, what is the name of the international group?

5. FLANKER BRAND:
   - Has the operator launched a flanker brand? (yes/no)
   - If yes, list the flanker brand name(s)

6. MVNO:
   - Has the operator launched or hosted an MVNO? (yes/no)
   - If yes, list the MVNO name(s)

Return ONLY a JSON object in exactly this format:
{{
  "operator_name": "{operator_name}",
  "country": "{country}",
  "international_group": "<name of international group or null if independent>",
  "data_year": "<year of financial data or null>",
  "total_revenue": "<USD value only, e.g. 'USD 4.2B' or null>",
  "total_revenue_local": "<local currency value, e.g. 'EUR 3.9B' or null — same as total_revenue if already USD>",
  "mobile_revenue": "<USD value only or null>",
  "mobile_revenue_local": "<local currency value or null>",
  "fixed_revenue": "<USD value only or null>",
  "fixed_revenue_local": "<local currency value or null>",
  "ebitda": "<USD value only or null>",
  "ebitda_local": "<local currency value or null>",
  "ebitda_margin": "<percentage or null>",
  "mobile_subscribers": "<value or null>",
  "fixed_broadband_subscribers": "<value or null>",
  "total_subscribers": "<value or null>",
  "service_types": ["Mobile", "Fixed"],
  "recent_news": ["news item 1", "news item 2"],
  "has_flanker_brand": true or false or null,
  "flanker_brand_names": ["brand1"] or [],
  "has_mvno": true or false or null,
  "mvno_names": ["mvno1"] or [],
  "sources": ["url1", "url2"]
}}"""


# ---------------------------------------------------------------------------
# Batch API helpers
# ---------------------------------------------------------------------------

def build_batch_request(operator_name: str, country: str, custom_id: str) -> dict:
    """
    Build a single request dict suitable for the Messages Batch API.
    Pass the returned dict as an element of the `requests` list in
    client.messages.batches.create().
    """
    return {
        "custom_id": custom_id,
        "params": {
            "model": MODEL,
            "max_tokens": MAX_TOKENS,
            "system": SYSTEM_PROMPT,
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
            "messages": [
                {"role": "user", "content": build_search_prompt(operator_name, country)}
            ],
        },
    }


def parse_response(content_blocks: list, operator_name: str, country: str) -> dict:
    """
    Parse Claude's response content blocks (from either a direct call or a
    batch result) into a structured result dict.
    Returns _empty_result on JSON parse failure.
    """
    result_text = ""
    for block in content_blocks:
        if block.type == "text":
            result_text += block.text

    result_text = result_text.strip()
    if result_text.startswith("```"):
        result_text = result_text.split("```")[1]
        if result_text.startswith("json"):
            result_text = result_text[4:]
    result_text = result_text.strip()

    try:
        return json.loads(result_text)
    except json.JSONDecodeError as e:
        return _empty_result(operator_name, country, error=f"JSON parse error: {e}")


# ---------------------------------------------------------------------------
# Standalone single-operator call (used for testing / CLI)
# ---------------------------------------------------------------------------

def search_operator(operator_name: str, country: str) -> dict:
    """
    Run the search agent for a single operator via a direct API call.
    Returns a dict with all researched fields.
    """
    print(f"  🔍 Searching: {operator_name} ({country})")

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": build_search_prompt(operator_name, country)}
            ],
        )
        data = parse_response(response.content, operator_name, country)
        usage = response.usage
        print(f"  ✅ Done: {operator_name}  ({usage.input_tokens:,} in / {usage.output_tokens:,} out tokens)")
        return data

    except Exception as e:
        print(f"  ❌ Error for {operator_name}: {e}")
        return _empty_result(operator_name, country, error=str(e))


def _empty_result(operator_name: str, country: str, error: str = None) -> dict:
    return {
        "operator_name": operator_name,
        "country": country,
        "international_group": None,
        "data_year": None,
        "total_revenue": None,
        "mobile_revenue": None,
        "fixed_revenue": None,
        "ebitda": None,
        "ebitda_margin": None,
        "mobile_subscribers": None,
        "fixed_broadband_subscribers": None,
        "total_subscribers": None,
        "service_types": [],
        "recent_news": [],
        "has_flanker_brand": None,
        "flanker_brand_names": [],
        "has_mvno": None,
        "mvno_names": [],
        "sources": [],
        "error": error,
    }


if __name__ == "__main__":
    # Quick test with a single operator
    result = search_operator("Vodafone", "United Kingdom")
    print(json.dumps(result, indent=2))
