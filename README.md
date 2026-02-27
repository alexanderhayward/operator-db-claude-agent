# Telecom Operator Research Agent

Automatically researches 100 telecom operators using Claude + web search,
and saves structured results to an Excel file.

---

## Files

| File | Purpose |
|---|---|
| `search_agent.py` | Searches the web for a single operator and returns structured JSON |
| `orchestrator.py` | Loops through your Excel file and runs the search agent for each operator |
| `requirements.txt` | Python dependencies |

---

## Setup

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Set your Anthropic API key

Get your key from https://console.anthropic.com

```bash
# Mac / Linux
export ANTHROPIC_API_KEY=sk-ant-...

# Windows (Command Prompt)
set ANTHROPIC_API_KEY=sk-ant-...

# Windows (PowerShell)
$env:ANTHROPIC_API_KEY="sk-ant-..."
```

### 3. Prepare your Excel file

Your input Excel file must have at least two columns:
- `Operator name` — the name of the telecom operator
- `Country` — the country where the operator is based

Column names are case-insensitive. Variations like "operator", "name", "company" are also detected automatically.

---

## Usage

### Run on your full list of operators

```bash
python orchestrator.py --input operators.xlsx --output telecom_results.xlsx
```

### Test on a single operator first

```bash
python search_agent.py
```
(Edit the test at the bottom of `search_agent.py` to use your operator)

---

## Output columns

The output Excel file will have one row per operator with these columns:

| Column | Description |
|---|---|
| `operator_name` | Operator name |
| `country` | Country |
| `data_year` | Year of the financial data found |
| `total_revenue` | Total revenue (with currency) |
| `mobile_revenue` | Mobile segment revenue |
| `fixed_revenue` | Fixed segment revenue |
| `ebitda` | EBITDA value |
| `ebitda_margin` | EBITDA margin % |
| `mobile_subscribers` | Mobile subscriber count |
| `fixed_broadband_subscribers` | Fixed broadband subscriber count |
| `total_subscribers` | Total subscribers across all services |
| `service_types` | Services offered (Mobile \| Fixed \| Satellite \| Wholesale) |
| `has_flanker_brand` | True / False |
| `flanker_brand_names` | Flanker brand name(s) |
| `has_mvno` | True / False |
| `mvno_names` | MVNO name(s) |
| `recent_news` | 2-3 recent news items |
| `sources` | URLs used as sources |
| `error` | Error message if search failed |

---

## Resume interrupted runs

If the script is interrupted, just run the same command again.
It will automatically skip operators that already have results in the output file
and continue from where it left off.

---

## Cost estimate

- Each operator search uses ~2,000–4,000 tokens (input + output + web search)
- For 100 operators: roughly **$0.50–$2.00 total** depending on search complexity
- Model used: `claude-sonnet-4-20250514` ($3/$15 per million tokens in/out)

---

## Troubleshooting

**"Could not find a column matching..."**
→ Check that your Excel file has columns named `Operator name` and `Country`.

**"ANTHROPIC_API_KEY not set"**
→ Make sure you've exported the environment variable (see Setup step 2).

**Some fields return null**
→ The web search couldn't find that data. Try running just that operator manually
   with `search_agent.py` to debug.
