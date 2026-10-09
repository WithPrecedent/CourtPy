# Recipes

Short answers to common jobs. See the [tutorial](tutorial.md) for the basics
and the [advanced user guide](advanced.md) for every setting.

## Study only criminal appeals

`outcome_criminal_defendant_won` is missing for civil cases, so keep only the
criminal cases (and those whose winner is known) before using it as a label.
This study loads a table of cases saved earlier (with `courtpy parse` or
`courtpy.save_cases`):

```ini
[general]
seed = 43
label = outcome_criminal_defendant_won

[criminal_project]
criminal_workers = wrangler, analyst, critic

[wrangler]
techniques = load_cases, filter_rows, drop_missing, drop_text

[load_cases_parameters]
source = cases.csv

[filter_rows_parameters]
query = type_criminal

[drop_missing_parameters]
columns = outcome_criminal_defendant_won

[analyst]
techniques = stratified, logit

[critic]
techniques = scorecard
```

Columns that record the outcome itself (such as `outcome_reversal`,
`disposition_reverse`, and `opinion_reverse`) would give the answer away, so
drop them (with `drop_columns`) or name the features to keep (with
`keep_columns`), as in the example study.

## Add your own variables

Write the rules in a CSV file (see [Writing Rules](rules.md)) and parse with
the federal rules and yours:

```text
target,variable,kind,pattern,value,ignorecase,dotall,note
opinion,cites_chevron,flag,Chevron(?: U\.S\.A\.)?(?:,)? Inc\. v\. Natural Res,,TRUE,FALSE,Chevron deference
opinion,cites_loper_bright,flag,Loper Bright,,TRUE,FALSE,
opinion,chevron_mentions,count,\bChevron\b,,FALSE,FALSE,
```

```python
table = courtpy.parse("court_listener", rulebooks = ["federal", "deference.csv"])
```

or, in a settings file, `rulebooks = federal, deference.csv` in the loader's
parameters (such as the "load_court_listener_parameters" section). Check the
file first with `courtpy rules deference.csv`.

For a quick look without a file of rules, keep the opinions' text and search
it with `amos`'s mungers, such as `flag_patterns` and `count_patterns`, after
the loader. Their "patterns" are a mapping, so the settings must be in a toml,
json, or yaml file (or a Python `dict`), not an ini file. `drop_text` then
removes the text before the analysis:

```toml
[wrangler]
techniques = "load_court_listener, flag_patterns, count_patterns, drop_text"

[load_court_listener_parameters]
keep_text = true

[flag_patterns_parameters]
column = "opinion"
ignorecase = true

[flag_patterns_parameters.patterns]
cites_loper_bright = 'Loper Bright'

[count_patterns_parameters]
column = "opinion"

[count_patterns_parameters.patterns]
chevron_mentions = '\bChevron\b'
```

## Keep a set of recent cases up to date

A download with the API saves its place, and running it again saves only the
cases added since. Run the same command each week (for example, with Windows
Task Scheduler or cron):

```sh
courtpy download api --courts federal_circuits --start 2026-01-01 --folder court_listener
```

## Parse cases downloaded from Lexis-Nexis

```sh
courtpy split downloads --folder lexis_nexis
courtpy parse lexis_nexis --source lexis_nexis --output lexis_cases.csv
```

The second command also codes the parties, case types, and outcomes (add
`--no-code` to skip that).

## Look at what the rules found in one case

```python
import courtpy

case = courtpy.courtlistener.read_case("court_listener/ca1/4567890.json")
print(case.sections["judges"])
parser = courtpy.Parser.create("federal", keep_text = True)
row = parser.parse(case)
print(row["panel_judges"], row["disposition"], row["disposition_reverse"])
```

## Use the table in Stata or R

The CSV file that `save_cases` writes can be read by Stata
(`import delimited cases.csv`), R (`read.csv("cases.csv")`), and most other
programs. Lists (such as the judges) are written as text, with "; " between
the items.
