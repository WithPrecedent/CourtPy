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

## Study how judges vote

To study judges rather than cases, first make a roster of judges, once (see
[Judges and panels](advanced.md#judges-and-panels)):

```python
import courtpy

courtpy.judges.Roster.from_fjc().save()
```

Then add `code_judges` and `judge_votes` to the loader's coders. `judge_votes`
is a shaper, which changes what a row is: the table has one row for each
judge on each case, and `vote_reversal` (whether the judge voted to reverse)
can be the label. This is the heart of
[examples/judge_votes.ini](https://github.com/WithPrecedent/courtpy/blob/main/examples/judge_votes.ini):

```ini
[general]
seed = 43
label = vote_reversal
groups = case_id

[votes_project]
votes_workers = wrangler, analyst, critic

[wrangler]
techniques = load_court_listener, filter_rows, keep_columns, drop_missing

[load_court_listener_parameters]
source = court_listener
coders = code_parties, code_case_type, code_outcome, code_judges, code_politics, judge_votes

[filter_rows_parameters]
query = type_criminal and panel_size == 3 and panel_found == 3

[keep_columns_parameters]
columns = court_num, year, politics_president_party, judge_party, judge_woman, judge_age, colleagues_party, colleagues_woman

[analyst]
techniques = group_split, logit

[critic]
techniques = scorecard

[scorecard_parameters]
metrics = roc_auc, accuracy, balanced_accuracy, f1
```

* The query keeps criminal appeals decided by three judges (so not by a court
  sitting en banc) who were all found in the roster. Compare `panel_found`
  with `panel_size` to see how many panels have a judge who was not found.
* The judges of a case decide it together, so `groups = case_id` and
  `group_split` keep them together when the data is split, and a model is
  tested on cases that it has not seen.
* A scorecard compares a model across a study's groups unless its metrics
  are named. Here the groups are cases, so that comparison would mean nothing
  (and it needs the optional fairlearn package).

To study panels instead, use `code_judges` without `judge_votes`. The table
keeps one row for each case, the label can stay `outcome_reversal`, and the
composition of each panel (such as `panel_party` and `panel_woman`) can be
among the features.

## Add what is known about each opinion's author

`merge_judges` is a merger: it adds the columns of the roster's row for the
judge that each row names, and never adds or loses a row. To study whether
the author of an opinion matters, name the column with the author's name and
a prefix for the new columns:

```ini
[wrangler]
techniques = load_court_listener, merge_judges, filter_rows, keep_columns

[merge_judges_parameters]
name = author
prefix = author_
indicator = author_found

[filter_rows_parameters]
query = type_criminal and author_found

[keep_columns_parameters]
columns = outcome_reversal, court_num, year, author_party, author_woman, author_age, author_prosecutor
```

The history of the study's result records how many cases' authors were
found (its "matched"), which belongs in a description of the data.

## Add the politics of each year

`code_politics` adds the party of the president in each case's year. For the
Supreme Court's Martin-Quinn scores and the median NOMINATE scores of the
Senate and House too, make a table of years once (CourtPy downloads the
scores from their sources), and name it as the technique's `source`:

```python
import courtpy

courtpy.politics.build_table().to_csv("data/politics.csv", index = False)
```

```ini
[files]
input_folder = data

[wrangler]
techniques = load_court_listener, code_politics, drop_text

[code_politics_parameters]
source = politics.csv
```

The table is a CSV file with a "year" column, so measures of your own can be
added to it in a spreadsheet. See
[Political context](advanced.md#political-context).

## Use the Supreme Court Database's coding

For cases of the Supreme Court, `code_scdb` adds the
[Supreme Court Database](https://scdb.la.psu.edu)'s coding of each case, with
"scdb_" before the database's names for its variables. CourtPy downloads the
latest release the first time:

```ini
[general]
label = outcome_reversal

[wrangler]
techniques = load_court_listener, code_scdb, filter_rows, keep_columns

[load_court_listener_parameters]
source = court_listener
download = bulk
courts = scotus
start_date = 2000-01-01

[code_scdb_parameters]
columns = issueArea, decisionDirection, lcDispositionDirection
indicator = in_scdb

[filter_rows_parameters]
query = in_scdb

[keep_columns_parameters]
columns = outcome_reversal, year, scdb_issueArea, scdb_lcDispositionDirection
```

See [The Supreme Court Database](advanced.md#the-supreme-court-database).

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
