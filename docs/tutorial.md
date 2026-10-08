# Tutorial

This tutorial collects a small set of federal appellate cases from
CourtListener, turns them into a table, and analyzes it. Each step can be run
on its own, and each saves its work.

## The vocabulary

| Part | What it is | In code |
| --- | --- | --- |
| Case | One decision: its sections of text (such as the parties, the judges, and the opinions) and information that needs no parsing (such as the date filed). | `courtpy.Case` |
| Rule | One row of a CSV file that finds something in a section of text. | `courtpy.Rule` |
| Rulebook | The rules of one or more CSV files, applied in order. | `courtpy.Rulebook` |
| Case table | A `pandas.DataFrame` with one row for each case. | `courtpy.parse` |
| Coder | A technique that adds variables to the case table, such as whether a decision reversed the court below. | `courtpy.coders` |
| Project | A study: the cases and an [amos](https://WithPrecedent.github.io/amos) analysis of them. | `courtpy.Project` |

## 1. Collect cases

CourtListener offers its opinions in two ways. The **bulk data** needs no API
key and has no limits, but its files are large (the opinions alone are more
than 50 GB), so extracting cases takes a few hours. It is the best way to
collect a large set of cases:

```python
import courtpy

courtpy.BulkData().extract(
    courts = "ca1",
    start_date = "2019-01-01",
    end_date = "2019-12-31",
    folder = "court_listener")
```

The **API** is faster for a small or very recent set of cases, but it needs an
API key and limits how many requests an account can make. Get a key from the
"API" page of your CourtListener profile and store it once, in a terminal:

```sh
courtpy key set
```

Then download:

```python
courtpy.CourtListener().download(
    "ca1", "2026-09-01", "2026-09-30", "court_listener", max_cases = 40)
```

Either way, each case is saved as a JSON file in a folder for its court, such
as `court_listener/ca1/4567890.json`. Running a download again skips the cases
that were already saved.

## 2. Parse the cases into a table

`courtpy.parse` reads every saved case in a folder and applies the rules in
the "federal" rulebook (see [Writing Rules](rules.md)):

```python
table = courtpy.parse("court_listener")
print(table.shape)
print(table[["case_name", "court_num", "panel_judges", "disposition"]].head())
```

Each row is labeled by the case's CourtListener id. Among the columns are the
parties (`party1` and `party2`) and their roles (`party1_appellant`,
`party2_united_states`, and so on), the judges (`panel_judges`, `author`,
`dissenting`), the disposition (`disposition_reverse` and others), the issues
the opinions discuss (`criminal_firearm`, `civil_titlevii`,
`general_amend4`, and many more), and the citations in the opinions
(`references_case` and others).

## 3. Code the outcomes

The coders add variables that combine others: whether each case is criminal,
each party's role in the case, and the outcome of the appeal:

```python
dataset = courtpy.code(table)
coded = dataset.data
print(coded["outcome_reversal"].mean())
print(coded[["type_criminal", "outcome_criminal_defendant_won"]].value_counts())
```

`courtpy.code` returns an `amos.Dataset`, whose `history` records each coder.
Save the table so that the steps above need not be repeated:

```python
courtpy.save_cases(coded, "cases.csv")
coded = courtpy.load_cases("cases.csv")
```

The CSV file can be opened in a spreadsheet. Lists (such as the judges) are
written with "; " between the items, and `load_cases` turns them back into
lists.

## 4. Analyze the table

`courtpy.Project` is `amos.Project`, so an analysis is described with `amos`
settings. This one predicts reversals from a few features. (`sk_logit` is
scikit-learn's logistic regression; amos's `logit` is the statsmodels model,
with a table of coefficients, which needs `pip install amos[statistics]`.)

```python
settings = {
    "general": {"seed": 43, "label": "outcome_reversal"},
    "study_project": {"study_workers": "wrangler, explorer, analyst, critic"},
    "wrangler": {"techniques": "keep_columns, drop_missing"},
    "keep_columns_parameters": {
        "columns": ["year", "published", "type_criminal", "dissents",
                    "word_count", "criminal_firearm", "standard_de_novo"]},
    "explorer": {"techniques": "summarize, label_balance"},
    "analyst": {
        "design": "experiment",
        "criterion": "roc_auc",
        "steps": "split, model",
        "split_techniques": "stratified",
        "model_techniques": "baseline, sk_logit"},
    "critic": {"techniques": "scorecard"},
}
project = courtpy.Project.create(settings, item = coded)
print(project.result.tables["scorecard"])
project.export()
```

`export` saves the report, the settings, the versions of every package, the
history of every technique, and the tables and figures in a folder named for
the run, so the study can be reported and reproduced.

## 5. Describe the whole study in one file

Steps 1 to 3 can be part of the study itself. CourtPy's loaders are `amos`
techniques that collect, parse, and code cases, so a project whose wrangler
starts with one needs no other data. Save this as `study.ini`:

```ini
[general]
seed = 43
label = outcome_reversal

[study_project]
study_workers = wrangler, analyst, critic

[wrangler]
techniques = load_court_listener, keep_columns, drop_missing

[load_court_listener_parameters]
source = court_listener
download = api
courts = ca1
start_date = 2026-09-01
end_date = 2026-09-30
max_cases = 40
save = cases.csv
reuse = true

[keep_columns_parameters]
columns = year, published, type_criminal, dissents, word_count

[analyst]
techniques = stratified, sk_logit

[critic]
techniques = scorecard
```

and run it in a terminal:

```sh
courtpy run study.ini --export
```

The first run downloads, parses, and codes the cases and saves the table in
`cases.csv`. Later runs reuse that file (because of `reuse = true`) and only
repeat the analysis. The project's history records the loader and each
coder, so the export shows exactly how the data was made. See [examples/federal_appeals.ini](https://github.com/WithPrecedent/CourtPy/blob/main/examples/federal_appeals.ini)
for a fuller study.
