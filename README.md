# courtpy

| | |
| --- | --- |
| Version | [![PyPI Latest Release](https://img.shields.io/pypi/v/courtpy.svg?style=for-the-badge&color=cornflowerblue&label=PyPI&logo=PyPI&logoColor=yellow)](https://pypi.org/project/courtpy/) [![GitHub Latest Release](https://img.shields.io/github/v/tag/WithPrecedent/courtpy?style=for-the-badge&color=forestgreen&label=GitHub&logo=github)](https://github.com/WithPrecedent/courtpy/releases) |
| Status | [![Build Status](https://img.shields.io/github/actions/workflow/status/WithPrecedent/courtpy/ci.yml?branch=main&style=for-the-badge&color=cadetblue&label=Tests&logo=pytest)](https://github.com/WithPrecedent/courtpy/actions/workflows/ci.yml?query=branch%3Amain) [![Development Status](https://img.shields.io/badge/Development-Active-seagreen?style=for-the-badge&logo=git)](https://www.repostatus.org/#active) [![Project Stability](https://img.shields.io/pypi/status/courtpy?style=for-the-badge&logo=pypi&label=Stability&logoColor=yellow)](https://pypi.org/project/courtpy/) |
| Documentation | [![Hosted By](https://img.shields.io/badge/Hosted_by-Github_Pages-blue?style=for-the-badge&color=forestgreen&logo=github)](https://WithPrecedent.github.io/courtpy) |
| Tools | [![Documentation](https://img.shields.io/badge/MkDocs-magenta?style=for-the-badge&color=deepskyblue&logo=markdown&labelColor=gray)](https://squidfunk.github.io/mkdocs-material/) [![Linter](https://img.shields.io/endpoint?style=for-the-badge&url=https://raw.githubusercontent.com/charliermarsh/Ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/Ruff) [![Dependency Manager](https://img.shields.io/badge/uv-mediumpurple?style=for-the-badge&logo=uv&labelColor=gray)](https://docs.astral.sh/uv/) [![Pre-commit](https://img.shields.io/badge/pre--commit-darkolivegreen?style=for-the-badge&logo=pre-commit&logoColor=white&labelColor=gray)](https://github.com/TezRomacH/python-package-template/blob/master/.pre-commit-config.yaml) [![CI](https://img.shields.io/badge/GitHub_Actions-forestgreen?style=for-the-badge&logo=githubactions&labelColor=gray&logoColor=white)](https://github.com/features/actions) [![Editor Settings](https://img.shields.io/badge/Editor_Config-paleturquoise?style=for-the-badge&logo=editorconfig&labelColor=gray)](https://editorconfig.org/) [![Repository Template](https://img.shields.io/badge/snickerdoodle-bisque?style=for-the-badge&logo=cookiecutter&labelColor=gray)](https://www.github.com/WithPrecedent/snickerdoodle) [![Dependency Maintainer](https://img.shields.io/badge/dependabot-forestgreen?style=for-the-badge&logo=dependabot&logoColor=white&labelColor=gray)](https://github.com/dependabot) |
| Compatibility | [![Compatible Python Versions](https://img.shields.io/pypi/pyversions/courtpy?style=for-the-badge&color=cornflowerblue&label=Python&logo=python&logoColor=yellow)](https://pypi.python.org/pypi/courtpy/) [![Linux](https://img.shields.io/badge/Linux-lightseagreen?style=for-the-badge&logo=linux&labelColor=gray&logoColor=white)](https://www.linux.org/) [![MacOS](https://img.shields.io/badge/MacOS-antiquewhite?style=for-the-badge&logo=apple&labelColor=gray)](https://www.apple.com/macos/)  [![Windows](https://img.shields.io/badge/Windows-blue?style=for-the-badge)](https://www.microsoft.com/en-us/windows?r=1) |
| Stats | [![PyPI Download Rate (per month)](https://img.shields.io/pypi/dm/courtpy?style=for-the-badge&color=cornflowerblue&label=Downloads%20💾&logo=pypi&logoColor=yellow)](https://pypi.org/project/courtpy) [![GitHub Stars](https://img.shields.io/github/stars/WithPrecedent/courtpy?style=for-the-badge&color=forestgreen&label=Stars%20⭐&logo=github)](https://github.com/WithPrecedent/courtpy/stargazers) [![GitHub Contributors](https://img.shields.io/github/contributors/WithPrecedent/courtpy?style=for-the-badge&color=forestgreen&label=Contributors%20🙋&logo=github)](https://github.com/WithPrecedent/courtpy/graphs/contributors) [![GitHub Issues](https://img.shields.io/github/issues/WithPrecedent/courtpy?style=for-the-badge&color=forestgreen&label=Issues%20📘&logo=github)](https://github.com/WithPrecedent/courtpy/graphs/contributors) [![GitHub Forks](https://img.shields.io/github/forks/WithPrecedent/courtpy?style=for-the-badge&color=forestgreen&label=Forks%20🍴&logo=github)](https://github.com/WithPrecedent/courtpy/forks) |
| | |

-----

## What is courtpy?

`courtpy` collects court opinions, turns them into data, and analyzes them. It
is designed for empirical legal research, where the data has to be explained
and the results reproduced. You don't need to know how to code to use `courtpy`.
Everything is described in a simple settings files and parsing instructions are
located in CSV files (of which many are included out of the box). But if you do
code, `courtpy` is highly extensible to allow easy data munging, creation of
new fields, and wrapping custom tools.

* **Collect** opinions from [CourtListener](https://www.courtlistener.com),
  from its free bulk data or its API, or use opinions you downloaded from
  Lexis-Nexis.
* **Parse** them into a table, one row for each case, with rules written in
  spreadsheets (CSV files) that anyone can read and change: the parties and
  their roles, the court, the judges, the disposition, the issues discussed,
  the citations, and more.
* **Study** the judges on each case, with integration of the Federal Judicial Center's
  biographical data and Judicial Common Space Scores. Data about the overall
  political and judicial context can also be easily added, including Martin-Quinn scores, NOMINATE scores of Congress, the Supreme Court Database, and the Federal Judicial Center's Integrated Database of appeals.
* **Analyze** the data with [amos](https://github.com/WithPrecedent/amos) (which is built into `courtpy`),
  which compares models, preprocessing, and statistical methods, and records
  everything needed to reproduce the results.

## Why use courtpy?

* **Transparent.** What `courtpy` finds in an opinion comes from rules in CSV
  files, not from code, so a study's methods can be read, reported, and
  changed by anyone who can use a spreadsheet. The rules are part of the
  study, so others can check them.
* **Reproducible.** A whole study (which cases, how they were parsed and coded,
  and how they were analyzed) fits in one settings file, and `amos` records
  every step and the version of every package.

## Getting started

### Requirements

`courtpy` requires Python 3.11 or later. It runs on Linux, macOS, and Windows.

Extracting cases from CourtListener's bulk data needs a good connection and
room for the bulk files (about 63 GB for the opinions, clusters, and dockets),
unless they are streamed. Downloading with the API needs a free CourtListener
account.

### Installation

To install `courtpy`, use `pip`:

```sh
pip install courtpy
```

To install the latest development version from GitHub instead:

```sh
pip install git+https://github.com/WithPrecedent/courtpy
```

To work on `courtpy` itself, see the
[contribution guide](https://github.com/WithPrecedent/courtpy/blob/main/CONTRIBUTING.md).

### Usage

#### Collecting opinions from CourtListener

CourtListener offers its opinions in two ways, and `courtpy` uses both. Both save
each case as a JSON file in a folder named for its court (such as
`court_listener/ca1/4567890.json`), and the parser reads them the same way.

**The bulk data** is the whole CourtListener database, published as compressed
CSV files every three months. It needs no API key and has no limits, so it is
the best way to collect many cases. The files are large (the opinions file is
more than 50 GB), so extracting cases takes a few hours. The files are kept
outside your project so that they can be used again:

```python
import courtpy

courtpy.BulkData().extract(
    courts = "federal_circuits",
    start_date = "2019-01-01",
    end_date = "2019-12-31",
    folder = "court_listener")
```

**The API** is best for small or very recent sets of cases. It needs an API
key: sign in to CourtListener and open the "API" page of your profile. A free
account can make 5 requests a minute, 50 an hour, and 125 a day (as of 2026).
`courtpy` asks for 20 cases at a time, waits when CourtListener asks it to, and
saves its place, so a download stopped by the daily limit resumes where it
stopped when it is run again:

```python
courtpy.CourtListener().download("ca1", "2026-09-01", "2026-09-30", "court_listener")
```

Courts are named by their CourtListener ids (such as `ca1`, `cadc`, `cafc`,
and `scotus`) or by groups: `federal_appellate`, `federal_circuits`, and
`supreme_court`.

#### Keeping your API key secret

Your API key, if you use one, is never written in a project's files or in `courtpy` itself.
Store it once:

```sh
courtpy key set
```

That asks for the key (without showing it) and stores it in your computer's
keyring (Windows Credential Manager, the macOS Keychain, or the Secret Service
on Linux), or, if there is none, in a private file in your user folder. From
then on, `courtpy` finds it whenever it is needed. `courtpy key show` says where
it was found (with most of it hidden), and `courtpy key delete` removes it.

`courtpy` looks for the key in this order: the `COURTLISTENER_API_KEY`
environment variable, the keyring, the private file, and a `.env` file in the
current folder (with a line such as `COURTLISTENER_API_KEY=your-key`; keep
such a file out of version control). In Python, use
`courtpy.secrets.set_api_key(...)` and `courtpy.secrets.get_api_key()`.

#### Using opinions from Lexis-Nexis

Lexis-Nexis downloads many cases into one text file. Divide such files into
one file for each case, and then parse them as the source "lexis_nexis":

```python
courtpy.lexis.split("downloads", folder = "lexis_nexis")
table = courtpy.parse("lexis_nexis", source = "lexis_nexis")
```

#### Parsing and coding

`courtpy.parse` reads a folder of saved cases and returns a
`pandas.DataFrame`, one row for each case, labeled by its id:

```python
table = courtpy.parse("court_listener")
dataset = courtpy.code(table)  # parties, case types, and outcomes
courtpy.save_cases(dataset.data, "cases.csv")
```

The rules are in the CSV files in `courtpy`'s
[instructions folder](https://github.com/WithPrecedent/courtpy/tree/main/src/courtpy/instructions),
with a guide to writing your own. The "federal" rules make more than 250
variables, including:

| Variables | Meaning |
| --- | --- |
| `party1`, `party2` | The parties on each side of "v." |
| `party1_appellant`, `party2_respondent`, `party1_united_states`, ... | Each party's role, and whether it is the United States. |
| `court_num` | The circuit (1 to 11, 12 for the D.C. Circuit, 13 for the Federal Circuit, 99 for the Supreme Court). |
| `panel_judges`, `panel_size`, `author`, `concurring`, `dissenting` | The judges. |
| `disposition_reverse`, `decision_reverse`, `opinion_reverse`, ... | The disposition: from the header, from the decision that the opinion states ("AFFIRMED."), and from the opinion's words anywhere. |
| `agency` | The federal agency involved, if any. |
| `civil_...`, `criminal_...`, `general_...`, `procedure_...`, `standard_...` | Issues discussed in the opinion (such as `criminal_firearm`, `civil_titlevii`, and `standard_de_novo`). |
| `references_case`, `references_statute`, ... | Citations in the opinion. |
| `published`, `year`, `date_filed`, `word_count` | Other information. |

The coders (which are `amos` techniques) add `type_criminal`, each party's
role in the appeal and in the case (such as `party2_criminal_defendant`), and
the outcomes: `outcome_reversal`, `outcome_party1_won`,
`outcome_criminal_defendant_won`, and others.

#### Adding the judges and the politics of the day

The rules find the judges' names as an opinion writes them ("LYNCH"). To
study the judges, make a roster once from the Federal Judicial Center's
[biographical directory](https://www.fjc.gov/history/judges/biographical-directory-article-iii-federal-judges-export)
(three small files, which `courtpy` downloads), and more coders can use it:

```python
courtpy.judges.Roster.from_fjc().save()
dataset = courtpy.code(table, [
    "code_parties", "code_case_type", "code_outcome",
    "code_judges", "code_politics", "judge_votes"])
```

Cases, judges, years, and other researchers' data are tables whose rows are
different things, so these coders are the mergers and shapers of
[amos](https://WithPrecedent.github.io/amos/advanced/#merging-data): a merger
adds the columns of another table to the rows that it matches, without ever
adding or losing a row, and records how many rows it matched.

| Coder | Adds |
| --- | --- |
| `merge_judges` | What the roster says about the judge that each row names, such as the author of each opinion (`author_party`, `author_woman`, `author_age`, ...). |
| `code_judges` | The judges of each panel who were found in the roster (`panel_names` and `panel_found`) and the panel's composition, such as `panel_party` (the mean party of the presidents who appointed its judges, from -1 for Democrats to 1 for Republicans), `panel_woman`, `panel_age`, and `panel_prosecutor`. |
| `judge_votes` | A table with one row for each judge on each case, with the judge's attributes (`judge_party`, ...), the other judges' (`colleagues_party`, ...), and the judge's vote (`vote_reversal`, the outcome or its opposite for a judge who dissented). |
| `code_politics` | The party of the president in the case's year (`politics_president_party`) and, from a table that `courtpy.politics.build_table` makes, the Supreme Court's [Martin-Quinn scores](https://mqscores.wustl.edu/measures.php) and the median [NOMINATE](https://voteview.com/data) scores of the Senate and House. |
| `code_scdb` | For cases of the Supreme Court, the coding of the [Supreme Court Database](https://scdb.la.psu.edu/data/), such as `scdb_issueArea`, `scdb_decisionDirection`, and `scdb_partyWinning`. |
| `code_idb` | For cases of the courts of appeals, the record of the appeal in the Federal Judicial Center's [Integrated Database](https://www.fjc.gov/research/idb), which [fjc-idb](https://github.com/WithPrecedent/fjc_idb) reads, such as `idb_appeal_type`, `idb_judgment_date`, `idb_pro_se_party`, and `idb_district_judge` (and, if asked, the district court's record of the case appealed). |

`courtpy` downloads the scores and the databases from their sources the first
time they are needed. See
[examples/judge_votes.ini](https://github.com/WithPrecedent/courtpy/blob/main/examples/judge_votes.ini)
for a study of judges' votes.

#### Describing a whole study in one file

A study is an [amos](https://github.com/WithPrecedent/amos) project, described
in one settings file. `courtpy` adds loaders to amos: `load_court_listener`,
`load_lexis_nexis`, and `load_cases`. Named first in the wrangler, a loader
collects the cases (downloading them, if asked), parses them, and codes them,
so the project needs no other data. See
[examples/federal_appeals.ini](https://github.com/WithPrecedent/courtpy/blob/main/examples/federal_appeals.ini):

```ini
[general]
seed = 43
label = outcome_reversal

[files]
input_folder = data
interim_folder = data

[appeals_project]
appeals_workers = wrangler, analyst, critic

[wrangler]
techniques = load_court_listener, filter_rows, keep_columns, drop_missing

[load_court_listener_parameters]
source = court_listener
download = bulk
courts = federal_circuits
start_date = 2019-01-01
end_date = 2019-12-31
save = cases.csv
reuse = true

[analyst]
design = experiment
criterion = roc_auc
steps = split, model
split_techniques = stratified
model_techniques = baseline, logit, random_forest

[critic]
techniques = scorecard
```

The loader finds (and saves) the cases in the input folder ("data") and saves
the coded table in the interim folder. With `reuse = true`, later runs load
that table instead of parsing the cases again.

Run it from a terminal with `courtpy run federal_appeals.ini --export`, or in
Python:

```python
project = courtpy.Project.create("federal_appeals.ini")
print(project.result.tables["scorecard"])
project.export()
```

`courtpy.Project` is `amos.Project`, so everything in the amos documentation
works.

#### Commands

| Command | Does |
| --- | --- |
| `courtpy key set`, `show`, `delete` | Manages your CourtListener API key. |
| `courtpy download bulk --courts ca1 --start 2019-01-01 --end 2019-12-31` | Extracts cases from the bulk data. |
| `courtpy download api --courts ca1 --start 2026-09-01` | Downloads cases with the API. |
| `courtpy split FILES --folder lexis_nexis` | Divides Lexis-Nexis files of many cases. |
| `courtpy parse FOLDER --output cases.csv` | Parses and codes saved cases into a table. |
| `courtpy rules my_rules.csv` | Checks a file of rules and lists the variables it makes. |
| `courtpy run study.ini --export` | Runs a whole study. |

Add `--help` to any command for its options. See the
[documentation](https://WithPrecedent.github.io/courtpy) for a tutorial, the
advanced user guide, a guide to writing rules, and recipes.

## Contributing

Contributors are always welcome. Feel free to grab an [issue](https://www.github.com/WithPrecedent/courtpy/issues) to work on or make a suggested improvement. If you wish to contribute, please read the [Contribution Guide](https://www.github.com/WithPrecedent/courtpy/blob/main/CONTRIBUTING.md) and [Code of Conduct](https://www.github.com/WithPrecedent/courtpy/blob/main/CODE_OF_CONDUCT.md).

## Similar Projects

* [CourtListener](https://www.courtlistener.com) and the Free Law Project's
  tools, including [eyecite](https://github.com/freelawproject/eyecite) (which
  finds and resolves legal citations in text) and
  [juriscraper](https://github.com/freelawproject/juriscraper) (which collects
  opinions from court websites).
* [amos](https://github.com/WithPrecedent/amos), which `courtpy` uses to analyze
  the cases it parses.

## Citation

If you use `courtpy` in your research, please cite it. GitHub's "Cite this
repository" button reads the same information from
[`CITATION.cff`](https://www.github.com/WithPrecedent/courtpy/blob/main/CITATION.cff).

```bibtex
@software{yung_courtpy,
  author  = {Yung, Corey Rayburn},
  title   = {courtpy: Accessible tools for collecting, parsing, and analyzing court opinions},
  version = {0.3.0},
  year    = {2026},
  url     = {https://github.com/WithPrecedent/courtpy},
  license = {Apache-2.0}
}
```

## License

Use of this repository is authorized under the [Apache Software License 2.0](https://www.github.com/WithPrecedent/courtpy/blob/main/LICENSE).
