# CourtPy

| | |
| --- | --- |
| Version | [![PyPI Latest Release](https://img.shields.io/pypi/v/courtpy.svg?style=flat-square&color=cornflowerblue&label=PyPI&logo=PyPI&logoColor=yellow)](https://pypi.org/project/courtpy/) [![GitHub Latest Release](https://img.shields.io/github/v/tag/WithPrecedent/CourtPy?style=flat-square&color=forestgreen&label=GitHub&logo=github)](https://github.com/WithPrecedent/CourtPy/releases) |
| Status | [![Build Status](https://img.shields.io/github/actions/workflow/status/WithPrecedent/CourtPy/ci.yml?branch=main&style=flat-square&color=cadetblue&label=Tests&logo=pytest)](https://github.com/WithPrecedent/CourtPy/actions/workflows/ci.yml?query=branch%3Amain) [![Development Status](https://img.shields.io/badge/Development-Active-seagreen?style=flat-square&logo=git)](https://www.repostatus.org/#active) [![Project Stability](https://img.shields.io/pypi/status/courtpy?style=flat-square&logo=pypi&label=Stability&logoColor=yellow)](https://pypi.org/project/courtpy/) |
| Documentation | [![Hosted By](https://img.shields.io/badge/Hosted_by-Github_Pages-blue?style=flat-square&color=forestgreen&logo=github)](https://WithPrecedent.github.io/CourtPy) |
| Tools | [![Documentation](https://img.shields.io/badge/MkDocs-magenta?style=flat-square&color=deepskyblue&logo=markdown&labelColor=gray)](https://squidfunk.github.io/mkdocs-material/) [![Linter](https://img.shields.io/endpoint?style=flat-square&url=https://raw.githubusercontent.com/charliermarsh/Ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/Ruff) [![Dependency Manager](https://img.shields.io/badge/uv-mediumpurple?style=flat-square&logo=uv&labelColor=gray)](https://docs.astral.sh/uv/) [![Pre-commit](https://img.shields.io/badge/pre--commit-darkolivegreen?style=flat-square&logo=pre-commit&logoColor=white&labelColor=gray)](https://github.com/TezRomacH/python-package-template/blob/master/.pre-commit-config.yaml) [![CI](https://img.shields.io/badge/GitHub_Actions-forestgreen?style=flat-square&logo=githubactions&labelColor=gray&logoColor=white)](https://github.com/features/actions) [![Editor Settings](https://img.shields.io/badge/Editor_Config-paleturquoise?style=flat-square&logo=editorconfig&labelColor=gray)](https://editorconfig.org/) [![Repository Template](https://img.shields.io/badge/snickerdoodle-bisque?style=flat-square&logo=cookiecutter&labelColor=gray)](https://www.github.com/WithPrecedent/snickerdoodle) [![Dependency Maintainer](https://img.shields.io/badge/dependabot-forestgreen?style=flat-square&logo=dependabot&logoColor=white&labelColor=gray)](https://github.com/dependabot) |
| Compatibility | [![Compatible Python Versions](https://img.shields.io/pypi/pyversions/courtpy?style=flat-square&color=cornflowerblue&label=Python&logo=python&logoColor=yellow)](https://pypi.python.org/pypi/courtpy/) [![Linux](https://img.shields.io/badge/Linux-lightseagreen?style=flat-square&logo=linux&labelColor=gray&logoColor=white)](https://www.linux.org/) [![MacOS](https://img.shields.io/badge/MacOS-antiquewhite?style=flat-square&logo=apple&labelColor=gray)](https://www.apple.com/macos/)  [![Windows](https://img.shields.io/badge/Windows-blue?style=flat-square)](https://www.microsoft.com/en-us/windows?r=1) |
| Stats | [![PyPI Download Rate (per month)](https://img.shields.io/pypi/dm/courtpy?style=flat-square&color=cornflowerblue&label=Downloads%20💾&logo=pypi&logoColor=yellow)](https://pypi.org/project/courtpy) [![GitHub Stars](https://img.shields.io/github/stars/WithPrecedent/CourtPy?style=flat-square&color=forestgreen&label=Stars%20⭐&logo=github)](https://github.com/WithPrecedent/CourtPy/stargazers) [![GitHub Contributors](https://img.shields.io/github/contributors/WithPrecedent/CourtPy?style=flat-square&color=forestgreen&label=Contributors%20🙋&logo=github)](https://github.com/WithPrecedent/CourtPy/graphs/contributors) [![GitHub Issues](https://img.shields.io/github/issues/WithPrecedent/CourtPy?style=flat-square&color=forestgreen&label=Issues%20📘&logo=github)](https://github.com/WithPrecedent/CourtPy/graphs/contributors) [![GitHub Forks](https://img.shields.io/github/forks/WithPrecedent/CourtPy?style=flat-square&color=forestgreen&label=Forks%20🍴&logo=github)](https://github.com/WithPrecedent/CourtPy/forks) |
| | |

-----

## What is CourtPy?

CourtPy collects court opinions, turns them into data, and analyzes them. It
is designed for empirical legal research, where the data has to be explained
and the results reproduced, and for researchers who do not write much code.

* **Collect** opinions from [CourtListener](https://www.courtlistener.com),
  from its free bulk data or its API, or use opinions you downloaded from
  Lexis-Nexis.
* **Parse** them into a table, one row for each case, with rules written in
  spreadsheets (CSV files) that anyone can read and change: the parties and
  their roles, the court, the judges, the disposition, the issues discussed,
  the citations, and more.
* **Code** derived variables, such as whether a case is criminal, whether the
  decision reversed the court below, and which side won.
* **Analyze** the table with [amos](https://github.com/WithPrecedent/amos),
  which compares models, preprocessing, and statistical methods, and records
  everything needed to reproduce the results.

## Why use CourtPy?

* **Transparent.** What CourtPy finds in an opinion comes from rules in CSV
  files, not from code, so a study's methods can be read, reported, and
  changed by anyone who can use a spreadsheet. The rules are part of the
  study, so others can check them.
* **Free data at scale.** CourtPy reads CourtListener's bulk data, which covers
  millions of opinions, needs no account, and has no limits on downloads. Its
  API client handles CourtListener's rate limits by waiting and resuming.
* **Reproducible.** A whole study (which cases, how they were parsed and coded,
  and how they were analyzed) fits in one settings file, and `amos` records
  every step and the version of every package.
* **Safe with secrets.** Your CourtListener API key is kept in your computer's
  keyring (or a private file in your user folder), never in a project, so it
  cannot be shared or committed by accident.

CourtPy is not a citation extractor or a legal research tool. For finding and
resolving citations in text, see [eyecite](https://github.com/freelawproject/eyecite).

## Getting started

### Requirements

CourtPy requires Python 3.11 or later. It runs on Linux, macOS, and Windows.
It is built on [amos](https://github.com/WithPrecedent/amos), `pandas`, and
`requests`, which are installed automatically.

Extracting cases from CourtListener's bulk data needs a good connection and
room for the bulk files (about 63 GB for the opinions, clusters, and dockets),
unless they are streamed. Downloading with the API needs a free CourtListener
account.

### Installation

To install `CourtPy`, use `pip`:

```sh
pip install courtpy
```

To install the latest development version from GitHub instead:

```sh
pip install git+https://github.com/WithPrecedent/CourtPy
```

To work on CourtPy itself, see the
[contribution guide](https://github.com/WithPrecedent/CourtPy/blob/main/CONTRIBUTING.md).

### Usage

#### Collecting opinions from CourtListener

CourtListener offers its opinions in two ways, and CourtPy uses both. Both save
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
CourtPy asks for 20 cases at a time, waits when CourtListener asks it to, and
saves its place, so a download stopped by the daily limit resumes where it
stopped when it is run again:

```python
courtpy.CourtListener().download("ca1", "2026-09-01", "2026-09-30", "court_listener")
```

Courts are named by their CourtListener ids (such as `ca1`, `cadc`, `cafc`,
and `scotus`) or by groups: `federal_appellate`, `federal_circuits`, and
`supreme_court`.

#### Keeping your API key secret

Your API key is never written in a project's files or in CourtPy itself.
Store it once:

```sh
courtpy key set
```

That asks for the key (without showing it) and stores it in your computer's
keyring (Windows Credential Manager, the macOS Keychain, or the Secret Service
on Linux), or, if there is none, in a private file in your user folder. From
then on, CourtPy finds it whenever it is needed. `courtpy key show` says where
it was found (with most of it hidden), and `courtpy key delete` removes it.

CourtPy looks for the key in this order: the `COURTLISTENER_API_KEY`
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

The rules are in the CSV files in CourtPy's
[instructions folder](https://github.com/WithPrecedent/CourtPy/tree/main/src/courtpy/instructions),
with a guide to writing your own. The "federal" rules make more than 250
variables, including:

| Variables | Meaning |
| --- | --- |
| `party1`, `party2` | The parties on each side of "v." |
| `party1_appellant`, `party2_respondent`, `party1_united_states`, ... | Each party's role, and whether it is the United States. |
| `court_num` | The circuit (1 to 11, 12 for the D.C. Circuit, 13 for the Federal Circuit, 99 for the Supreme Court). |
| `panel_judges`, `panel_size`, `author`, `concurring`, `dissenting` | The judges. |
| `disposition_reverse`, `opinion_reverse`, ... | The disposition, from the header and from the opinion's own words. |
| `agency` | The federal agency involved, if any. |
| `civil_...`, `criminal_...`, `general_...`, `procedure_...`, `standard_...` | Issues discussed in the opinion (such as `criminal_firearm`, `civil_titlevii`, and `standard_de_novo`). |
| `references_case`, `references_statute`, ... | Citations in the opinion. |
| `published`, `year`, `date_filed`, `word_count` | Other information. |

The coders (which are `amos` techniques) add `type_criminal`, each party's
role in the appeal and in the case (such as `party2_criminal_defendant`), and
the outcomes: `outcome_reversal`, `outcome_party1_won`,
`outcome_criminal_defendant_won`, and others.

#### Describing a whole study in one file

A study is an [amos](https://github.com/WithPrecedent/amos) project, described
in one settings file. CourtPy adds loaders to amos: `load_court_listener`,
`load_lexis_nexis`, and `load_cases`. Named first in the wrangler, a loader
collects the cases (downloading them, if asked), parses them, and codes them,
so the project needs no other data. See
[examples/federal_appeals.ini](https://github.com/WithPrecedent/CourtPy/blob/main/examples/federal_appeals.ini):

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
[documentation](https://WithPrecedent.github.io/CourtPy) for a tutorial, the
advanced user guide, a guide to writing rules, and recipes.

## Contributing

Contributors are always welcome. Feel free to grab an [issue](https://www.github.com/WithPrecedent/CourtPy/issues) to work on or make a suggested improvement. If you wish to contribute, please read the [Contribution Guide](https://www.github.com/WithPrecedent/CourtPy/blob/main/CONTRIBUTING.md) and [Code of Conduct](https://www.github.com/WithPrecedent/CourtPy/blob/main/CODE_OF_CONDUCT.md).

## Similar Projects

* [CourtListener](https://www.courtlistener.com) and the Free Law Project's
  tools, including [eyecite](https://github.com/freelawproject/eyecite) (which
  finds and resolves legal citations in text) and
  [juriscraper](https://github.com/freelawproject/juriscraper) (which collects
  opinions from court websites).
* [amos](https://github.com/WithPrecedent/amos), which CourtPy uses to analyze
  the cases it parses.

## License

Use of this repository is authorized under the [Apache Software License 2.0](https://www.github.com/WithPrecedent/CourtPy/blob/main/LICENSE).
