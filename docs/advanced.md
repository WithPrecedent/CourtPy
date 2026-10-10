# Advanced User Guide

The [tutorial](tutorial.md) shows how to build a study. This guide describes
how CourtPy works and everything you can configure.

## How the pieces fit together

```text
CourtListener ──BulkData / CourtListener──▶ saved cases ──read──▶ Case ──Parser──▶ case table ──coders──▶ amos.Dataset ──amos.Project──▶ results
Lexis-Nexis   ──lexis.split──────────────▶ (JSON or text)        (sections   (rulebooks)  (one row      (parties,                   (models, tables,
                                                                   of text)                 per case)     outcomes)                   figures, report)
```

In a project, a loader (such as `load_court_listener`) does every step before
`amos.Dataset` as the first technique of the wrangler.

| Module | Contents |
| --- | --- |
| `courtpy.bulk` | `BulkData`, which extracts cases from CourtListener's bulk data. |
| `courtpy.courtlistener` | `CourtListener`, a client for the REST API, and `read_case`, which reads a saved CourtListener case. |
| `courtpy.lexis` | `split` and `read_case` for Lexis-Nexis text files. |
| `courtpy.rules` | `Rule` and `Rulebook`, which apply the CSV files of rules. |
| `courtpy.parsers` | `Parser` and `parse`, which turn saved cases into a table. |
| `courtpy.coders` | The coders, which are `amos` techniques, and `code`, which applies them. |
| `courtpy.judges` | `Roster`, which matches the names in opinions to judges, and the `merge_judges`, `code_judges`, and `judge_votes` techniques. |
| `courtpy.politics` | `build_table`, which makes a table of each year's political context from the sources' current files, and the `code_politics` technique. |
| `courtpy.scdb` | `cases` and `votes`, which load the Supreme Court Database, and the `code_scdb` technique. |
| `courtpy.loaders` | The loaders, which are `amos` techniques that collect, parse, and code cases in a project. |
| `courtpy.cases` | `Case`, and `save_cases` and `load_cases` for case tables. |
| `courtpy.secrets` | Storing and finding the CourtListener API key. |
| `courtpy.options` | Defaults that a project can change before they are used. |

## Saved cases

Both ways of collecting CourtListener cases save each case as a JSON file named
for its cluster id, in a folder named for its court. Its keys are:

| Key | Contents |
| --- | --- |
| `source` | "court_listener". |
| `court` | The court's `id`, `full_name`, `short_name`, `citation_string`, and `jurisdiction`. |
| `cluster` | CourtListener's opinion cluster: the case name, date filed, judges, attorneys, disposition, precedential status, and so on. |
| `docket` | The docket (with the docket number and date argued), from the bulk data, or from the API if "dockets" was asked for. Otherwise `null`. |
| `citations` | The case's citations as text, such as "950 F.3d 12". |
| `opinions` | Each opinion: its type (such as "010combined" or "040dissent"), author, and one field of text (the first of `options._TEXT_FIELDS` that is not blank, which CourtListener recommends be "html_with_citations"). |
| `bulk_date` | For cases from the bulk data, the date of the files. |

Lexis-Nexis cases are text files, one for each case. `courtpy.lexis.split`
divides Lexis-Nexis downloads of many cases into such files and removes
clutter such as star paging.

## Parsing

`courtpy.parse` (or a `Parser`) turns each saved case into a row in three
stages:

1. A reader makes a `Case` with sections of text and `metadata`. CourtListener
   sections come from its data (see the table of sections in [Writing
   Rules](rules.md)); a Lexis-Nexis case has one section, "text".
2. For Lexis-Nexis, the "lexis_nexis" rules divide the text into the header and
   the opinions and find each part of the header.
3. Every section except those whose names end in "_lines" is collapsed onto one
   line, and the rules of the jurisdiction ("federal" by default) are applied.

The row starts with the case's `metadata`, which is used instead of anything
the rules find with the same name (unless the metadata is missing). A few
columns are then added: `author` (the first of `authors`), `panel_size`,
`concurrences`, `dissents`, and `word_count`. The text of the opinions is not
kept unless `keep_text` is true, because it would make the table very large.

Parsing a large set of cases takes a while. Pass `workers` (the number of
processes) to `parse` to use more of your computer.

## The case table

The table is labeled by "case_id": the CourtListener cluster id, or the name of
the Lexis-Nexis file. Flags are booleans, counts are integers, and matches and
names are lists. Columns of booleans or whole numbers that are missing for
some cases (such as `published` or `year`) have the "boolean" and "Int64"
types, which allow missing values.

`save_cases` saves a table as a CSV file (with lists written as text, separated
by "; ", and the names of those columns saved beside the file) or as a parquet
file (with the optional pyarrow package, which keeps lists and types as they
are), and `load_cases` loads it again.

## Coders

The coders are `amos` techniques, so they can be named in the settings of any
`amos` worker, and they record what they did in the dataset's `history`.
`code_parties`, `code_case_type`, and `code_outcome` are mungers (`amos.Munger`),
which change and add columns without adding or removing rows, so the history
lists the columns that each one changed and created. `drop_text` is a cleaner
(`amos.Cleaner`), which removes columns. They use the columns made by the
federal rules.

| Coder | Adds |
| --- | --- |
| `code_parties` | Completes the parties' roles from their opposites (if one party is an appellant, the other is an appellee), and adds `party1_appealing`, `party1_defending`, and the same for `party2`. |
| `code_case_type` | `type_criminal` (the United States is a party and there is a sign of a criminal case), and each party's role in the case: `party1_prosecution`, `party1_criminal_defendant`, `party1_civil_plaintiff`, and `party1_civil_defendant` (and the same for `party2`). |
| `code_outcome` | `outcome_reversal` (from the header's disposition, or else from the decision that the opinion states, or else from the opinion's words anywhere), `outcome_party1_won` and `outcome_party2_won`, the winner by side (`outcome_criminal_defendant_won`, `outcome_prosecution_won`, `outcome_civil_plaintiff_won`, and `outcome_civil_defendant_won`), and `appeal_by_defendant`. Outcomes that cannot be known are missing. |
| `drop_text` | Removes columns of text and lists (such as names and dates), which models cannot use. Its `keep` parameter names columns to keep. |

To write your own, subclass `amos.Munger` and write `munge` (or subclass
`amos.Cleaner` and write `clean` for a coder that removes rows or columns,
`amos.Merger` and write `match` for one that adds the columns of another
table, or `amos.Shaper` and write `shape` for one that changes what a row
is). It is added to the library as soon as it is defined, so it can be named
in settings:

```python
import dataclasses

import amos


@dataclasses.dataclass
class CodeLongOpinion(amos.Munger):
    """Flags opinions longer than a number of words."""

    def munge(self, data, words = 5000, **kwargs):
        data["long_opinion"] = data["word_count"] > words
        return data
```

## Judges and panels

The federal rules find the names of each case's judges as the opinion writes
them (`panel_judges`, `authors`, `concurring`, and `dissenting`), such as
"LYNCH". `courtpy.judges` matches those names to people, with a roster made
from the Federal Judicial Center's
[Biographical Directory of Article III Federal Judges](https://www.fjc.gov/history/judges/biographical-directory-article-iii-federal-judges-export):

```python
import courtpy

roster = courtpy.judges.Roster.from_fjc()
roster.save()
```

`from_fjc` downloads three files of a few megabytes (federal judicial service,
demographics, and professional careers) unless they were downloaded before
(`update = True` downloads them again, for the judges and changes since). The
roster is a table with one row for each court that a judge has served on, with
the president who made the appointment and that president's party, the
American Bar Association's rating, the Senate's vote, the judge's sex, race or
ethnicity, and year of birth, and flags for the judge's career (see
`courtpy.judges.build_roster` for every column). `save` writes it as
"roster.csv" in "judges" in CourtPy's data folder, outside of any project,
where the techniques below look for it. Their `roster` parameter names another.

The roster is a CSV file, so you can change it in a spreadsheet:

* Add a judge, or another name that opinions use for a judge (as "First
  Middle Last" in the `aliases` column, with semicolons between names).
* Add a column of your own, such as a score of each judge's ideology (or pass
  `scores`, a table with the judges' `nid` numbers, to `from_fjc`). Every
  column of numbers or booleans that does not identify or date a judge's
  service is an attribute of the judge, which the techniques use.
* Change how careers are coded with rules of your own (the `rulebook`
  argument). The built-in rules are in
  [`instructions/judges.csv`](https://github.com/WithPrecedent/courtpy/blob/main/src/courtpy/instructions/judges.csv).

Opinions usually give only a judge's last name, so a name is matched with the
court and year of its case: to a judge of the case's court, or else to a judge
of a district court in its circuit (who may sit by designation), or else to a
judge of any other court (who may be visiting). Only judges who were serving
that year, or had left their court no more than two years before
(`options._JUDGE_GRACE`), are considered. Rather than guess, a name that fits
more than one such judge is not matched (which is why opinions add initials,
as in "M. SMITH" and "N.R. SMITH"), and neither is a name on a case of a court
that has no judges in the roster, such as a bankruptcy appellate panel.

Cases and judges are different things: a case table has a row for each case,
and a roster a row for each court that each judge has served on. `amos` (0.2.6
and later) joins such tables with
[mergers](https://WithPrecedent.github.io/amos/advanced/#merging-data), which
add the columns of another table to the rows that they match without ever
adding or losing a row, and
[shapers](https://WithPrecedent.github.io/amos/advanced/#reshaping-data),
which change what a row is. CourtPy's three techniques for judges are built
from them:

| Technique | Adds |
| --- | --- |
| `merge_judges` | A merger. Each row names one judge (in the column that its `name` parameter names, which is "judge" unless another is set), and gets the columns of that judge's row of the roster, with "judge_" (or another `prefix`) before their names: every attribute, and `age`, `senior`, `designated` (sitting on a court other than the judge's own), and `district_judge`, which depend on the case's year and court. All are numbers (1 and 0 for true and false). Its `columns` parameter names the columns to add, which can be any others of the roster too (such as `judge`, the judge's name, or `president`), and `indicator` names a column that says whether each judge was found. |
| `code_judges` | A munger. `panel_names` (the judges who were found, as the roster names them), `panel_found` (how many, to compare with `panel_size`), `author_name`, `concurring_names`, and `dissenting_names`, and the composition of the panel: `panel_{attribute}` is the mean of each attribute among its judges. So `panel_woman` is the share who are women, and `panel_party` is the mean party of the presidents who appointed them (from -1, all Democrats, to 1, all Republicans). `panel_age`, `panel_senior`, `panel_designated`, and `panel_district_judge` depend on the case's year and court. |
| `judge_votes` | A shaper. Replaces the table of cases with one row for each judge on each case, labeled by "vote_id" (such as "4567890-2"): the case's columns, `case_id`, `judge`, `judge_nid`, `judge_{attribute}`, `colleagues_{attribute}` (the mean among the other judges of the panel), `judge_author`, `judge_concurred`, `judge_dissented`, and for each outcome of the case, `vote_{outcome}`: the outcome, or its opposite if the judge dissented. So `vote_reversal` is whether the judge voted to reverse. Its `outcomes` parameter names the outcomes (by default, every column that starts with "outcome_"). Like every shaper, it takes `label`, `task`, and `groups` for the table that it makes, and must come before the data is split. |

`code_judges` and `judge_votes` do what a wrangler of
`lists_to_rows, merge_judges, group_rows` would do: they make a row for each
name that a case lists, find the judge that each name refers to, and (for
`code_judges`) summarize the rows of each case. Use `merge_judges` itself
for a table that names one judge in each row. For example, to add what is
known about the author of each opinion to a table of cases:

```ini
[wrangler]
techniques = load_court_listener, merge_judges, drop_text

[merge_judges_parameters]
name = author
prefix = author_
indicator = author_found
```

The dataset's history records how many rows a merger matched, which is worth
checking (and reporting) every time:

```python
print(project.result.history[-2])
# {'technique': 'merge_judges', 'source': '...roster.csv', 'rows': 5212, 'matched': 4980, 'created': ['author_party', ...]}
```

All three can be among a loader's "coders", so a vote can be the label of a
study (see [examples/judge_votes.ini](https://github.com/WithPrecedent/courtpy/blob/main/examples/judge_votes.ini)
and the [recipes](recipes.md)). In Python, `courtpy.judges.vote_table` makes
the same table as `judge_votes`, and a `Roster` matches one name at a time:

```python
row = roster.match("LYNCH", court_num = 1, year = 2020)
print(roster.names([row]), roster.describe(row, court_num = 1, year = 2020))
```

A loader passes its coders no parameters, so as coders they use the saved
roster. To name another, list them as techniques of the wrangler instead
(after the loader), with a section of parameters for each. `merge_judges`
takes the roster as its `source`, as every merger takes its other table, and
finds a file that is named there as the project's clerk finds files: in the
current folder, and then in the input folder. `code_judges` and `judge_votes`
take it as `roster`, whose path is relative to the folder that the study is
run from.

## Political context

`code_politics` adds what a table of years says about each case's year, with
"politics_" before the names of the table's columns. With no table, it adds
`politics_president_party`: -1 for a Democratic president and 1 for a
Republican. `courtpy.politics.build_table` makes a table with two more
sources:

| Columns | Meaning | File |
| --- | --- | --- |
| `supreme_court_median`, `supreme_court_min`, `supreme_court_max` | The [Martin-Quinn scores](https://mqscores.wustl.edu/measures.php) of the median justice and of the most liberal and most conservative justices in each term. | "court.csv", the scores of the court in each term. |
| `senate_median`, `house_median` | The median [NOMINATE](https://voteview.com/data) score of the members of each chamber of Congress. | "HSall_members.csv", the ideology of every member of every Congress (Voteview's "Member Ideology" data). |

Higher scores are more conservative. `build_table` downloads the sources'
current files (about 6 megabytes) the first time, and keeps them in
"politics" in CourtPy's data folder.
`courtpy.politics.download(overwrite = True)` downloads them again, for the
terms and votes since. To use files of your own instead, pass their paths,
or `False` to leave a source out:

```python
table = courtpy.politics.build_table()
table.to_csv("data/politics.csv", index = False)
```

```ini
[files]
input_folder = data

[wrangler]
techniques = load_court_listener, code_politics, drop_text

[code_politics_parameters]
source = politics.csv
```

`code_politics` is the `merge_keys` merger of `amos` with the year as its key,
so it takes that merger's parameters. `source` is the table of years, which
is found as the project's clerk finds files (in the current folder, and then
in the input folder). Any table with a "year" column can be used, so other
measures can be added in a spreadsheet. `on` and `other_on` name the columns
to match by, `prefix` the text before the new columns' names, `columns` the
columns to add, and `indicator` a column that says whether each case's year
was in the table. A table with two rows for a year stops the study, unless
`duplicates` says to use the "first" or "last" of them. As one of a loader's
coders, `code_politics` gets no parameters, so it adds only the party of the
president.

`courtpy.politics.justices` returns the Martin-Quinn score of each justice in
each term, from the "justices.csv" file of the same page, for studies of the
Supreme Court (see below).

## The Supreme Court Database

The [Supreme Court Database](https://scdb.la.psu.edu) codes every case that
the Supreme Court has decided since its 1946 term: the issue, the direction
of the decision, what the court did with the decision below, which party won,
and how each justice voted. `code_scdb` (a merger) adds its coding to parsed
cases of the Supreme Court, with "scdb_" before the database's names for its
columns:

```ini
[load_court_listener_parameters]
courts = scotus
coders = code_parties, code_case_type, code_outcome, code_scdb
```

A case is matched by the database's id for it, which CourtListener records
for cases of the Supreme Court (the `scdb_id` column), or else by one of its
citations (the `citation` column), such as "347 U.S. 483". Rather than guess,
a case whose citations are those of more than one case of the database (as
short orders on one page of a reporter are) is not matched. The history
records how many cases were matched and the release of the database that was
used.

The latest release is downloaded from the database's
[data page](https://scdb.la.psu.edu/data/) the first time, and kept in "scdb"
in CourtPy's data folder (`courtpy.scdb.download(overwrite = True)` looks for
a newer one). As a technique of the wrangler, `code_scdb` takes a merger's
parameters: `source` (a file of the database that you downloaded yourself),
`columns` (such as "issueArea, decisionDirection, partyWinning"), `prefix`,
and `indicator`, and `on`, `other_on`, and `citations` for the columns to
match by. The database's
[online codebook](https://scdb.la.psu.edu/online-codebook/) explains each
variable.

In Python, `courtpy.scdb.cases` returns the database's table with a row for
each case, and `courtpy.scdb.votes` its table with a row for each justice in
each case. The Martin-Quinn scores use the database's numbers for the
justices, so a merger adds each justice's score to each vote:

```python
import amos
import courtpy

votes = amos.mergers.MergeKeys().apply(
    courtpy.scdb.votes(),
    source = courtpy.politics.justices(),
    on = ["term", "justice"])
print(votes.data[["caseName", "justiceName", "vote", "martin_quinn"]].head())
print(votes.history[-1]["matched"])
```

## Loaders

`amos` (0.2.3 and later) loads a project's data with loaders, the first
techniques of the wrangler, so a project with a loader needs no `item`.
CourtPy's loaders are a genre of `amos` loaders, `CaseLoader`:

| Loader | Source | Does |
| --- | --- | --- |
| `load_court_listener` | A folder of saved CourtListener cases. Defaults to "court_listener". | Downloads cases into the folder (if "download" is set), parses them, and codes them. |
| `load_lexis_nexis` | A folder of Lexis-Nexis cases, one per text file. Defaults to "lexis_nexis". | Divides any "batches" into the folder, parses the cases, and codes them. |
| `load_cases` | A table saved by `save_cases` (or `courtpy parse`). | Loads the table, with its lists and types. It applies no coders unless "coders" names some. |

Their parameters go in a "{loader}_parameters" section of the settings:

| Parameter | Loaders | Meaning |
| --- | --- | --- |
| `source` | all | The folder (or, for `load_cases`, the file) to load. |
| `coders` | all | Techniques to apply after parsing. Defaults to "code_parties, code_case_type, code_outcome" ("none" for `load_cases`). Use "none" for none. |
| `save` | all | File to save the coded table to (".csv" or ".parquet"). |
| `reuse` | all | Whether to load the saved table, if there is one, instead of collecting, parsing, and coding again. |
| `label`, `task`, `groups` | all | As in `amos`. They default to those in the "general" section. |
| `download` | `load_court_listener` | "bulk" to extract cases from the bulk data, "api" to download them with the API, or "none" (the default) to use the cases already in the folder. |
| `courts` | `load_court_listener` | CourtListener court ids (such as "ca1, ca2") or groups (`federal_appellate`, `federal_circuits`, or `supreme_court`). |
| `start_date`, `end_date` | `load_court_listener` | Dates filed (YYYY-MM-DD). |
| `max_cases`, `overwrite` | `load_court_listener` | Most cases to download, and whether to download cases saved before. |
| `dockets` | `load_court_listener` | For "api", whether to download each case's docket too (one more request per case). |
| `bulk_folder`, `bulk_date`, `stream` | `load_court_listener` | For "bulk", where to keep the bulk files, which date's files to use, and whether to read them from CourtListener without saving them. |
| `batches` | `load_lexis_nexis` | Files (or folders) of many cases to divide into the source folder first. |
| `rulebooks` | `load_court_listener`, `load_lexis_nexis` | Rules to parse with: built-in names (such as "federal") or CSV files or folders. Defaults to "federal". |
| `keep_text`, `limit`, `workers` | `load_court_listener`, `load_lexis_nexis` | Whether to keep the opinions' text, most cases to parse, and processes to parse with. |

The loaders work through the project's clerk, as `amos` loaders do. A folder
or file named by a relative path is looked for in the current folder and
then in the clerk's input folder (the "input_folder" of the "files" section),
where downloads save their cases, and "save" is in the clerk's interim folder
("interim_folder"). The label is checked after the coders run, so it can be a
column that a coder makes (such as `outcome_reversal`), and the dataset's
history records the loader and then each coder.

A loader can also be used without a project. `apply` returns an
`amos.Dataset`:

```python
dataset = courtpy.loaders.LoadCourtListener().apply(
    source = "court_listener", label = "outcome_reversal")
```

## The CourtListener API

A free CourtListener account can make 5 requests a minute, 50 an hour, and 125
a day (as of 2026); members have more. `CourtListener` asks for 20 cases (one
page of opinion clusters) in one request and their opinions in one more, so a
free account can download roughly a thousand cases a day.

* Requests are at least `min_interval` seconds apart (1 by default).
* When CourtListener asks for a pause (an HTTP 429 response), the client waits
  as long as it is asked, up to `max_wait` seconds (an hour by default), which
  rides out the minute and hour limits. A longer pause (the daily limit)
  raises a `RateLimitError`.
* The place in each court's list of cases is saved in a ".courtpy" folder
  inside the download folder, so running the same download again resumes it.
  Running a finished download again takes one request for each court and
  saves any cases added since.
* `download_case` downloads one case by its cluster id.

## The bulk data

CourtListener publishes its whole database every three months, at
<https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/>.
`BulkData.extract` reads five files, in this order: courts, dockets (about 5 GB),
opinion clusters (about 2.5 GB), citations, and opinions (about 55 GB). It keeps
only the cases of the courts and dates asked for, so it needs little memory,
but reading the opinions file takes hours. Extract every court and year you may
need at once.

By default, the files are downloaded (resuming an interrupted download) and
kept in "bulk" in CourtPy's data folder, outside of any project, so they can
be used again. Set `folder` to keep them elsewhere, `date` to use older files
(`available()` lists the dates), or `stream = True` to read them from
CourtListener without saving them.

## Secrets and folders

`courtpy.secrets.get_api_key` looks for the API key in, in order: the
`COURTLISTENER_API_KEY` environment variable, the system keyring, the secrets
file (`courtpy.secrets.secrets_path()`), and a `.env` file in the current
folder. `set_api_key` stores the key in the keyring if there is one and in the
secrets file otherwise (or where its `store` argument says).

| Folder | Holds | Default | Changed with |
| --- | --- | --- | --- |
| Configuration | The secrets file. | "courtpy" in `%APPDATA%` (Windows) or `~/.config` | `COURTPY_CONFIG_DIR` |
| Data | Bulk data files, the roster of judges and the files it is made from, and the files of Martin-Quinn scores, NOMINATE scores, and the Supreme Court Database. | "courtpy" in `%LOCALAPPDATA%` (Windows) or `~/.local/share` | `COURTPY_DATA_DIR` |

## Options

`courtpy.options` holds defaults as module-level constants, which a project can
change before they are used. For example, to wait at least two seconds between
requests to the API, or to add a group of courts:

```python
import courtpy

courtpy.options._MIN_INTERVAL = 2.0
courtpy.options._COURT_GROUPS["new_england"] = ("ca1", "mad", "nhd", "med", "rid")
```

## Errors you may see

| Error | Cause |
| --- | --- |
| `MissingAPIKeyError: no CourtListener API key was found` | The API needs a key. Run `courtpy key set`, or use the bulk data. |
| `CourtListenerError: CourtListener rejected the request (401)` | The key is wrong. Check it with `courtpy key show`. |
| `RateLimitError: CourtListener asked courtpy to wait ... hours` | The account's daily limit was reached. Run the same download again later. |
| `ValueError: ... row N: the pattern ... is not valid` | A rule's regular expression has an error. `courtpy rules file.csv` checks a file. |
| `KeyError: 'code_outcome' needs the columns [...]` | A coder needs columns made by the federal rules. |
| `ValueError: no court_listener cases were found in ...` | The loader's source folder has no saved cases. Set "download" or "source". |
| `ModuleNotFoundError: No module named 'statsmodels'` | `logit_sm` (and the other statsmodels models) need statsmodels. Use `logit` for scikit-learn's logistic regression, or `pip install amos[statistics]`. |
| `KeyError: the label 'outcome_reversal' is not a column of the data` | The label is made by a coder that was not run. Check the "coders" setting. |
| `FileNotFoundError: there is no roster of judges at ...` | `merge_judges`, `code_judges`, and `judge_votes` need a roster. Make and save one with `courtpy.judges.Roster.from_fjc().save()`, or name one with the "source" parameter of `merge_judges` or the "roster" parameter of the others. |
| `ValueError: ... rows of the other table have the same ['year'] as another of its rows` | The table of `code_politics` has more than one row for a year, and a case can match only one. Set its "duplicates" to "first" or "last", or remove the rows. |
| `TypeError: 'code_politics' cannot match 'year', which is text in the data, with 'year', which is numbers in the other table` | The cases' years are not numbers (as they are in a table that was saved and loaded without `courtpy.load_cases`). Load the table with `load_cases`, or change the column with a munger such as `parse_numbers`. |
| `KeyError: 'code_scdb' needs the column 'scdb_id' ... or 'citation'` | `code_scdb` matches cases by the Supreme Court Database's ids, which CourtListener records for cases of the Supreme Court, or by their citations. Set its "on" or "citations" to the columns that have them. |
| `ValueError: '...' changes the rows of the data, so it must come before the data is split` | `judge_votes` is a shaper, which came after a splitter. Put it in the wrangler (or among a loader's coders). |
| `ImportError: cannot import 'fairlearn.metrics...'` | A study has "groups" (such as `case_id`, for judges' votes), so the scorecard compares the model across them, which needs fairlearn. Name the scorecard's "metrics", or `pip install amos[fairness]`. |
