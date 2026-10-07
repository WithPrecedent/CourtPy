# Advanced User Guide

The [tutorial](tutorial.md) shows how to build a study. This guide describes
how CourtPy works and everything you can configure.

## How the pieces fit together

```text
CourtListener ──BulkData / CourtListener──▶ saved cases ──read──▶ Case ──Parser──▶ case table ──coders──▶ amos.Dataset ──amos.Project──▶ results
Lexis-Nexis   ──lexis.split──────────────▶ (JSON or text)        (sections   (rulebooks)  (one row      (parties,                   (models, tables,
                                                                   of text)                 per case)     outcomes)                   figures, report)
```

| Module | Contents |
| --- | --- |
| `courtpy.bulk` | `BulkData`, which extracts cases from CourtListener's bulk data. |
| `courtpy.courtlistener` | `CourtListener`, a client for the REST API, and `read_case`, which reads a saved CourtListener case. |
| `courtpy.lexis` | `split` and `read_case` for Lexis-Nexis text files. |
| `courtpy.rules` | `Rule` and `Rulebook`, which apply the CSV files of rules. |
| `courtpy.parsers` | `Parser` and `parse`, which turn saved cases into a table. |
| `courtpy.coders` | The coders, which are `amos` techniques. |
| `courtpy.cases` | `Case`, and `save_cases` and `load_cases` for case tables. |
| `courtpy.interface` | `Project`, `build`, `code`, and `collect`. |
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

The coders are `amos.Cleaner` techniques, so they can be named in the settings
of any `amos` worker, and they record what they did in the dataset's
`history`. They use the columns made by the federal rules.

| Coder | Adds |
| --- | --- |
| `code_parties` | Completes the parties' roles from their opposites (if one party is an appellant, the other is an appellee), and adds `party1_appealing`, `party1_defending`, and the same for `party2`. |
| `code_case_type` | `type_criminal` (the United States is a party and there is a sign of a criminal case), and each party's role in the case: `party1_prosecution`, `party1_criminal_defendant`, `party1_civil_plaintiff`, and `party1_civil_defendant` (and the same for `party2`). |
| `code_outcome` | `outcome_reversal` (from the stated disposition, or else from the opinion's words), `outcome_party1_won` and `outcome_party2_won`, the winner by side (`outcome_criminal_defendant_won`, `outcome_prosecution_won`, `outcome_civil_plaintiff_won`, and `outcome_civil_defendant_won`), and `appeal_by_defendant`. Outcomes that cannot be known are missing. |
| `drop_text` | Removes columns of text and lists (such as names and dates), which models cannot use. Its `keep` parameter names columns to keep. |

To write your own, subclass `amos.Cleaner` and write `clean`. It is added to
the library as soon as it is defined, so it can be named in settings:

```python
import dataclasses

import amos


@dataclasses.dataclass
class CodeLongOpinion(amos.Cleaner):
    """Flags opinions longer than a number of words."""

    def clean(self, data, words = 5000, **kwargs):
        data["long_opinion"] = data["word_count"] > words
        return data
```

## The "cases" section

| Setting | Meaning |
| --- | --- |
| `source` | "court_listener" (the default) or "lexis_nexis". |
| `folder` | Folder of the saved cases, in the root folder. Defaults to the name of the source. |
| `download` | For CourtListener: "bulk" to extract cases from the bulk data, "api" to download them with the API, or "none" (the default) to use the cases already in `folder`. |
| `courts` | CourtListener court ids (such as "ca1, ca2") or groups (`federal_appellate`, `federal_circuits`, or `supreme_court`). |
| `start_date`, `end_date` | Dates filed (YYYY-MM-DD). |
| `max_cases` | Most cases to download. |
| `dockets` | For "api", whether to download each case's docket too (one more request per case). |
| `bulk_folder`, `bulk_date`, `stream` | For "bulk", where to keep the bulk files, which date's files to use, and whether to read them from CourtListener without saving them. |
| `batches` | For Lexis-Nexis, files (or a folder) of many cases to divide into `folder` first. |
| `rulebooks` | Rules to parse with: built-in names (such as "federal") or CSV files or folders. Defaults to "federal". |
| `keep_text` | Whether to keep the text of the opinions in the table. |
| `limit` | Most cases to parse, for trying out rules. |
| `workers` | Number of processes to parse with. |
| `coders` | Techniques to apply after parsing. Defaults to "code_parties, code_case_type, code_outcome". Use "none" for none. Parameters for them go in "{coder}_parameters" sections. |
| `save` | File to save the table to (".csv" or ".parquet"), in the root folder. |
| `reuse` | Whether to load the saved table, if there is one, instead of collecting and parsing again. |

Relative paths are in the project's root folder: the "root_folder" of the
"files" section, the `clerk` passed to `Project.create`, or the current folder.

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
| Data | Bulk data files. | "courtpy" in `%LOCALAPPDATA%` (Windows) or `~/.local/share` | `COURTPY_DATA_DIR` |

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
| `ValueError: no court_listener cases were found in ...` | The "cases" section's folder has no saved cases. Set "download" or "folder". |
| `KeyError: the label 'outcome_reversal' is not a column of the data` | The label is made by a coder that was not run. Check the "coders" setting. |
