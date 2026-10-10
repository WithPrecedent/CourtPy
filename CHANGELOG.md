# Changelog

All notable changes to this project will be documented in this file.

<!-- insertion marker -->

## 0.2.2

Updated to [amos](https://github.com/WithPrecedent/amos) 0.2.6, whose mergers
and shapers join tables whose rows are different things. CourtPy uses them to
add judges, the politics of each year, and the Supreme Court Database to
cases, and downloads other researchers' data from its sources.

The federal rules now read what recent CourtListener cases have only in the
text of their opinions. CourtListener's full case name, judges, and
disposition are blank for about nine in ten of the federal appellate cases
filed since 2025, so the parties' roles and the panel were unknown, and
reversals were coded from the opinion's words anywhere. In a sample of 1,500
such criminal appeals, the roles are now found in 97%, and a panel of three
judges in 99%.

### Added

* The federal rules find the caption in the opinion's opening (`caption`,
  `caption1`, and `caption2`) when the header's caption names no appellant
  or appellee, and code `party1_appellant`, `party1_appellee`,
  `party2_appellant`, and `party2_appellee` from it. A caption is a role
  before "v." ("Plaintiff-Appellee, v."), or parties in capital letters
  with one role, so a citation in the opinion's words is not mistaken for
  one.
* The federal rules find the decision that an opinion states (`decision`:
  "AFFIRMED.", "VACATED AND REMANDED.", "Affirmed by unpublished per
  curiam opinion", or "we reverse", but not "we will reverse only if") and
  code `decision_affirm`, `decision_reverse`, `decision_vacate`,
  `decision_remand`, and `decision_dismiss` from it.
* `courtpy.judges` identifies the judges on each case, which the earlier,
  unreleased CourtPy did and this version had not rebuilt. A `Roster`
  (`Roster.from_fjc()`) is made from the Federal Judicial Center's files of
  judges' service, demographics, and careers, which `download` gets, and
  matches a name in an opinion to a judge of the case's court, or else of a
  district court in its circuit, or else of another court, among the judges
  serving in the case's year. The careers are coded by the rules in
  `instructions/judges.csv`. Unlike the earlier version, a name that fits
  more than one judge is left unmatched instead of being given to one of
  them, no judge is matched outside the judge's years of service, a judge
  who was reassigned to another court keeps the appointment of the judge's
  own earlier service, a rating or vote that is not recorded is missing
  instead of zero, the district courts of Vermont are in the Second Circuit,
  and a clerkship at a district court does not make a judge a prosecutor.
* `merge_judges` (an `amos.Merger`) adds what the roster says about the
  judge that each row names, such as the author of each opinion, with a
  prefix: the judge's attributes, and the judge's age, senior status, and
  whether the judge sat by designation in the case's year and court. Like
  every merger, it takes "source", "columns", "prefix", and "indicator",
  never adds or removes a row, and records how many rows it matched.
* `code_judges` (a munger) adds the judges of each panel who were found
  (`panel_names` and `panel_found`) and the panel's composition: the mean of
  each attribute of its judges, such as `panel_party`, `panel_woman`,
  `panel_age`, and `panel_designated`. It is built from the `lists_to_rows`
  shaper of amos, `merge_judges`, and the `merge_summary` merger.
* `judge_votes` (an `amos.Shaper`) and `courtpy.judges.vote_table` make a
  table with one row for each judge on each case, with the judge's
  attributes (`judge_{attribute}`), those of the other judges of the panel
  (`colleagues_{attribute}`), and the judge's vote on each outcome
  (`vote_reversal` and so on: the outcome, or its opposite for a judge who
  dissented). Like every shaper, it takes "label", "task", and "groups" for
  the table that it makes, and must come before the data is split. It can be
  one of a loader's coders, so a vote can be the label of a study, and
  `load_cases` reads the `case_id` column of a saved table of votes as text,
  so a study splits its cases the same way when the table is reused.
* `courtpy.politics` and `code_politics` add the political context of each
  case's year: the party of the president and, from a table that
  `build_table` makes, the Supreme Court's Martin-Quinn scores and the
  median NOMINATE scores of each chamber of Congress. `code_politics` is the
  `merge_keys` merger of amos with the year as its key, so it takes that
  merger's parameters ("source", "on", "other_on", "columns", "prefix",
  "indicator", and "duplicates"), and a table with a year twice stops a
  study instead of quietly using one of its rows. A term of the court that
  the scores divide in two (as 2005 is) is one year, which the earlier
  version's table had twice.
* `courtpy.politics.download` gets the current files of Martin-Quinn scores
  (from the links of https://mqscores.wustl.edu/measures.php) and of the
  ideology of members of Congress (the "Member Ideology" data of
  https://voteview.com/data), and `build_table`, `martin_quinn`, and
  `nominate` use them when they are given no files.
  `courtpy.politics.justices` returns the Martin-Quinn score of each justice
  in each term.
* `courtpy.scdb` adds the Supreme Court Database. `download` gets the latest
  release from https://scdb.la.psu.edu/data/, `cases` and `votes` return its
  case centered and justice centered tables, and `code_scdb` (an
  `amos.Merger`) adds its coding to parsed cases of the Supreme Court
  (`scdb_issueArea`, `scdb_decisionDirection`, and so on), matched by the
  database's id of each case or else by a citation. CourtListener cases of
  the Supreme Court have that id as `scdb_id`.
* `examples/judge_votes.ini` is a study of how judges vote in criminal
  appeals.

### Changed

* Requires amos 0.2.6.
* The repository is "courtpy" (in lower case, like the package), so the
  links to it and to the documentation, which had stopped working, are
  https://github.com/WithPrecedent/courtpy and
  https://WithPrecedent.github.io/courtpy.
* `panel_judges` comes from the panel that the opinion's opening names
  ("Before SMITH, Chief Judge, and JONES and BROWN, Circuit Judges", or
  "Present:"), including a judge sitting by designation, and only
  otherwise from the source's list of judges, which in CourtListener may
  name only the author or be blank. The text that was used is `panel_text`.
* `code_outcome` uses the decision that the opinion states when the header
  has no disposition, and the opinion's words anywhere (`opinion_reverse`,
  `opinion_vacate`, and `opinion_remand`) only when there is neither. Those
  words are often about an earlier decision or a standard of review ("we
  will reverse only if"): in the sample, they coded 17% of defendants'
  appeals as reversals, and the stated decisions 9%.

### Fixed

* A judge's middle initial "J." is no longer read as "Justice", which
  divided "LAWRENCE J. VILARDO" into two judges, and initials such as
  "S.R." before a name are no longer read as "Sr.", which left "S.R.
  THOMAS" as "THOMAS". "J.", "JR.", and "SR." are still removed when no
  name follows them.

## 0.2.1

Updated to [amos](https://github.com/WithPrecedent/amos) 0.2.5.

### Changed

* Requires amos 0.2.5.
* `code_parties`, `code_case_type`, and `code_outcome` are now mungers
  (`amos.Munger`, new in amos 0.2.4), since they change and add columns
  without adding or removing rows. Their names in settings are unchanged, and
  the dataset's history now lists the columns that each one changed and
  created. `drop_text`, which removes columns, is still a cleaner.
* The example study and the documentation use `logit` for scikit-learn's
  logistic regression, which amos 0.2.4 renamed from `sk_logit`. (Its
  statsmodels model is now `logit_sm`.) Settings that name `sk_logit` must use
  `logit`.

### Added

* A recipe for searching the opinions' text with amos's `flag_patterns` and
  `count_patterns` mungers, without a file of rules.

## 0.2.0

Updated to [amos](https://github.com/WithPrecedent/amos) 0.2.3, whose loaders
replace the "cases" section of a project's settings. Settings written for
0.1.0 need the changes below.

### Added

* Loaders, a genre of `amos` loaders (`CaseLoader`) in the new `loaders`
  module: `load_court_listener` downloads (if asked), parses, and codes
  CourtListener cases; `load_lexis_nexis` divides, parses, and codes
  Lexis-Nexis cases; and `load_cases` loads a table saved by `save_cases`.
  Named first in the wrangler, a loader makes the project's data, so a
  project needs no `item`. Loaders work through the project's clerk: sources
  are found (and downloads saved) in its input folder, and a "save"d table is
  kept in its interim folder. The label is checked after the coders run, so
  it can be a column that a coder makes, and the dataset's history records
  the loader and each coder.

### Changed

* Requires amos 0.2.3 (and pandas 3).
* `courtpy.Project` is now `amos.Project` itself, which runs a workflow with a
  loader without an `item`.
* `code` moved to the `coders` module (it is still `courtpy.code`).
* The example study and the documentation use `sk_logit`, scikit-learn's
  logistic regression, because amos 0.2.3 gave the name `logit` to the
  statsmodels model (which needs `pip install amos[statistics]`).

### Removed

* The "cases" section of a project's settings, `courtpy.build`,
  `courtpy.collect`, and the `interface` module. Move the settings of a
  "cases" section to a "load_court_listener_parameters" (or
  "load_lexis_nexis_parameters") section, add the loader to the start of the
  wrangler's techniques, and rename "folder" to "source". Relative paths are
  now in the clerk's input folder ("save" in its interim folder) rather than
  its root folder.

## 0.1.0

The first release, and the first on PyPI. It is a complete rewrite of the
earlier, unreleased CourtPy, which was built on siMpLify. The analysis is now
done with [amos](https://github.com/WithPrecedent/amos) (and its workflows
with [chrisjen](https://github.com/WithPrecedent/chrisjen)). The changes below
are from that earlier code.

### Added

* Downloads from CourtListener, now the main source of opinions:
  * `BulkData` extracts cases from CourtListener's quarterly bulk data, which
    needs no API key and has no limits. It downloads the files (resuming an
    interrupted download) or streams them.
  * `CourtListener` downloads cases with the REST API (version 4). It spaces
    its requests, waits when CourtListener asks it to, and saves its place so
    that a download stopped by an account's limit resumes where it stopped.
  * Both save each case as a JSON file, which `courtlistener.read_case` reads.
    CourtListener's header information comes from its data, so no rules are
    needed to find it.
* `courtpy.secrets` keeps the CourtListener API key outside of any project:
  in the system keyring, in a private file in the user's configuration folder,
  in the `COURTLISTENER_API_KEY` environment variable, or in a `.env` file.
* A single rule engine (`Rule` and `Rulebook`) for every CSV file of rules,
  with one set of columns (`target`, `variable`, `kind`, `pattern`, `value`,
  `ignorecase`, `dotall`, and `note`) and nine kinds of rules. See
  `src/courtpy/instructions/README.md`.
* The coders `code_parties`, `code_case_type`, `code_outcome`, and `drop_text`,
  which are `amos` techniques and can be named in any `amos` settings.
* `courtpy.Project`, an `amos.Project` whose "cases" section collects, parses,
  and codes the cases before the analysis.
* The `courtpy` command (`key`, `download`, `split`, `parse`, `rules`, and
  `run`).
* Tests, and an example study (`examples/federal_appeals.ini`).
* The parts of the [snickerdoodle](https://github.com/WithPrecedent/snickerdoodle)
  template: GitHub Actions (tests on Linux, macOS, and Windows with Python
  3.11 to 3.14, linting, type checking, documentation deployed to GitHub
  Pages, releases, publishing to PyPI, and validating the Codecov settings),
  dependabot, issue templates, pre-commit hooks, an editor configuration, a
  code of conduct, a contribution guide, and documentation built with MkDocs
  (a tutorial, an advanced user guide, a guide to writing rules, recipes, and
  the API reference).

### Changed

* The package uses a `src` layout and is built with hatchling and uv.
* The old instruction files were converted to the new columns:
  `organizer_lexis_nexis.csv` and `separate_opinions.csv` became
  `lexis_nexis.csv`, `keywords_federal.csv` became `federal/header.csv`, and
  `parser_opinions_federal.csv` became `federal/opinion.csv` (and is now
  saved in UTF-8). Some variables were renamed to be easier to read, such as
  `party_appnt1` to `party1_appellant`, `counsel_us_atty` to
  `counsel_us_attorney`, `notice_unpub_rule` to `notice_unpublished`, and
  `disposition_op_reversed` to `opinion_reverse`.
* A decision is coded as a reversal from its stated disposition when it has
  one, and only otherwise from the opinion's words. (Before, either one was
  enough, so "we will not reverse" in an affirmed case counted as a
  reversal.)
* Which party won an appeal is now coded from the party's role: an appellant
  wins a reversal, and an appellee wins an affirmance. (The old code had the
  two reversed.) Outcomes that cannot be known are missing values rather than
  false.
* Judges' names are split at whole words only, so names such as "Kirby" and
  "Holland" are no longer cut at "BY" and "AND".

### Fixed in the rules

* `Fed_App_P`, `Fed_Civ_P`, `Fed_Crim_P`, and `Fed_Evid` required a "§" sign,
  which is not used when federal rules are cited, so they never matched. The
  evidence rule's pattern was also malformed.
* `references_case` now includes the Federal Reporter, Fourth Series (F.4th).
* `amend8` searched for the Seventh Amendment's numeral ("VII").
* `titlevii` required the text "Title VII.," followed by a space.
* `alcohol` searched for "ddistilled".
* The opinion's dispositions (`opinion_reverse`, `opinion_vacate`,
  `opinion_remand`, and `opinion_dismiss`) only matched "We willreverse" (with
  no space) or a capitalized "Dismiss".
* The variables `criminal_manslaugther` and `general_aritcleiv` are now
  `criminal_manslaughter` and `general_articleiv`.
* Non-breaking spaces in patterns now match any space.
* Lexis-Nexis header sections that end the header (such as a "JUDGES:"
  paragraph just before "OPINION") are now found.
* Agencies are matched from the longest name down, so "Postal Service Board of
  Contract Appeals" is no longer labeled "Postal Service".

### Removed

* The siMpLify-based modules (`almanac`, `cookbook`, `entities`, `implements`,
  `cases.py`, and `controller.py`) and the settings files that went with them.
* The `federal_archive` instruction files, which the combined files had
  replaced.
* The external data used to add judges' biographies and political context is
  kept in `reference/`, but is not yet used by the new version.
