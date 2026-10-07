# Changelog

All notable changes to this project will be documented in this file.

<!-- insertion marker -->

## 0.2.0

A complete rewrite. The analysis is done with
[amos](https://github.com/WithPrecedent/amos) (and its workflows with
[chrisjen](https://github.com/WithPrecedent/chrisjen)), which replace
siMpLify.

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
