# Instructions: the rules that parse court opinions

courtpy finds information in court opinions with rules written in CSV files,
which can be opened and changed in Excel, Google Sheets, or any spreadsheet.
Each row is one rule. You can change these files, or make your own and pass
them to courtpy (`rulebooks = my_rules.csv` in a settings file, or
`courtpy.parse(folder, rulebooks = ["federal", "my_rules.csv"])` in Python).

| File | What it does |
| --- | --- |
| `lexis_nexis.csv` | Divides a Lexis-Nexis case into its header and opinions, and finds each part of the header (the parties, court, docket number, dates, history, counsel, disposition, citation, judges, and author). CourtListener cases do not need it, because CourtListener stores those parts separately. |
| `federal/header.csv` | Finds variables in the parts of the header: the court's number, each party's role, the agency involved, the disposition, the counsel, publication, and the names of the judges. When a case has no roles or judges in its header (as recent CourtListener cases often do not), it reads them from the caption and the "Before ..." line at the start of the opinion. |
| `federal/opinion.csv` | Finds variables in the text of the opinions: the legal issues discussed (civil, criminal, constitutional, procedural, and standards of review), the decision that the opinion states ("AFFIRMED." or "we reverse"), its words about dispositions anywhere, and citations to cases, statutes, regulations, and other sources. |
| `judges.csv` | Not for opinions: it codes the careers of judges for a roster of them (see `courtpy.judges.build_roster`), as a prosecutor, public defender, law clerk, Supreme Court clerk, lawyer of a solicitor general's office, or law professor. Its rules search the section `career`, which has each position that a judge held (as the Federal Judicial Center reports it) on its own line. |

The "federal" rulebook is both `federal` files, applied in that order.

## The columns

| Column | Meaning |
| --- | --- |
| `target` | The section of text to search, such as `opinion`, `party1`, or `disposition`. List several, separated by commas, to search each of them. |
| `variable` | The name of the column that the rule makes. Rules with the same variable are combined. |
| `kind` | What the rule does (see below). |
| `pattern` | A [regular expression](https://docs.python.org/3/howto/regex.html) to search for. Plain words work too. |
| `value` | For a `label` rule, the value to record. Otherwise leave it blank. |
| `ignorecase` | `TRUE` to ignore the difference between capital and lower-case letters. |
| `dotall` | `TRUE` for `.` in the pattern to also match line breaks. |
| `note` | Anything you want to say about the rule. courtpy ignores it. |

## The kinds of rules

| Kind | Makes | Example |
| --- | --- | --- |
| `flag` | TRUE if the pattern appears, and otherwise FALSE. | `opinion,criminal_firearm,flag,[Pp]ossession of (a )?[Ff]irearm` |
| `count` | The number of times the pattern appears. | `opinion,habeas_mentions,count,habeas` |
| `matches` | A list of every match. | `opinion,references_case,matches,\d+ (?:F\.(?:[23]d\|4th)\|U\.S\.) \d+` |
| `label` | The rule's `value` if the pattern appears. The first rule for a variable that matches is used. | `court,court_num,label,FIRST CIRCUIT,1` |
| `names` | A list of names, found by splitting the text wherever the pattern matches. The pattern should match what comes between names: titles, filler words, and punctuation. | `judges,panel_judges,names,\b(?:BEFORE\|CIRCUIT\|JUDGES?\|AND)\b\|[,;:]` |
| `section` | A new section of text to search: the first match. | `header,counsel,section,\nCOUNSEL\:.*?(?=\n\n)` |
| `excerpts` | A new section of text to search: every match, one per line. | `header,dates,excerpts,...` |
| `split` | Two new sections, named in `variable`: the text before and after the first match. | `party,"party1, party2",split,\Wv\.\W` |
| `remove` | Nothing: it deletes every match from the target for the rules below it. | `party1,,remove,\bET AL\b` |

## Things to know

* **Rules run from top to bottom.** A rule can search a section made by a rule
  above it (for example, `party1` and `party2` exist only after the `split`
  rule that makes them).
* **Text is put on one line first.** Before the `federal` rules run, every
  section except those whose names end in `_lines` has its line breaks and
  extra spaces replaced with single spaces, so that a phrase broken across two
  lines is still found. (The `lexis_nexis` rules run before this, so they can
  use line breaks, written `\n`.)
* **Names are cleaned.** A `names` rule makes the text upper-case and removes
  periods and apostrophes before splitting it, so write its pattern in
  capital letters.
* **Missing sections are not errors.** If a case has no section for a rule's
  target (such as `future` in a CourtListener case), a flag is FALSE, a count
  is 0, a list is empty, and a label or section is blank.
* **A `section` rule does not replace a section made by a rule above it.** So
  two `section` rules for one variable are a first choice and a fallback: the
  `federal` rules make `panel_text` from the opinion's "Before ..." line,
  and only otherwise from the header's judges.
* **Three sets of variables record the disposition.** `disposition_...` come
  from the header's disposition, `decision_...` from the decision that the
  opinion states ("AFFIRMED.", "VACATED AND REMANDED.", or "we reverse"),
  and `opinion_...` from those words anywhere in the opinion, which may be
  about an earlier decision or a standard of review ("we will reverse only
  if"). The `code_outcome` coder uses the first of the three that a case has.
* **Check your rules** with `courtpy rules my_rules.csv` in a terminal. It
  reports any row that is not a valid rule, by its row number, and lists the
  variables the rules make.
* **Save files as CSV in UTF-8** ("CSV UTF-8" in Excel), so that symbols such as
  `§` are kept.

## Sections of text

These sections can be targets in the `federal` rules (and your own):

| Section | Lexis-Nexis | CourtListener |
| --- | --- | --- |
| `party` | The caption. | The full case name. |
| `party1`, `party2` | Made by the `split` rule in `federal/header.csv`. | The same. |
| `court` | The court's line in the header. | The court's full name. |
| `docket_number` | The docket line. | The docket number (if dockets were downloaded). |
| `citation` | The citation line. | The case's citations. |
| `dates` | Every line with a date. | (Dates come from CourtListener's data.) |
| `history` | "PRIOR HISTORY". | The procedural history and the court appealed from. |
| `future` | "SUBSEQUENT HISTORY". | (None.) |
| `notice` | "NOTICE". | (None: publication comes from CourtListener's precedential status.) |
| `counsel` | "COUNSEL". | The attorneys. |
| `disposition` | "DISPOSITION". | The disposition (often blank). |
| `judges` | "JUDGES". | The judges. |
| `opinion_by` | "OPINION BY". | The authors of the majority (or lead) opinions. |
| `concurring_lines`, `dissenting_lines` | Lines about concurrences and dissents. | The authors of concurrences and dissents. |
| `posture`, `syllabus` | (None.) | CourtListener's posture and syllabus. |
| `opinion` | Everything after the line "OPINION". | The text of every opinion, the majority first. |

The `federal` rules make these sections from the opinion, which your own
rules (below them) can search too:

| Section | Holds |
| --- | --- |
| `opening_lines` | The first 4,000 characters of the opinion (not kept in the table). |
| `caption` | The header's caption, if it names an appellant or appellee. Otherwise, the caption in the opinion's opening, from the first party's role through the second's (such as "Plaintiff-Appellee, v. JOHN DOE, Defendant-Appellant"). |
| `caption1`, `caption2` | The caption before and after "v.", in the order of `party1` and `party2`. |
| `panel_text` | The panel as the opinion's opening names it ("Before SMITH, JONES, and BROWN, Circuit Judges"). Otherwise, the `judges` section. |
| `decision` | The decision as the opinion states it. |
