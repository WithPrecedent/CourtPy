# Reference data

These files were used by the earlier, unreleased CourtPy (built on siMpLify)
to add information about judges and the political context to court cases.
They are kept here, unchanged, as that version left them. They are not part
of the installed package.

The features that used them are rebuilt in `courtpy.judges` and
`courtpy.politics`, which make their tables from the sources' current files
instead (see the "Judges and panels" and "Political context" sections of the
advanced user guide). The raw files here can still be used with them, and
the tables that the earlier version made are useful for checking a study
against it.

| Folder | Contents | Source | Now |
| --- | --- | --- | --- |
| `biographies/federal` | The Federal Judicial Center's files of judges' service, demographics, and careers as of 2019 (`fjc_jud_serv.csv`, `fjc_demo.csv`, and `fjc_career.csv`), the earlier version's table of judges from 1980 to 2016 (`fjc_bios.csv`), their names in every form for each year (`fjc_names.csv`), and Judicial Common Space scores (`jcs.csv`, with the judges' `nid` numbers as `keys` and the scores as `values`). | Federal Judicial Center; Epstein et al. | `courtpy.judges.build_roster` makes a roster from the three raw files (or `Roster.from_fjc` from today's), and its `scores` argument adds the scores. |
| `biographies` | The earlier version's patterns for coding judges' careers (`employ_hist.csv`). | CourtPy | The rules in `src/courtpy/instructions/judges.csv`. |
| `courts/federal` | Names and numbers of federal appellate and trial courts. | CourtPy | The numbers of appellate courts come from the federal rules, and a district court's circuit from its state. |
| `executive/federal` | The president's party by year (`president.csv`). | | `courtpy.politics.presidents`. |
| `judiciary/federal` | Martin-Quinn scores of the Supreme Court by term (`martin_quinn_raw.csv`) and by year from 1980 to 2016 (`martin_quinn.csv`). | mqscores.wustl.edu (then mqscores.lsa.umich.edu) | `courtpy.politics.martin_quinn`, which downloads the current file unless it is given one. |
| `legislature/federal` | DW-NOMINATE scores of every member of Congress (`nominate_raw.csv`) and the medians of each chamber by year from 1980 to 2016 (`nominate.csv`). | voteview.com | `courtpy.politics.nominate`, which downloads the current file unless it is given one. |

For example, to make a roster from the files here:

```python
import pandas as pd

import courtpy

folder = "reference/biographies/federal"
scores = pd.read_csv(f"{folder}/jcs.csv").rename(
    columns = {"keys": "nid", "values": "jcs"})[["nid", "jcs"]]
roster = courtpy.judges.Roster(courtpy.judges.build_roster(
    f"{folder}/fjc_jud_serv.csv", f"{folder}/fjc_demo.csv",
    f"{folder}/fjc_career.csv", scores = scores))
```

## What the earlier version's tables got wrong

The new modules were checked against these tables, and they agree except
where the earlier version was mistaken:

* `fjc_bios.csv` codes a judge as a prosecutor if any position in the judge's
  career has the word "district" (so every law clerk of a district court is
  one), gives the district courts of Vermont to the First Circuit, records a
  missing rating or Senate vote as 0, and gives a judge who was reassigned to
  another court no rating.
* `fjc_names.csv` covers only the years from 1980 to 2015, and the earlier
  version gave a name that several judges shared to whichever of them came
  last in the file.
* `martin_quinn.csv` has 2005 twice, for the two parts of that term.

Most of these sources are updated regularly, so the files here end years ago.
CourtListener's own judge database (with data from the Federal Judicial
Center) can also identify the judges in CourtListener cases: see the
"panel_ids" and "author_id" columns that CourtPy makes, and CourtListener's
"people" bulk files.

The earlier version itself is in this repository's history (the last commit
before the rewrite is `76397af`).
