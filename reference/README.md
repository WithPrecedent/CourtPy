# Reference data

These files were used by the earlier, unreleased CourtPy (built on siMpLify)
to add information about judges and the political context to court cases.
They are kept here, unchanged, until those features are rebuilt on the new
version. They are not part of the installed package.

| Folder | Contents | Source |
| --- | --- | --- |
| `biographies/federal` | Federal judges' biographies, careers, demographics, and service (`fjc_*.csv`), their names in every form for matching (`fjc_names.csv`), and Judicial Common Space scores (`jcs.csv`). | Federal Judicial Center; Epstein et al. |
| `biographies` | Patterns for coding judges' prior employment (`employ_hist.csv`). | CourtPy |
| `courts/federal` | Names and numbers of federal appellate and trial courts. | CourtPy |
| `executive/federal` | The president's party by year. | |
| `judiciary/federal` | Martin-Quinn scores of the Supreme Court by term. | mqscores.lsa.umich.edu |
| `legislature/federal` | DW-NOMINATE scores of Congress (raw and by year). | voteview.com |

Most of these sources are updated regularly, so the files here end years ago
(the scores by year end in 2016). CourtListener's own judge database (with
data from the Federal Judicial Center) can now identify the judges in
CourtListener cases: see the "panel_ids" and "author_id" columns that CourtPy
makes, and CourtListener's "people" bulk files.
