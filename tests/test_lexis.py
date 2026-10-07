"""Tests the lexis module and parsing Lexis-Nexis cases."""

from __future__ import annotations

import pathlib

import courtpy
from courtpy import lexis


def test_split(lexis_folder: pathlib.Path) -> None:
    files = sorted(lexis_folder.glob('*.txt'))
    assert [f.name for f in files] == ['lexis_batch_00001.txt', 'lexis_batch_00002.txt']
    text = files[0].read_text(encoding = 'utf-8')
    assert text.startswith('UNITED STATES OF AMERICA')
    assert '[101]' not in text and '*' not in text


def test_clean() -> None:
    text = 'Signal: Caution\nAs of: Jan 1\n\nA  *12  case [3] here \r\n  next'
    assert lexis.clean(text) == 'A 12 case here\nnext'


def test_read_case(lexis_folder: pathlib.Path) -> None:
    case = lexis.read_case(lexis_folder / 'lexis_batch_00001.txt')
    assert case.id == 'lexis_batch_00001'
    assert case.sections['text'].startswith('\nUNITED STATES')
    assert case.metadata == {'source': 'lexis_nexis', 'file': 'lexis_batch_00001.txt'}


def test_parse(lexis_folder: pathlib.Path) -> None:
    table = courtpy.parse(lexis_folder, source = 'lexis_nexis')
    doe = table.loc['lexis_batch_00001']
    assert doe['party1'] == 'UNITED STATES OF AMERICA, Plaintiff, Appellee,'
    assert doe['party2'] == 'JOHN DOE, Defendant, Appellant.'
    assert doe['court_num'] == 1
    assert doe['docket_number'] == 'No. 08-1234'
    assert doe['docket_numbers'] == ['08-1234']
    assert doe['date_argued'] == '2009-03-03'
    assert doe['date_decided'] == '2009-06-01'
    assert doe['date_filed'] == '2009-06-01'
    assert doe['year'] == 2009
    assert doe['published']
    assert doe['panel_judges'] == ['TORRUELLA', 'LYNCH', 'HOWARD']
    assert doe['panel_size'] == 3
    assert doe['author'] == 'LYNCH'
    assert doe['dissenting'] == ['HOWARD'] and doe['dissents'] == 1
    assert doe['concurrences'] == 0
    assert doe['counsel_us_attorney'] and doe['counsel_public_defender']
    assert doe['party1_united_states'] and doe['party2_appellant']
    assert doe['criminal_firearm'] and doe['general_amend4'] and doe['standard_de_novo']
    assert doe['opinion_reverse'] and not doe['disposition_reverse']
    assert doe['references_case'] == ['400 F.3d 10']
    assert doe['word_count'] > 50
    assert 'opinion' not in table.columns and 'header' not in table.columns
    acme = table.loc['lexis_batch_00002']
    assert acme['court_num'] == 9
    assert acme['agency'] == 'NATIONAL LABOR RELATIONS BOARD'
    assert acme['disposition'] == 'DISPOSITION: PETITION DENIED.'
    assert acme['disposition_deny']
    assert acme['notice_unpublished'] and not acme['published']
    assert acme['date_submitted'] == '2008-01-10' and acme['date_filed'] == '2008-02-02'
    assert acme['panel_judges'] == ['SCHROEDER', 'OSCANNLAIN', 'BYBEE']
    assert acme['party1_petitioner'] and acme['party2_respondent']
    assert acme['civil_labor']


def test_keep_text(lexis_folder: pathlib.Path) -> None:
    table = courtpy.parse(lexis_folder, source = 'lexis_nexis', keep_text = True, limit = 1)
    assert len(table) == 1
    assert table.iloc[0]['opinion'].startswith('LYNCH, Circuit Judge.')
    assert 'COUNSEL:' in table.iloc[0]['header']
