"""Tests the rules module."""

from __future__ import annotations

import pathlib
import re

import pytest

from courtpy import rules


def make(**cells: str) -> rules.Rule:
    row = {'ignorecase': 'TRUE', 'dotall': 'FALSE', 'value': '', **cells}
    return rules.Rule.create(row)


def apply(rulebook: list[rules.Rule], sections: dict[str, str]) -> dict:
    return rules.Rulebook(rules = rulebook).apply(sections)


def test_flag_count_and_matches() -> None:
    book = [
        make(target = 'opinion', variable = 'habeas', kind = 'flag', pattern = 'habeas'),
        make(target = 'opinion', variable = 'jury', kind = 'flag', pattern = 'jury'),
        make(target = 'opinion', variable = 'cites', kind = 'count', pattern = r'\d+ U\.S\. \d+'),
        make(target = 'opinion', variable = 'refs', kind = 'matches', pattern = r'\d+ U\.S\. \d+'),
        make(target = 'opinion', variable = 'refs', kind = 'matches', pattern = r'\d+ F\.3d \d+')]
    found = apply(book, {'opinion': 'A HABEAS case. See 1 U.S. 2, 3 U.S. 4, and 5 F.3d 6.'})
    assert found == {
        'habeas': True, 'jury': False, 'cites': 2,
        'refs': ['1 U.S. 2', '3 U.S. 4', '5 F.3d 6']}


def test_missing_targets_give_empty_values() -> None:
    book = [
        make(target = 'notice', variable = 'unpublished', kind = 'flag', pattern = 'x'),
        make(target = 'notice', variable = 'n', kind = 'count', pattern = 'x'),
        make(target = 'notice', variable = 'm', kind = 'matches', pattern = 'x'),
        make(target = 'notice', variable = 'l', kind = 'label', pattern = 'x', value = '1'),
        make(target = 'notice', variable = 's', kind = 'section', pattern = 'x'),
        make(target = 'notice', variable = 'names', kind = 'names', pattern = ',')]
    assert apply(book, {}) == {
        'unpublished': False, 'n': 0, 'm': [], 'l': None, 's': None, 'names': []}


def test_label_uses_first_match_and_converts_numbers() -> None:
    book = [
        make(target = 'court', variable = 'court_num', kind = 'label', pattern = 'FIRST CIRCUIT', value = '1'),
        make(target = 'court', variable = 'court_num', kind = 'label', pattern = 'CIRCUIT', value = 'other')]
    assert apply(book, {'court': 'Court of Appeals for the First Circuit'})['court_num'] == 1
    assert apply(book, {'court': 'Second Circuit'})['court_num'] == 'other'
    assert apply(book, {'court': 'District Court'})['court_num'] is None


def test_label_searches_every_target() -> None:
    rule = make(
        target = 'history, party1', variable = 'agency', kind = 'label',
        pattern = 'LABOR RELATIONS', value = 'NLRB')
    assert apply([rule], {'party1': 'National Labor Relations Board'}) == {'agency': 'NLRB'}


def test_split_section_and_excerpts_make_new_sections() -> None:
    book = [
        make(target = 'text', variable = 'header, opinion', kind = 'split', pattern = r'\nOPINION\n'),
        make(target = 'header', variable = 'judges', kind = 'section', pattern = r'JUDGES:[^\n]*'),
        make(target = 'header', variable = 'dates', kind = 'excerpts', pattern = r'^\w+ \d+, \d{4}.*$',
             dotall = 'FALSE'),
        make(target = 'judges', variable = 'panel', kind = 'names', pattern = r'\bJUDGES\b|:|,|\bAND\b')]
    rulebook = rules.Rulebook(rules = book)
    sections = {'text': 'A v. B\nJudges: Smith, Jones and Brown\nMarch 3, 2009, Argued\nOPINION\nWe affirm.'}
    found = rulebook.apply(sections)
    assert sections['opinion'] == 'We affirm.'
    assert found['judges'] == 'Judges: Smith, Jones and Brown'
    assert found['panel'] == ['SMITH', 'JONES', 'BROWN']
    assert rulebook.sections == ['header', 'opinion', 'judges', 'dates']
    assert rulebook.variables == ['header', 'opinion', 'judges', 'dates', 'panel']


def test_dates_excerpts_use_multiline_anchors() -> None:
    rule = rules.Rule.create({
        'target': 'header', 'variable': 'dates', 'kind': 'excerpts',
        'pattern': r'(?<=\n)\w+ \d+, \d{4}.*?(?=\n)', 'ignorecase': 'TRUE',
        'dotall': 'FALSE'})
    found = apply([rule], {'header': '\nMarch 3, 2009, Argued\nJune 1, 2009, Decided\n'})
    assert found['dates'] == 'March 3, 2009, Argued\nJune 1, 2009, Decided'


def test_split_without_match_keeps_everything_first() -> None:
    rule = make(target = 'party', variable = 'party1, party2', kind = 'split', pattern = r'\Wv\.\W')
    assert apply([rule], {'party': 'In re Smith'}) == {'party1': 'In re Smith', 'party2': None}
    assert apply([rule], {'party': 'Smith v. Jones'}) == {'party1': 'Smith', 'party2': 'Jones'}


def test_remove_changes_later_rules_only() -> None:
    book = [
        make(target = 'party1', variable = 'defendant', kind = 'flag', pattern = 'DEFENDANT'),
        make(target = 'party1', kind = 'remove', variable = '', pattern = 'DEFENDANT'),
        make(target = 'party1', variable = 'still', kind = 'flag', pattern = 'DEFENDANT')]
    assert apply(book, {'party1': 'Doe, Defendant'}) == {'defendant': True, 'still': False}


def test_split_names() -> None:
    separators = re.compile(r"\b(?:BEFORE|CIRCUIT|JUDGES?|AND)\b|[,:]")
    names = rules.split_names(
        "Before: O'Scannlain, Kirby, and HOLLAND, Circuit Judges.\nBybee", separators)
    assert names == ['OSCANNLAIN', 'KIRBY', 'HOLLAND', 'BYBEE']


def test_names_ignores_groups_in_pattern() -> None:
    rule = make(target = 'j', variable = 'names', kind = 'names', pattern = r'(,)|(\bAND\b)')
    assert apply([rule], {'j': 'Smith, Jones and Brown'}) == {'names': ['SMITH', 'JONES', 'BROWN']}


@pytest.mark.parametrize(('cells', 'message'), [
    ({'kind': 'nope'}, 'is not one of'),
    ({'pattern': '('}, 'is not valid'),
    ({'pattern': ''}, 'pattern is blank'),
    ({'target': ''}, 'target is blank'),
    ({'kind': 'split', 'variable': 'one'}, 'needs two variables'),
    ({'ignorecase': 'maybe'}, 'is not TRUE or FALSE')])
def test_invalid_rules(cells: dict[str, str], message: str) -> None:
    row = {'target': 'opinion', 'variable': 'x', 'kind': 'flag', 'pattern': 'x',
           'ignorecase': 'TRUE', 'dotall': 'FALSE', **cells}
    with pytest.raises(ValueError, match = message):
        rules.Rule.create(row)


def test_load_csv_from_excel(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'mine.csv'
    path.write_text(
        '﻿Target,Variable,Kind,Pattern,Value,IgnoreCase,DotAll,Note\n'
        'opinion,my_flag,flag,"a, b",,true,false,anything\n\n',
        encoding = 'utf-8')
    rulebook = rules.Rulebook.load(path)
    assert len(rulebook) == 1
    assert rulebook.rules[0].pattern.pattern == 'a, b'
    assert rulebook.rules[0].note == 'anything'


def test_load_reports_file_and_row(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'bad.csv'
    path.write_text(
        'target,variable,kind,pattern,ignorecase,dotall\n'
        'opinion,ok,flag,x,TRUE,FALSE\n'
        'opinion,bad,flag,(,TRUE,FALSE\n', encoding = 'utf-8')
    with pytest.raises(ValueError, match = r'bad\.csv, row 3'):
        rules.Rulebook.load(path)
    missing = tmp_path / 'missing.csv'
    missing.write_text('target,variable\n', encoding = 'utf-8')
    with pytest.raises(ValueError, match = 'missing the columns'):
        rules.Rulebook.load(missing)


def test_find_instructions() -> None:
    assert [p.name for p in rules.find_instructions('federal')] == ['header.csv', 'opinion.csv']
    assert rules.find_instructions('lexis_nexis')[0].name == 'lexis_nexis.csv'
    with pytest.raises(FileNotFoundError, match = 'built-in'):
        rules.find_instructions('no_such_rules')


def test_folder_of_rules(tmp_path: pathlib.Path) -> None:
    for name, variable in (('b.csv', 'second'), ('a.csv', 'first')):
        (tmp_path / name).write_text(
            f'target,variable,kind,pattern,ignorecase,dotall\nopinion,{variable},flag,x,TRUE,FALSE\n',
            encoding = 'utf-8')
    assert rules.Rulebook.load(tmp_path).variables == ['first', 'second']


def test_built_in_rulebooks_are_valid() -> None:
    federal = rules.Rulebook.load('federal')
    lexis = rules.Rulebook.load('lexis_nexis')
    assert len(federal) > 300
    for variable in (
        'party1', 'party2', 'court_num', 'agency', 'panel_judges', 'authors',
        'concurring', 'dissenting', 'disposition_reverse', 'opinion_reverse',
        'criminal_firearm', 'civil_titlevii', 'general_amend4',
        'references_case', 'references_statute', 'cite_federal_reporter',
        'party1_appellant', 'party2_united_states', 'counsel_us_attorney',
        'docket_criminal', 'notice_unpublished'):
        assert variable in federal.variables
    assert lexis.sections == [
        'header', 'opinion', 'party', 'court', 'docket_number', 'history',
        'future', 'counsel', 'notice', 'dates', 'disposition', 'citation',
        'judges', 'opinion_by', 'concurring_lines', 'dissenting_lines']
    combined = federal + lexis
    assert len(combined) == len(federal) + len(lexis)
