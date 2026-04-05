"""
Parse ESTC MARC bibliographic and holdings JSON files.

Covers all MARC tags observed across ~481K bib records and ~481K holdings records.
"""

import json
from typing import Any


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _get_subfield(field_value: dict, code: str) -> str | None:
    """Return the first subfield matching `code`, or None."""
    for sf in field_value.get("subfields", []):
        if code in sf:
            return sf[code]
    return None


def _get_all_subfields(field_value: dict, code: str) -> list[str]:
    """Return every subfield value matching `code`."""
    return [sf[code] for sf in field_value.get("subfields", []) if code in sf]


def _join_subfields(field_value: dict, codes: str | None = None) -> str:
    """Join subfield values into a single string.

    If *codes* is given (e.g. ``"abc"``), only those subfield codes are
    included.  Otherwise all subfields are joined.
    """
    parts = []
    for sf in field_value.get("subfields", []):
        for code, value in sf.items():
            if codes is None or code in codes:
                parts.append(value)
    return " ".join(parts).strip()


def _flatten_field(field_value: dict) -> dict[str, Any]:
    """Turn a data field into ``{code: value, ...}``.

    If a subfield code appears more than once the value becomes a list.
    Indicators are stored as ``_ind1`` / ``_ind2``.
    """
    out: dict[str, Any] = {
        "_ind1": field_value.get("ind1", " "),
        "_ind2": field_value.get("ind2", " "),
    }
    for sf in field_value.get("subfields", []):
        for code, value in sf.items():
            if code in out:
                existing = out[code]
                if isinstance(existing, list):
                    existing.append(value)
                else:
                    out[code] = [existing, value]
            else:
                out[code] = value
    return out


def _collect_fields(fields: list[dict], tag: str) -> list:
    """Return every occurrence of *tag* from the MARC fields list.

    Control fields (string values) are returned as-is; data fields are
    returned as the raw dict with ``ind1``, ``ind2``, ``subfields``.
    """
    return [f[tag] for f in fields if tag in f]


def _collect_flat(fields: list[dict], tag: str) -> list[dict]:
    """Collect every occurrence of *tag*, flattened via ``_flatten_field``."""
    return [_flatten_field(v) for v in _collect_fields(fields, tag)
            if isinstance(v, dict)]


def _first_flat(fields: list[dict], tag: str) -> dict | None:
    """Return the first flattened occurrence of *tag*, or None."""
    hits = _collect_flat(fields, tag)
    return hits[0] if hits else None


def _first_control(fields: list[dict], tag: str) -> str | None:
    """Return the first control-field value for *tag*, or None."""
    hits = _collect_fields(fields, tag)
    return hits[0] if hits else None


def _note_texts(fields: list[dict], tag: str) -> list[str]:
    """Collect $a from every instance of a note field."""
    return [
        _get_subfield(f, "a")
        for f in _collect_fields(fields, tag)
        if isinstance(f, dict) and _get_subfield(f, "a")
    ]


# ---------------------------------------------------------------------------
# Name / heading helpers (shared by 100/110/111/700/710/711 etc.)
# ---------------------------------------------------------------------------

def _parse_personal_name(field_value: dict) -> dict:
    return {
        "name": _get_subfield(field_value, "a"),
        "numeration": _get_subfield(field_value, "b"),
        "qualifier": _get_subfield(field_value, "c"),
        "dates": _get_subfield(field_value, "d"),
        "relator": _get_subfield(field_value, "e"),
        "relator_code": _get_subfield(field_value, "4"),
        "title_of_work": _get_subfield(field_value, "t"),
        "fuller_name": _get_subfield(field_value, "q"),
        "misc_info": _get_subfield(field_value, "f"),
    }


def _parse_corporate_name(field_value: dict) -> dict:
    return {
        "name": _get_subfield(field_value, "a"),
        "subordinate": _get_all_subfields(field_value, "b"),
        "location": _get_subfield(field_value, "c"),
        "date": _get_subfield(field_value, "d"),
        "relator": _get_subfield(field_value, "e"),
        "relator_code": _get_subfield(field_value, "4"),
        "title_of_work": _get_subfield(field_value, "t"),
    }


def _parse_meeting_name(field_value: dict) -> dict:
    return {
        "name": _get_subfield(field_value, "a"),
        "location": _get_subfield(field_value, "c"),
        "date": _get_subfield(field_value, "d"),
        "subordinate": _get_subfield(field_value, "e"),
        "number": _get_subfield(field_value, "n"),
    }


# ---------------------------------------------------------------------------
# Subject helpers
# ---------------------------------------------------------------------------

def _parse_subject_common(field_value: dict, name_fields: dict) -> dict:
    """Parse common subject subdivision subfields, merged with *name_fields*."""
    return {
        **name_fields,
        "form": _get_subfield(field_value, "v"),
        "general_subdiv": _get_subfield(field_value, "x"),
        "chronological_subdiv": _get_subfield(field_value, "y"),
        "geographic_subdiv": _get_subfield(field_value, "z"),
        "source": _get_subfield(field_value, "2"),
    }


# ---------------------------------------------------------------------------
# Linking-entry helper (760-787)
# ---------------------------------------------------------------------------

def _parse_linking_entry(field_value: dict) -> dict:
    return {
        "heading": _get_subfield(field_value, "a"),
        "title": _get_subfield(field_value, "t"),
        "edition": _get_subfield(field_value, "b"),
        "qualifier": _get_subfield(field_value, "c"),
        "place_pub_date": _get_subfield(field_value, "d"),
        "issn": _get_subfield(field_value, "s"),
        "control_number": _get_subfield(field_value, "w"),
        "note": _get_subfield(field_value, "n"),
        "relationship": _get_subfield(field_value, "i"),
    }


# ===================================================================
# PUBLIC API
# ===================================================================

def parse_bib_record(path_or_data: str | dict) -> dict[str, Any]:
    """Parse an ESTC bibliographic MARC JSON file.

    Parameters
    ----------
    path_or_data : str or dict
        File path or already-loaded dict.

    Returns
    -------
    dict -- structured record.  Every MARC tag observed in the corpus is
    captured either as a named key or inside ``extra_fields``.
    """
    if isinstance(path_or_data, str):
        with open(path_or_data) as fh:
            data = json.load(fh)
    else:
        data = path_or_data

    fields = data.get("fields", [])

    # =================================================================
    # CONTROL FIELDS
    # =================================================================
    estc_id       = _first_control(fields, "001")
    source_code   = _first_control(fields, "003")
    date_entered  = _first_control(fields, "005")
    fixed_field   = _first_control(fields, "008")
    system_number = _first_control(fields, "009")

    # 035 -- system control numbers
    system_control_numbers = _collect_flat(fields, "035")

    # 040 -- cataloguing source
    cataloguing_source = _first_flat(fields, "040")

    # =================================================================
    # MAIN ENTRY (1XX) -- mutually exclusive in MARC
    # =================================================================
    author_personal  = None
    author_corporate = None
    author_meeting   = None
    for f in _collect_fields(fields, "100"):
        author_personal = _parse_personal_name(f); break
    for f in _collect_fields(fields, "110"):
        author_corporate = _parse_corporate_name(f); break
    for f in _collect_fields(fields, "111"):
        author_meeting = _parse_meeting_name(f); break

    # =================================================================
    # TITLE FIELDS
    # =================================================================
    # 130 -- Uniform title (main entry)
    uniform_title_main = _first_flat(fields, "130")

    # 210 -- Abbreviated title
    abbreviated_title = _first_flat(fields, "210")

    # 240 -- Uniform title
    uniform_title = _first_flat(fields, "240")

    # 243 -- Collective uniform title
    collective_title = _first_flat(fields, "243")

    # 245 -- Title statement
    title = None
    subtitle = None
    statement_of_resp = None
    title_ind1 = None
    title_ind2 = None
    for f245 in _collect_fields(fields, "245"):
        title = _get_subfield(f245, "a")
        subtitle = _get_subfield(f245, "b")
        statement_of_resp = _get_subfield(f245, "c")
        title_ind1 = f245.get("ind1")
        title_ind2 = f245.get("ind2")
        break

    # 246 -- Varying form of title (repeatable)
    varying_titles = _collect_flat(fields, "246")

    # =================================================================
    # EDITION / IMPRINT
    # =================================================================
    # 250 -- Edition statement
    edition = _first_flat(fields, "250")

    # 260 -- Publication (pre-RDA)
    publication = {"place": None, "publisher": None, "date": None}
    for f260 in _collect_fields(fields, "260"):
        publication = {
            "place":     _get_subfield(f260, "a"),
            "publisher": _get_subfield(f260, "b"),
            "date":      _get_subfield(f260, "c"),
            "place_of_manufacture": _get_subfield(f260, "e"),
            "manufacturer":        _get_subfield(f260, "f"),
        }
        break

    # 264 -- Production / Publication / Distribution (RDA)
    production_pub = _collect_flat(fields, "264")

    # =================================================================
    # PHYSICAL DESCRIPTION
    # =================================================================
    # 300 -- Physical description
    physical_desc = {"extent": None, "illustrations": None,
                     "dimensions": None, "accompanying": None}
    for f300 in _collect_fields(fields, "300"):
        physical_desc = {
            "extent":        _get_subfield(f300, "a"),
            "illustrations": _get_subfield(f300, "b"),
            "dimensions":    _get_subfield(f300, "c"),
            "accompanying":  _get_subfield(f300, "e"),
        }
        break

    # 310/321 -- Frequency (serials)
    current_frequency = _first_flat(fields, "310")
    former_frequency  = _first_flat(fields, "321")

    # 336/337/338 -- Content / Media / Carrier type (RDA)
    content_type = _collect_flat(fields, "336")
    media_type   = _collect_flat(fields, "337")
    carrier_type = _collect_flat(fields, "338")

    # 362 -- Dates of publication (serials)
    dates_of_pub = _first_flat(fields, "362")

    # 370 -- Associated place
    associated_place = _first_flat(fields, "370")

    # 388 -- Time period of creation
    time_period = _first_flat(fields, "388")

    # =================================================================
    # LANGUAGE
    # =================================================================
    # 041 -- Language codes
    language_codes = _first_flat(fields, "041")

    # 043 -- Geographic area code
    geographic_area = _first_flat(fields, "043")

    # =================================================================
    # SERIES
    # =================================================================
    # 490 -- Series statement
    series_statements = _collect_flat(fields, "490")

    # 830 -- Series added entry
    series_added = _collect_flat(fields, "830")

    # =================================================================
    # NOTES (5XX)
    # =================================================================
    notes              = _note_texts(fields, "500")   # General
    with_notes         = _note_texts(fields, "501")   # "With" note
    bibliography_notes = _note_texts(fields, "504")   # Bibliography
    contents_notes     = _note_texts(fields, "505")   # Contents
    scale_notes        = _note_texts(fields, "507")   # Scale (maps)
    credits_notes      = _note_texts(fields, "508")   # Credits
    estc_notes         = _collect_flat(fields, "509")  # ESTC-specific (richer)
    numbering_notes    = _note_texts(fields, "515")   # Numbering peculiarities
    summary_notes      = _note_texts(fields, "520")   # Summary
    audience_notes     = _note_texts(fields, "521")   # Target audience
    geographic_notes   = _note_texts(fields, "522")   # Geographic coverage
    supplement_notes   = _note_texts(fields, "525")   # Supplement note
    add_phys_form      = _note_texts(fields, "530")   # Additional physical form
    language_notes     = _note_texts(fields, "546")   # Language
    issuing_body_notes = _note_texts(fields, "550")   # Issuing body
    index_notes        = _note_texts(fields, "555")   # Cumulative index
    linking_notes      = _note_texts(fields, "580")   # Linking entry complexity
    local_notes        = _note_texts(fields, "590")   # Local note
    local_notes_591    = _collect_flat(fields, "591")  # Local note (591)

    # 510 -- References / citations
    references = []
    for f510 in _collect_fields(fields, "510"):
        references.append({
            "source":     _get_subfield(f510, "a"),
            "identifier": _get_subfield(f510, "c"),
            "location":   _get_subfield(f510, "x"),
        })

    # 533 -- Reproduction note
    reproductions = _collect_flat(fields, "533")

    # 534 -- Original version note
    original_version = _collect_flat(fields, "534")

    # 535 -- Location of originals
    location_originals = _note_texts(fields, "535")

    # 539 -- Source of cataloguing (ESTC-specific)
    source_of_cat = _collect_flat(fields, "539")

    # 561 -- Ownership / custodial history
    ownership_hist = _collect_flat(fields, "561")

    # =================================================================
    # SUBJECTS (6XX)
    # =================================================================
    # 600 -- Subject: personal name
    subjects_personal = []
    for f in _collect_fields(fields, "600"):
        subjects_personal.append(
            _parse_subject_common(f, _parse_personal_name(f)))

    # 610 -- Subject: corporate name
    subjects_corporate = []
    for f in _collect_fields(fields, "610"):
        subjects_corporate.append(
            _parse_subject_common(f, _parse_corporate_name(f)))

    # 611 -- Subject: meeting name
    subjects_meeting = []
    for f in _collect_fields(fields, "611"):
        subjects_meeting.append(
            _parse_subject_common(f, _parse_meeting_name(f)))

    # 630 -- Subject: uniform title
    subjects_title = _collect_flat(fields, "630")

    # 648 -- Chronological subject
    era = None
    for f648 in _collect_fields(fields, "648"):
        era = _get_subfield(f648, "a"); break

    # 650 -- Subject: topical
    subjects_topical = []
    for f in _collect_fields(fields, "650"):
        subjects_topical.append(_parse_subject_common(f, {
            "term": _get_subfield(f, "a"),
        }))

    # 651 -- Subject: geographic
    subjects_geographic = []
    for f in _collect_fields(fields, "651"):
        subjects_geographic.append(_parse_subject_common(f, {
            "place": _get_subfield(f, "a"),
        }))

    # 655 -- Genre / form
    genres = []
    for f in _collect_fields(fields, "655"):
        genres.append({
            "term":   _get_subfield(f, "a"),
            "source": _get_subfield(f, "2"),
            "form":   _get_subfield(f, "v"),
            "general_subdiv":      _get_subfield(f, "x"),
            "chronological_subdiv": _get_subfield(f, "y"),
            "geographic_subdiv":   _get_subfield(f, "z"),
        })

    # =================================================================
    # ADDED ENTRIES (7XX)
    # =================================================================
    # 700 -- Added entry: personal
    added_persons = []
    for f in _collect_fields(fields, "700"):
        added_persons.append(_parse_personal_name(f))

    # 710 -- Added entry: corporate
    added_corporates = []
    for f in _collect_fields(fields, "710"):
        added_corporates.append(_parse_corporate_name(f))

    # 711 -- Added entry: meeting
    added_meetings = []
    for f in _collect_fields(fields, "711"):
        added_meetings.append(_parse_meeting_name(f))

    # 730 -- Added entry: uniform title
    added_titles_730 = _collect_flat(fields, "730")

    # 740 -- Added entry: uncontrolled related title
    added_titles_740 = _collect_flat(fields, "740")

    # =================================================================
    # PLACE OF PUBLICATION (752)
    # =================================================================
    places_of_pub = []
    for f752 in _collect_fields(fields, "752"):
        places_of_pub.append({
            "country": _get_subfield(f752, "a"),
            "state":   _get_subfield(f752, "b"),
            "county":  _get_subfield(f752, "c"),
            "city":    _get_subfield(f752, "d"),
        })

    # =================================================================
    # LINKING ENTRIES (76X-78X)
    # =================================================================
    linking_entries = {}
    for tag, label in [
        ("760", "main_series"),
        ("765", "original_language"),
        ("770", "supplement"),
        ("772", "supplement_parent"),
        ("775", "other_edition"),
        ("777", "issued_with"),
        ("780", "preceding"),
        ("785", "succeeding"),
        ("787", "other_relationship"),
    ]:
        entries = [_parse_linking_entry(f)
                   for f in _collect_fields(fields, tag)
                   if isinstance(f, dict)]
        if entries:
            linking_entries[label] = entries

    # =================================================================
    # ELECTRONIC LOCATION (856)
    # =================================================================
    urls = []
    for f856 in _collect_fields(fields, "856"):
        url = _get_subfield(f856, "u")
        labels = _get_all_subfields(f856, "y")
        label = next((l for l in labels if l), None)
        note = _get_subfield(f856, "z")
        if url:
            urls.append({"url": url, "label": label, "note": note})

    # =================================================================
    # ASSEMBLE
    # =================================================================
    return {
        # -- control --
        "estc_id":       estc_id,
        "source_code":   source_code,
        "date_entered":  date_entered,
        "fixed_field":   fixed_field,
        "system_number": system_number,
        "system_control_numbers": system_control_numbers,
        "cataloguing_source":     cataloguing_source,
        "leader": data.get("leader"),

        # -- main entry --
        "author_personal":  author_personal,
        "author_corporate": author_corporate,
        "author_meeting":   author_meeting,

        # -- titles --
        "title":              title,
        "title_ind1":         title_ind1,
        "title_ind2":         title_ind2,
        "subtitle":           subtitle,
        "statement_of_resp":  statement_of_resp,
        "uniform_title_main": uniform_title_main,
        "abbreviated_title":  abbreviated_title,
        "uniform_title":      uniform_title,
        "collective_title":   collective_title,
        "varying_titles":     varying_titles,

        # -- edition / imprint --
        "edition":        edition,
        "publication":    publication,
        "production_pub": production_pub,

        # -- physical --
        "physical_desc":     physical_desc,
        "current_frequency": current_frequency,
        "former_frequency":  former_frequency,
        "content_type":      content_type,
        "media_type":        media_type,
        "carrier_type":      carrier_type,
        "dates_of_pub":      dates_of_pub,
        "associated_place":  associated_place,
        "time_period":       time_period,

        # -- language / geography --
        "language_codes":  language_codes,
        "geographic_area": geographic_area,

        # -- series --
        "series_statements": series_statements,
        "series_added":      series_added,

        # -- notes --
        "notes":              notes,
        "with_notes":         with_notes,
        "bibliography_notes": bibliography_notes,
        "contents_notes":     contents_notes,
        "scale_notes":        scale_notes,
        "credits_notes":      credits_notes,
        "estc_notes":         estc_notes,
        "numbering_notes":    numbering_notes,
        "summary_notes":      summary_notes,
        "audience_notes":     audience_notes,
        "geographic_notes":   geographic_notes,
        "supplement_notes":   supplement_notes,
        "add_phys_form":      add_phys_form,
        "language_notes":     language_notes,
        "issuing_body_notes": issuing_body_notes,
        "index_notes":        index_notes,
        "linking_notes":      linking_notes,
        "local_notes":        local_notes,
        "local_notes_591":    local_notes_591,
        "references":         references,
        "reproductions":      reproductions,
        "original_version":   original_version,
        "location_originals": location_originals,
        "source_of_cat":      source_of_cat,
        "ownership_hist":     ownership_hist,

        # -- subjects --
        "subjects_personal":   subjects_personal,
        "subjects_corporate":  subjects_corporate,
        "subjects_meeting":    subjects_meeting,
        "subjects_title":      subjects_title,
        "subjects_topical":    subjects_topical,
        "subjects_geographic": subjects_geographic,
        "genres":              genres,
        "era":                 era,

        # -- added entries --
        "added_persons":     added_persons,
        "added_corporates":  added_corporates,
        "added_meetings":    added_meetings,
        "added_titles_730":  added_titles_730,
        "added_titles_740":  added_titles_740,

        # -- place --
        "places_of_pub": places_of_pub,

        # -- linking --
        "linking_entries": linking_entries,

        # -- electronic --
        "urls": urls,
    }


def parse_holdings_record(path_or_data: str | dict) -> dict[str, Any]:
    """Parse an ESTC holdings MARC JSON file.

    Parameters
    ----------
    path_or_data : str or dict
        File path or already-loaded dict.

    Returns
    -------
    dict with estc_id, system_number, holdings list, and leader.
    Each holding has: institution_code ($a), institution_name ($b),
    sublocation ($c), former_shelfmark ($d), location ($e),
    shelfmark ($j), provenance ($p), note ($q), record_id ($r),
    verification ($x), public_note ($z).
    """
    if isinstance(path_or_data, str):
        with open(path_or_data) as fh:
            data = json.load(fh)
    else:
        data = path_or_data

    fields = data.get("fields", [])

    estc_id       = _first_control(fields, "001")
    system_number = _first_control(fields, "009")

    holdings = []
    for f852 in _collect_fields(fields, "852"):
        holdings.append({
            "institution_code":  _get_subfield(f852, "a"),
            "institution_name":  _get_subfield(f852, "b"),
            "sublocation":       _get_subfield(f852, "c"),
            "former_shelfmark":  _get_subfield(f852, "d"),
            "location":          _get_subfield(f852, "e"),
            "shelfmark":         _get_subfield(f852, "j"),
            "provenance":        _get_subfield(f852, "p"),
            "note":              _get_subfield(f852, "q"),
            "record_id":         _get_subfield(f852, "r"),
            "verification":      _get_subfield(f852, "x"),
            "public_note":       _get_subfield(f852, "z"),
        })

    return {
        "estc_id":       estc_id,
        "system_number": system_number,
        "holdings":      holdings,
        "leader":        data.get("leader"),
    }


# -----------------------------------------------------------------------
# Quick smoke test
# -----------------------------------------------------------------------
if __name__ == "__main__":
    from pprint import pprint

    bib_json = {
        "fields": [
            {"001": "N454"}, {"003": "CU-RivES"},
            {"008": "830218s1733    enk||||       00||||eng c"},
            {"100": {"ind1": "1", "ind2": " ", "subfields": [
                {"a": "Willoughby de Broke, Richard Verney,"},
                {"c": "Lord,"}, {"d": "1693-1752."}
            ]}},
            {"245": {"ind1": "1", "ind2": "0", "subfields": [
                {"a": "Dunces out of state."},
                {"b": "A poem. Addressed to Mr. Pope."}
            ]}},
            {"260": {"ind1": " ", "ind2": " ", "subfields": [
                {"a": "London :"}, {"b": "printed in the year,"},
                {"c": "1733."}
            ]}},
            {"510": {"ind1": "4", "ind2": " ", "subfields": [
                {"a": "Foxon,"}, {"c": "V28"}
            ]}},
            {"650": {"ind1": " ", "ind2": "0", "subfields": [
                {"a": "Satire"}, {"v": "Early works to 1800."},
                {"x": "English."}
            ]}},
            {"775": {"ind1": " ", "ind2": " ", "subfields": [
                {"a": "Related work"}, {"t": "State dunces"},
                {"w": "(ESTC)T12345"}
            ]}},
        ],
        "leader": "00851cam  2200229   4500"
    }

    hol_json = {
        "fields": [
            {"001": "N454"}, {"009": "006038689"},
            {"852": {"ind1": " ", "ind2": " ", "subfields": [
                {"a": "nMH-H"},
                {"b": "Harvard University, Houghton Library"},
                {"e": "Cambridge, Massachusetts"},
                {"j": "*fEC7.W6848.733d"},
                {"x": "P"}, {"r": "181603"},
                {"z": "Cropped at top margin."},
            ]}},
        ],
        "leader": "00180cam  2200061   4500"
    }

    print("=== Bib (non-empty fields only) ===")
    rec = parse_bib_record(bib_json)
    pprint({k: v for k, v in rec.items() if v})

    print("\n=== Holdings ===")
    pprint(parse_holdings_record(hol_json))