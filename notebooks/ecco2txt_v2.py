# %% [markdown]
# # ECCO XML → Plain Text (lxml)
#
# Parse Gale/ECCO XML files into structured line records using lxml.
# Handles `.xml.gz`, `.xml`, or an already-parsed lxml Element.

# %%
from __future__ import annotations
import gzip
import re
from typing import Union
from lxml import etree

# %% [markdown]
# ## Core parser
#
# The fundamental representation is a flat list of line dicts:
# ```
# {'page_num': 1, 'page_type': 'bodyPage', 'page_ocr': 84.1,
#  'para_num': 0, 'line_num': 0, 'line_txt': 'THen this FlouriOiing Iflaad'}
# ```
# Everything else (plain text, DataFrame, filtering) builds on this.

# %%
def parse_ecco_xml(source: Union[str, etree._Element]) -> etree._Element:
    """Accept an xml.gz path, xml path, or lxml Element. Return the root Element."""
    if isinstance(source, etree._Element):
        return source
    path = str(source)
    if path.endswith('.gz'):
        with gzip.open(path, 'rb') as f:
            return etree.fromstring(f.read())
    else:
        return etree.parse(path).getroot()


def ecco_lines(
    source: Union[str, etree._Element],
    page_types: set[str] | None = None,
) -> list[dict]:
    """Parse ECCO XML into a flat list of line records.

    Each record: page_num, page_type, page_ocr, para_num, line_num, line_txt.

    Line breaks detected via y-coordinate jumps (>20px) in the ``pos``
    attribute on ``<wd>`` tags.  Paragraph breaks from ``<p>`` parent changes.
    """
    if page_types is None:
        page_types = {'bodyPage'}
    root = parse_ecco_xml(source)
    body = root.find('.//text')
    if body is None:
        return []

    lines = []
    for page_el in body.iterfind('.//page'):
        page_type = page_el.get('type', '')
        if page_type not in page_types:
            continue

        sp_el = page_el.find('pageInfo/sourcePage')
        ocr_el = page_el.find('pageInfo/ocr')
        page_num = int(sp_el.text) if sp_el is not None and sp_el.text.isdigit() else None
        page_ocr = float(ocr_el.text) if ocr_el is not None else None

        para_num = 0
        line_num = 0
        line_words = []
        last_parent = None
        last_y = None

        def _flush_line():
            nonlocal line_words, line_num
            if line_words:
                lines.append({
                    'page_num': page_num,
                    'page_type': page_type,
                    'page_ocr': page_ocr,
                    'para_num': para_num,
                    'line_num': line_num,
                    'line_txt': ' '.join(line_words),
                })
                line_words = []
                line_num += 1

        for wd in page_el.iter('wd'):
            parent = wd.getparent()
            if parent is not last_parent:
                _flush_line()
                if last_parent is not None:
                    para_num += 1
                    line_num = 0
            last_parent = parent

            pos = wd.get('pos', '')
            if pos:
                try:
                    y = int(pos.split(',')[1])
                    if last_y is not None and y - last_y > 20:
                        _flush_line()
                    last_y = y
                except (IndexError, ValueError):
                    pass

            line_words.append(wd.text or '')

        _flush_line()

    return lines

# %% [markdown]
# ## Convenience: plain text and DataFrame

# %%
def ecco_xml2txt(
    source: Union[str, etree._Element],
    page_types: set[str] | None = None,
    remove_catchwords: bool = True,
) -> str:
    """Convert ECCO XML to plain text.

    ``\\n\\n\\n`` between pages, ``\\n\\n`` between paragraphs, ``\\n`` between lines.
    """
    lines = ecco_lines(source, page_types=page_types)
    if not lines:
        return ''

    if remove_catchwords:
        lines = _remove_catchwords(lines)

    parts = []
    prev_page = None
    prev_para = None
    for rec in lines:
        if prev_page is not None and rec['page_num'] != prev_page:
            parts.append('\n\n\n')
        elif prev_para is not None and rec['para_num'] != prev_para:
            parts.append('\n\n')
        elif parts:
            parts.append('\n')
        parts.append(rec['line_txt'])
        prev_page = rec['page_num']
        prev_para = rec['para_num']

    plain = ''.join(parts)
    plain = _fix_dangling_hyphens(plain)
    plain = re.sub(r'(?m)^\s*\(\d+\)\s*$', '', plain)
    return plain


def ecco_df(source: Union[str, etree._Element], **kwargs):
    """Parse ECCO XML into a pandas DataFrame of line records."""
    import pandas as pd
    return pd.DataFrame(ecco_lines(source, **kwargs))

# %% [markdown]
# ## Post-processing

# %%
def _remove_catchwords(lines: list[dict]) -> list[dict]:
    """Remove catchwords: last word of page N == first word of page N+1."""
    if not lines:
        return lines

    result = []
    page_groups = []
    current_page = lines[0]['page_num']
    current_group = []

    for rec in lines:
        if rec['page_num'] != current_page:
            page_groups.append(current_group)
            current_group = []
            current_page = rec['page_num']
        current_group.append(rec)
    page_groups.append(current_group)

    for i, group in enumerate(page_groups):
        if i + 1 < len(page_groups) and group and page_groups[i + 1]:
            last_line = group[-1]['line_txt']
            first_line = page_groups[i + 1][0]['line_txt']
            last_word = last_line.split()[-1] if last_line.split() else ''
            first_word = first_line.split()[0] if first_line.split() else ''
            if last_word and last_word == first_word:
                remaining = ' '.join(last_line.split()[:-1])
                if remaining:
                    group[-1] = {**group[-1], 'line_txt': remaining}
                else:
                    group = group[:-1]
        result.extend(group)

    return result


def _fix_dangling_hyphens(text, hyphens=frozenset({'¬', '-'})):
    """Rejoin words broken across lines with a hyphen or negation obelus."""
    lines = [l.rstrip() for l in text.split('\n')]
    for i, line in enumerate(lines):
        if i + 1 >= len(lines) or not lines[i + 1]:
            continue
        for hyph in hyphens:
            if line.endswith(hyph):
                next_words = lines[i + 1].split()
                if next_words:
                    lines[i] = line[:-1] + next_words[0]
                    lines[i + 1] = ' '.join(next_words[1:])
                break
    return '\n'.join(lines)

# %% [markdown]
# ## Demo

# %%
if __name__ == '__main__':
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else '/Users/rj416/lltk_data/corpora/ecco/xml/LitAndLang1/0000200100.xml.gz'

    lines = ecco_lines(path)
    print(f"{len(lines)} lines from {len(set(r['page_num'] for r in lines if r['page_num']))} pages\n")

    for rec in lines[:10]:
        print(f"  p{rec['page_num'] or '?':>3}  para={rec['para_num']}  line={rec['line_num']}  "
              f"OCR={rec['page_ocr']}  {rec['line_txt'][:80]}")

    print(f"\n--- plain text (first 500 chars) ---\n")
    print(ecco_xml2txt(path)[:500])

# %%
