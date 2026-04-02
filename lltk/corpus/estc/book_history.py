"""
Standardize ESTC book_dimensions and book_extent fields.

Functions:
  standardize_format(book_dimensions) → dict with:
    format_std, format_modifier, format_note, format_secondary, format_cm

  parse_extent(book_extent) → dict with:
    num_pages, num_volumes, has_plates, extent_type

Usage:
  from standardize_book_dimensions import standardize_format, parse_extent

  df['fmt'] = df['book_dimensions'].apply(standardize_format)
  df['format_std'] = df['fmt'].apply(lambda x: x['format_std'])

  df['ext'] = df['book_extent'].apply(parse_extent)
  df['num_pages'] = df['ext'].apply(lambda x: x['num_pages'])
"""

import re

# ── Valid bibliographic format numbers ────────────────────────────────
VALID_FORMATS = {1, 2, 4, 6, 8, 12, 16, 18, 24, 32, 36, 48, 64, 128}
VALID_FRACTIONS = {'1/2', '1/3', '1/4', '1/6', '1/8', '1/12', '1/16', 
                   '1/18', '1/24', '1/32', '1/64', '1/72'}

WORD_TO_FORMAT = {
    'broadside': '1⁰', 'broadsheet': '1⁰',
    'folio': '2⁰', 'fol': '2⁰',
    'quarto': '4⁰', '4to': '4⁰',
    'octavo': '8⁰', '8vo': '8⁰',
    'duodecimo': '12⁰', '12mo': '12⁰',
    '16mo': '16⁰', '24mo': '24⁰', '32mo': '32⁰', '48mo': '48⁰', '64mo': '64⁰',
}

def cm_height_to_format(cm):
    if cm >= 40: return '2⁰'
    if cm >= 30: return '2⁰'
    if cm >= 25: return '4⁰'
    if cm >= 20: return '8⁰'
    if cm >= 15: return '12⁰'
    if cm >= 10: return '16⁰'
    return '24⁰'


def _is_cm_pattern(s):
    """Check if string starting with N⁰ is actually N0 cm."""
    # N⁰ cm, N⁰ x M, N⁰-N cm, N⁰cm
    return bool(re.match(r'^\d+\s*⁰\s*(?:cm|x\s*\d|[\-–]\d)', s))


def _extract_cm_value(s):
    """Extract the first cm height from a string, handling ⁰=0 encoding."""
    # First try normal cm: "34 x 21 cm"
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*)?cm', s)
    if m:
        h = float(m.group(1))
        if m.group(2):
            h = max(h, float(m.group(2)))
        return h
    return None


def _extract_format_from_parens(s):
    """Extract a format code from parenthetical, e.g. '(8⁰' or '(folio'."""
    # Fraction format in parens
    m = re.search(r'[\(]\s*(1/\d+)\s*⁰', s)
    if m and m.group(1) in VALID_FRACTIONS:
        return m.group(1) + '⁰'
    # Number format in parens
    m = re.search(r'[\(]\s*(\d+)\s*⁰', s)
    if m and int(m.group(1)) in VALID_FORMATS:
        return m.group(1) + '⁰'
    # obl in parens
    m = re.search(r'[\(]\s*obl[\.\s]*(\d+)\s*⁰', s)
    if m and int(m.group(1)) in VALID_FORMATS:
        return m.group(1) + '⁰'  # caller should also flag obl
    # Word format in parens
    m = re.search(r'[\(]\s*(folio|fol|quarto|octavo|duodecimo|8vo|4to|12mo|16mo|24mo)', s)
    if m:
        w = m.group(1).lower()
        return WORD_TO_FORMAT.get(w, None)
    return None


def standardize_format(raw):
    result = {
        'format_std': None,
        'format_modifier': '',
        'format_note': '',
        'format_secondary': '',
        'format_cm': '',
    }

    if raw is None or (isinstance(raw, float) and raw != raw):
        return result
    
    s = str(raw).strip()
    if not s:
        return result

    original = s

    # ── Fix encoding ──────────────────────────────────────────────
    s = s.replace('�', '⁰').replace('°', '⁰').replace('$⁰', '⁰')
    s = re.sub(r'\\U\+[0-9a-fA-F]+', '⁰', s)

    sl = s.lower().strip()

    # ── Extract modifiers ─────────────────────────────────────────
    modifier = ''
    
    obl_pat = re.match(r'^(obl[\./]?\s*)', sl)
    if obl_pat:
        modifier = 'obl.'
        sl = sl[obl_pat.end():].strip()

    ssh_pat = re.match(r'^s\.sh\.?\s*', sl)
    if ssh_pat:
        modifier = 's.sh.'
        sl = sl[ssh_pat.end():].strip()
        obl_pat2 = re.match(r'^(obl?[\./]?\s*)', sl)
        if obl_pat2:
            modifier = 's.sh.obl.'
            sl = sl[obl_pat2.end():].strip()

    size_pat = re.match(r'^(long|sm\.?|large|nar\.?|square|lon\.?)\s*', sl)
    if size_pat:
        mod_word = size_pat.group(1).rstrip('.')
        if mod_word == 'lon': mod_word = 'long'
        modifier = (modifier + mod_word + '.').strip() if not modifier else (modifier + ' ' + mod_word + '.').strip()
        sl = sl[size_pat.end():].strip()

    # ── Strip annotations ─────────────────────────────────────────
    caption_match = re.match(r'^caption title;\s*', sl)
    if caption_match:
        sl = sl[caption_match.end():].strip()

    sl = re.sub(r'\[?\s*fewer than 5⁰ pages\s*\]?', '', sl).strip()
    sl = sl.rstrip('.')  # trailing periods

    result['format_modifier'] = modifier

    # ── PRIORITY 1: Check for cm patterns where ⁰ = 0 ────────────
    # Strings like "3⁰ cm", "3⁰ x 19 cm", "5⁰ cm (2⁰", "4⁰ cm", "2⁰ cm"
    # If the string contains 'cm', try to decode the full cm value
    if 'cm' in sl:
        # Reconstruct the actual cm values by replacing ⁰ with 0
        sl_fixed = sl.replace('⁰', '0')
        cm_val = _extract_cm_value(sl_fixed)
        if cm_val:
            result['format_cm'] = f"{cm_val:.0f} cm"
        
        # Check if there's an explicit format in parentheses
        fmt_in_parens = _extract_format_from_parens(sl)
        if fmt_in_parens:
            result['format_std'] = fmt_in_parens
            # Check for obl in parens
            if re.search(r'[\(]\s*obl', sl):
                if 'obl' not in modifier:
                    modifier = ('obl. ' + modifier).strip()
                    result['format_modifier'] = modifier
            return result
        
        # Check if there's a format AFTER cm notation like "15 cm. 8⁰"
        after_cm = re.search(r'cm\.?\s+(\d+)\s*⁰', sl)
        if after_cm and int(after_cm.group(1)) in VALID_FORMATS:
            result['format_std'] = after_cm.group(1) + '⁰'
            return result
        after_cm_word = re.search(r'cm\.?\s+(?:[\(]?\s*)(folio|fol|quarto|octavo|duodecimo|8vo|4to|12mo|16mo|24mo)', sl)
        if after_cm_word:
            result['format_std'] = WORD_TO_FORMAT.get(after_cm_word.group(1), None)
            return result
        
        # Leading format before cm: "8⁰ (22 cm" = octavo, 22cm
        leading_fmt = re.match(r'^(1/\d+|\d+)\s*⁰\s*[\(]', sl)
        if leading_fmt:
            val = leading_fmt.group(1)
            if '/' in val and val in VALID_FRACTIONS:
                result['format_std'] = val + '⁰'
                return result
            elif '/' not in val and int(val) in VALID_FORMATS:
                result['format_std'] = val + '⁰'
                return result
        
        # No explicit format found — estimate from cm
        if cm_val:
            result['format_std'] = cm_height_to_format(cm_val)
            return result

    # ── PRIORITY 2: Fraction format (1/2⁰, 1/4⁰, etc.) ──────────
    frac_match = re.match(r'^(1/\d+)\s*⁰', sl)
    if frac_match:
        frac = frac_match.group(1)
        if frac in VALID_FRACTIONS:
            result['format_std'] = frac + '⁰'
        else:
            result['format_std'] = frac + '⁰'  # keep even if unusual
        remainder = sl[frac_match.end():].strip()
        imp = re.search(r"in (\d+)[''s]*", remainder)
        if imp: result['format_note'] = f"in {imp.group(1)}s"
        _extract_secondary(remainder, result)
        return result

    # Pattern: 1/fol, 1/obl.fol
    fol_frac = re.match(r'^1/(obl[\./]?\s*)?fol\.?', sl)
    if fol_frac:
        result['format_std'] = '1/2⁰'
        if fol_frac.group(1) and 'obl' not in modifier:
            result['format_modifier'] = ('obl. ' + modifier).strip()
        return result

    # Pattern: bare 1/2 without degree
    half_match = re.match(r'^1/2\b', sl)
    if half_match and '⁰' not in sl[:8]:
        result['format_std'] = '1/2⁰'
        return result
    # 1/4 without degree
    quarter_match = re.match(r'^1/4\b', sl)
    if quarter_match and '⁰' not in sl[:8]:
        result['format_std'] = '1/4⁰'
        return result
    # 1/2 sh.fol etc
    half_sh = re.match(r'^1/2\s*sh\.?\s*(obl[\./]?\s*)?(fol|4⁰|8⁰)', sl)
    if half_sh:
        result['format_std'] = '1/2⁰'
        return result

    # ── PRIORITY 3: Number + degree sign ──────────────────────────
    num_match = re.match(r'^(\d+)\s*⁰', sl)
    if num_match:
        num = int(num_match.group(1))
        if num in VALID_FORMATS:
            result['format_std'] = str(num) + '⁰'
            remainder = sl[num_match.end():].strip()
            imp = re.search(r"in (\d+)[''s]*", remainder)
            if imp: result['format_note'] = f"in {imp.group(1)}s"
            _extract_secondary(remainder, result)
            return result
        else:
            # Not a valid format — might be junk or a weird value
            # Try treating as cm (N⁰ = N0 cm)
            height = num * 10 if num < 10 else num
            # Only if it's plausible as cm
            if 5 <= height <= 100:
                result['format_cm'] = f"{height} cm"
                result['format_std'] = cm_height_to_format(height)
                return result
            # Otherwise leave unmatched
            return result

    # ── PRIORITY 4: Bare degree sign ──────────────────────────────
    if sl.startswith('⁰'):
        return result  # can't recover

    # ── PRIORITY 5: English words ─────────────────────────────────
    for word, fmt in WORD_TO_FORMAT.items():
        if sl == word or sl.startswith(word + ' ') or sl.startswith(word + '.'):
            result['format_std'] = fmt
            return result
    
    if sl.startswith('broadside') or sl.startswith('broadsheet'):
        result['format_std'] = '1⁰'
        return result

    # ── PRIORITY 6: Bare numbers ──────────────────────────────────
    bare_num = re.match(r'^(\d+)\.?\s*$', sl)
    if bare_num:
        num = int(bare_num.group(1))
        if num in VALID_FORMATS:
            result['format_std'] = str(num) + '⁰'
            return result

    # ── PRIORITY 7: Remaining patterns ────────────────────────────
    # "N D⁰." 
    d_match = re.match(r'^(\d+)\s*d⁰', sl)
    if d_match and int(d_match.group(1)) in VALID_FORMATS:
        result['format_std'] = d_match.group(1) + '⁰'
        return result

    # "duodecimo 12⁰" 
    dup_match = re.match(r'^(folio|quarto|octavo|duodecimo)\s+(\d+)⁰', sl)
    if dup_match:
        result['format_std'] = dup_match.group(2) + '⁰'
        return result

    # pipe-separated: "8vo | 8⁰"
    pipe_match = re.search(r'(\d+)\s*⁰', sl)
    if pipe_match and int(pipe_match.group(1)) in VALID_FORMATS:
        result['format_std'] = pipe_match.group(1) + '⁰'
        return result

    # letter o: "4o" 
    letter_o = re.match(r'^(\d+)o\b', sl)
    if letter_o and int(letter_o.group(1)) in VALID_FORMATS:
        result['format_std'] = letter_o.group(1) + '⁰'
        return result

    # cm ranges without ⁰: "42-46 cm", "32-45 cm"
    cm_range = re.match(r'^(\d+)[\-–](\d+)\s*cm', sl)
    if cm_range:
        h = max(float(cm_range.group(1)), float(cm_range.group(2)))
        result['format_cm'] = cm_range.group(0).strip()
        result['format_std'] = cm_height_to_format(h)
        return result

    # "vertical slip" → broadside-like
    if 'vertical slip' in sl:
        result['format_std'] = '1⁰'
        return result

    # "obl.fol(N" → obl. folio
    if re.match(r'^fol', sl):
        result['format_std'] = '2⁰'
        return result

    # "N mo" with space: "12 mo", "18mo"
    mo_match = re.match(r'^(\d+)\s*mo\b', sl)
    if mo_match and int(mo_match.group(1)) in VALID_FORMATS:
        result['format_std'] = mo_match.group(1) + '⁰'
        return result

    # "4 in 8's", "2 in 6's" — format with imposition
    in_match = re.match(r"^(\d+)\s+in\s+(\d+)[''s]*", sl)
    if in_match and int(in_match.group(1)) in VALID_FORMATS:
        result['format_std'] = in_match.group(1) + '⁰'
        result['format_note'] = f"in {in_match.group(2)}s"
        return result

    # "8v⁰" "4c⁰" — garbled but recoverable
    garble_match = re.match(r'^(\d+)[a-z]\s*⁰', sl)
    if garble_match and int(garble_match.group(1)) in VALID_FORMATS:
        result['format_std'] = garble_match.group(1) + '⁰'
        return result

    # "4 o." — letter o instead of degree  
    letter_o_space = re.match(r'^(\d+)\s+o\.?\s*$', sl)
    if letter_o_space and int(letter_o_space.group(1)) in VALID_FORMATS:
        result['format_std'] = letter_o_space.group(1) + '⁰'
        return result

    # "4to; fol" — mixed formats with semicolon
    mixed_semi = re.match(r'^(folio|fol|quarto|4to|octavo|8vo|duodecimo|12mo)', sl)
    if mixed_semi:
        w = mixed_semi.group(1).lower()
        result['format_std'] = WORD_TO_FORMAT.get(w, None)
        sec = re.search(r';\s*(folio|fol|quarto|4to|octavo|8vo|duodecimo|12mo|\d+⁰)', sl)
        if sec:
            sw = sec.group(1).lower()
            result['format_secondary'] = WORD_TO_FORMAT.get(sw, sw)
        return result

    # "oblsixes" or "sixes" → 6⁰
    if 'sixes' in sl or 'six' in sl:
        result['format_std'] = '6⁰'
        return result

    # "8in4s" → 8⁰ in 4s
    compact_in = re.match(r'^(\d+)in(\d+)s', sl)
    if compact_in and int(compact_in.group(1)) in VALID_FORMATS:
        result['format_std'] = compact_in.group(1) + '⁰'
        result['format_note'] = f"in {compact_in.group(2)}s"
        return result

    # "obl. 1/8." → 1/8⁰
    obl_frac_nodeg = re.match(r'^(1/\d+)\.?\s*$', sl)
    if obl_frac_nodeg:
        result['format_std'] = obl_frac_nodeg.group(1) + '⁰'
        return result

    # "1/4mo", "1/24" without degree
    frac_mo = re.match(r'^(1/\d+)\s*mo\b', sl)
    if frac_mo:
        result['format_std'] = frac_mo.group(1) + '⁰'
        return result
    frac_bare = re.match(r'^(1/\d+)\s*$', sl)
    if frac_bare:
        result['format_std'] = frac_bare.group(1) + '⁰'
        return result

    # mm measurements
    mm_match = re.match(r'^(\d+(?:\.\d+)?)\s*(?:x\s*(\d+(?:\.\d+)?)\s*)?mm', sl)
    if mm_match:
        h = float(mm_match.group(1))
        if mm_match.group(2):
            h = max(h, float(mm_match.group(2)))
        h_cm = h / 10
        result['format_cm'] = f"{h_cm:.1f} cm"
        result['format_std'] = cm_height_to_format(h_cm)
        return result

    # "obl.slip." → broadside
    if 'slip' in sl:
        result['format_std'] = '1⁰'
        return result

    return result


def _extract_secondary(remainder, result):
    sec = re.search(r'(?:and|&|;|\|)\s*(1/\d+|\d+)\s*⁰', remainder)
    if sec:
        val = sec.group(1)
        if '/' in val:
            result['format_secondary'] = val + '⁰'
        elif int(val) in VALID_FORMATS:
            result['format_secondary'] = val + '⁰'
    sec2 = re.search(r'(?:and|&|;)\s*(folio|quarto|octavo|duodecimo)', remainder)
    if sec2 and not result['format_secondary']:
        result['format_secondary'] = WORD_TO_FORMAT.get(sec2.group(1), '')


# ══════════════════════════════════════════════════════════════════════
# Parse book_extent field → page count
# ══════════════════════════════════════════════════════════════════════

ROMAN_MAP = {'i': 1, 'v': 5, 'x': 10, 'l': 50, 'c': 100, 'd': 500, 'm': 1000}

def _roman_to_int(s):
    """Convert a roman numeral string to int. Returns None if invalid."""
    s = s.strip().lower()
    if not s or not all(c in ROMAN_MAP for c in s):
        return None
    total = 0
    prev = 0
    for c in reversed(s):
        val = ROMAN_MAP[c]
        if val < prev:
            total -= val
        else:
            total += val
        prev = val
    if total <= 0 or total > 5000:
        return None
    return total


def parse_extent(raw):
    """
    Parse a book_extent string and return a dict:
      num_pages:     total estimated pages (int or None)
      num_volumes:   number of volumes (int or None)
      has_plates:    whether plates are mentioned (bool)
      extent_type:   'pages' | 'sheet' | 'volumes' | 'leaves' | None

    Examples:
      "8p."                → num_pages=8
      "3,[1]p."            → num_pages=4
      "xi, [1], 216 p."    → num_pages=228
      "[2], 6 p."          → num_pages=8
      "1 sheet ([1] p.)"   → num_pages=1
      "2v."                → num_volumes=2, num_pages=None
    """
    result = {
        'num_pages': None,
        'num_volumes': None,
        'has_plates': False,
        'extent_type': None,
    }

    if not raw or (isinstance(raw, float) and raw != raw):
        return result

    s = str(raw).strip()
    if not s:
        return result

    sl = s.lower()

    result['has_plates'] = bool(re.search(r'plate', sl))

    # ── "No pagination" ───────────────────────────────────────────
    if 'no pagination' in sl:
        return result

    # ── Volumes: "2v.", "3 v.", "2v.,plates" ──────────────────────
    vol_match = re.match(r'^(\d+)\s*v\.', sl)
    if vol_match:
        result['num_volumes'] = int(vol_match.group(1))
        result['extent_type'] = 'volumes'
        paren = re.search(r'\((.+)\)', s)
        if paren:
            pages = _sum_page_segments(paren.group(1))
            if pages:
                result['num_pages'] = pages
        return result

    if sl.startswith('v.') or sl == 'v':
        result['extent_type'] = 'volumes'
        return result

    # ── Sheets: "1 sheet ([1] p.)", "1 sheet", "2 sheets" ────────
    sheet_match = re.match(r'^(\d+)\s*(?:sheet|broadside)s?', sl)
    if sheet_match:
        num_sheets = int(sheet_match.group(1))
        result['extent_type'] = 'sheet'
        paren = re.search(r'\((\[?\d+\]?)\s*(?:\)|\s*p)', sl)
        if paren:
            p = paren.group(1).strip('[]')
            try:
                result['num_pages'] = int(p)
            except ValueError:
                result['num_pages'] = num_sheets * 2
        elif '2 p' in sl or '[2]' in sl:
            result['num_pages'] = 2 * num_sheets
        else:
            result['num_pages'] = num_sheets
        return result

    if sl.startswith('1sheet') or sl.startswith('1 broadside'):
        result['extent_type'] = 'sheet'
        paren = re.search(r'\((\[?\d+\]?)\s*(?:\)|\s*p)', sl)
        if paren:
            p = paren.group(1).strip('[]')
            try:
                result['num_pages'] = int(p)
            except ValueError:
                result['num_pages'] = 1
        else:
            result['num_pages'] = 1
        return result

    # ── Leaves: "2 leaves", "4 leaves" ────────────────────────────
    leaf_match = re.search(r'(\d+)\s*leav', sl)
    if leaf_match and 'p.' not in sl and 'p,' not in sl:
        result['extent_type'] = 'leaves'
        result['num_pages'] = int(leaf_match.group(1)) * 2
        return result

    # ── Standard page sequences ───────────────────────────────────
    result['extent_type'] = 'pages'
    total = _sum_page_segments(s)
    if total and total > 0:
        result['num_pages'] = total

    return result


def _sum_page_segments(s):
    """
    Sum all page-count segments in a string like "xi, [1], 216 p."
    Handles: arabic numbers, [bracketed] numbers, roman numerals.
    """
    total = 0
    found_any = False

    s = re.sub(r'\s*p\.?\s*$', '', s.strip().rstrip(']').rstrip(')'))
    segments = re.split(r'[;,]\s*', s)

    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue

        # Clean stray brackets (common ESTC issue: missing opening [)
        if seg.endswith(']') and '[' not in seg:
            seg = '[' + seg
        if seg.startswith('[') and ']' not in seg:
            seg = seg + ']'

        # Skip non-page tokens
        if any(skip in seg.lower() for skip in ['plate', 'map', 'table', 'port',
                                                  'leaf', 'leav', 'fold', 'chart',
                                                  'ill', 'front', 'genealog']):
            continue

        # Bracketed number: [1], [2], [48]
        brack = re.match(r'^\[(\d+)\]$', seg)
        if brack:
            total += int(brack.group(1))
            found_any = True
            continue

        # Plain arabic number: 216, 8, 48
        arabic = re.match(r'^(\d+)$', seg)
        if arabic:
            total += int(arabic.group(1))
            found_any = True
            continue

        # Arabic with trailing p: "216 p", "8p"
        arabic_p = re.match(r'^(\d+)\s*p\.?$', seg)
        if arabic_p:
            total += int(arabic_p.group(1))
            found_any = True
            continue

        # Bracketed with p: "[48] p"
        brack_p = re.match(r'^\[(\d+)\]\s*p\.?$', seg)
        if brack_p:
            total += int(brack_p.group(1))
            found_any = True
            continue

        # Roman numeral: "xi", "viii", "xlviii"
        cleaned = seg.strip('[]').strip()
        roman_val = _roman_to_int(cleaned)
        if roman_val:
            total += roman_val
            found_any = True
            continue

        # Compound: "216 p." embedded in a larger string
        num_in_seg = re.search(r'(\d+)\s*p', seg)
        if num_in_seg:
            total += int(num_in_seg.group(1))
            found_any = True
            continue

        # Bracketed roman: "[xii]"
        brack_roman = re.match(r'^\[([ivxlcdm]+)\]$', seg.lower())
        if brack_roman:
            rv = _roman_to_int(brack_roman.group(1))
            if rv:
                total += rv
                found_any = True
                continue

    return total if found_any else None









"""
Classify ESTC records as fiction based on form and subject_topic fields.

Usage:
    from classify_fiction import is_fiction
    df['marked_as_fiction'] = df.apply(
        lambda r: is_fiction(r['form'], r['subject_topic']), axis=1
    )
"""

# ── Keywords that strongly indicate fiction ────────────────────────────
# These are checked against individual pipe-separated segments after lowercasing.

FICTION_FORM_EXACT = {
    # Exact matches (after lowercasing and stripping)
    'fiction',
    'novels',
    'epistolary novels',
    'epistolary fiction',
    'fables',
    # 'chapbooks',
    'novellas',
    'adventure stories',
    'utopian literature',
    'utopian literature.',
    'picaresque fiction',
    'gothic fiction',
    'gothic novels',
    'historical fiction',
    'biographical fiction',
    'fantasy literature',
    'mystery and detective fiction',
    'nursery stories',
    'harlequinades',
    'fabliaux',
    'fairy tales',
    'imaginary voyages',
    'imaginary conversations',
}

FICTION_FORM_KEYWORDS = [
    # Substring matches for form field
    'fiction',
    'novel',
    "romance",
    'tale',
]

FICTION_TOPIC_KEYWORDS = [
    # Substring matches for subject_topic field
    'fiction',              # "English fiction", "French fiction", "Epistolary fiction"
    'imaginary voyage',     # "Voyages, Imaginary"
    'voyages, imaginary',
    'utopia',              # "Utopias"
    'fairy tale',
    'fables, english',
    'fables, latin',
    'fables, french',
    # 'chapbooks, english',
    # 'chapbooks, scottish',
    'children\'s stories',
    'robinsonade',
    'romances',
]

FICTION_TOPIC_EXACT = {
    'english fiction',
    'french fiction',
    'fables',
    'utopias',
    'fairy tales',
    # 'chapbooks',
}


def is_fiction(form, subject_topic):
    """
    Return True if the record's form or subject_topic indicates fiction.
    
    Checks both pipe-separated segments and substring keywords.
    Conservative: only flags things that are clearly fictional prose/narrative.
    Does NOT flag: poetry, drama, satire, ballads (use genre terms for those).
    """
    # ── Check form field ──────────────────────────────────────────
    if _check_field(form, FICTION_FORM_EXACT, FICTION_FORM_KEYWORDS):
        return True
    
    # ── Check subject_topic field ─────────────────────────────────
    if _check_field(subject_topic, FICTION_TOPIC_EXACT, FICTION_TOPIC_KEYWORDS):
        return True
    
    return False


def _check_field(value, exact_set, keyword_list):
    """Check a pipe-separated field against exact matches and keyword substrings."""
    if not value or (isinstance(value, float) and value != value):
        return False
    
    s = str(value).lower().strip()
    if not s:
        return False
    
    # Split on pipe and check each segment
    segments = [seg.strip() for seg in s.split('|')]
    
    for seg in segments:
        # Exact match
        if seg in exact_set:
            return True
        
        # Keyword substring match
        for kw in keyword_list:
            if kw in seg:
                return True
    
    return False


# # ── Quick test ────────────────────────────────────────────────────────
# if __name__ == '__main__':
#     tests = [
#         # (form, topic, expected)
#         ('Fiction', '', True),
#         ('Novels', '', True),
#         ('Epistolary novels', '', True),
#         ('Epistolary Novels', '', True),  # case insensitive
#         ('Poems | Fiction', '', True),    # pipe-separated
#         ('Sermons', '', False),
#         ('Plays', '', False),             # drama excluded
#         ('Satires', '', False),           # satire excluded
#         ('', 'English fiction', True),
#         ('', 'French fiction', True),
#         ('', 'Epistolary fiction, English', True),
#         ('', 'Voyages, Imaginary', True),
#         ('', 'Utopias', True),
#         ('', 'Fables', True),
#         ('', 'Fables, English', True),
#         ('', 'Chapbooks, English', True),
#         ('', "Children's stories", True),
#         ('', 'English poetry', False),
#         ('', 'Christianity', False),
#         ('', 'Medicine', False),
#         ('', 'Sermons, English', False),
#         ('', 'English drama', False),     # drama excluded
#         ('', 'English drama (Comedy', False),
#         ('Gothic fiction', 'English fiction', True),
#         ('Chapbooks', '', True),
#         ('Adventure stories', '', True),
#         ('', '', False),
#         (None, None, False),
#         ('Fables', '', True),
#         ('', 'Murder', False),            # true crime, not fiction
#         ('', 'Courtship', False),         # theme, not fiction
#         ('', 'Man-woman relationships', False),
#         ('Ballads', '', False),           # not flagged as fiction
#         ('', 'Robin Hood (Legendary character', False),  # character, not fiction genre
#     ]
    
#     all_ok = True
#     for form, topic, expected in tests:
#         got = is_fiction(form, topic)
#         ok = got == expected
#         if not ok:
#             all_ok = False
#             print(f"  FAIL: form={form!r}, topic={topic!r} → expected {expected}, got {got}")
    
#     if all_ok:
#         print(f"All {len(tests)} tests passed")
#     else:
#         print(f"\nSome tests failed")