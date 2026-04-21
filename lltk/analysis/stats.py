"""
lltk.analysis.stats — generic statistical primitives for corpus discrimination.

Fully passage-ignorant: callers supply pre-built boolean DataFrames and
receive tidy result DataFrames. No knowledge of tasks, schemas, or CH tables.

Requires: pip install lltk-dh[analysis]  (adds scipy)
"""


def group_matrix(long_df, *, group_col, member_col, value_col=None):
    """Pivot a long DataFrame into a boolean member × group matrix.

    Args:
        long_df:    DataFrame with at least group_col and member_col columns.
        group_col:  Column whose unique values become matrix columns.
        member_col: Column whose unique values become matrix rows (the index).
        value_col:  Optional boolean/numeric column to use as cell value.
                    If None, presence of (member, group) pair → True.

    Returns:
        bool DataFrame, rows=unique members, cols=unique groups, NaN → False.
    """
    import pandas as pd

    if value_col is not None:
        mat = long_df.pivot_table(
            index=member_col, columns=group_col, values=value_col,
            aggfunc='max', fill_value=False,
        )
    else:
        long_df = long_df[[member_col, group_col]].drop_duplicates()
        long_df = long_df.assign(_present=True)
        mat = long_df.pivot_table(
            index=member_col, columns=group_col, values='_present',
            aggfunc='max', fill_value=False,
        )

    mat.columns.name = None
    mat.index.name = member_col
    return mat.astype(bool)


def fisher_tests(
    feature_matrix,
    group_matrix,
    *,
    min_group_n=30,
    min_feature_n=20,
    include_feature_pairs=False,
    cross_task_pairs_only=True,
):
    """Run Fisher exact tests for every (group, feature) pair.

    Args:
        feature_matrix:       bool DataFrame, rows=observations, cols=features.
        group_matrix:         bool DataFrame, rows=observations, cols=groups.
                              Must share the same row index as feature_matrix.
        min_group_n:          Skip groups with fewer True rows than this.
        min_feature_n:        Skip features with fewer True rows than this.
        include_feature_pairs: If True, also test every (feature_a, feature_b)
                              pair treating one feature as the 'group'.
        cross_task_pairs_only: When include_feature_pairs=True, only include
                              pairs where cols come from different task prefixes
                              (prefix = text before first ':' in column name).

    Returns:
        DataFrame with columns: group, feature, a_group_feat, b_group_nofeat,
        c_nogroup_feat, d_nogroup_nofeat, rate_in_group, rate_not_group,
        odds_ratio, p_value.
        Sorted by p_value ascending.
    """
    try:
        from scipy.stats import fisher_exact
    except ImportError:
        raise ImportError(
            "scipy is required for fisher_tests. "
            "Install with: pip install lltk-dh[analysis]"
        )
    import pandas as pd
    import numpy as np

    shared_idx = feature_matrix.index.intersection(group_matrix.index)
    feat = feature_matrix.loc[shared_idx]
    grp = group_matrix.loc[shared_idx]

    # Filter by minimum support
    feat = feat.loc[:, feat.sum() >= min_feature_n]
    grp = grp.loc[:, grp.sum() >= min_group_n]

    feat_arr = feat.values  # (n, F)
    grp_arr = grp.values    # (n, G)
    feat_cols = list(feat.columns)
    grp_cols = list(grp.columns)

    rows = []

    def _run_tests(g_arr, g_col, f_arr, f_cols):
        for fi, f_col in enumerate(f_cols):
            f_vec = f_arr[:, fi]
            for gi, g_col_name in enumerate(g_col):
                g_vec = g_arr[:, gi]
                a = int((g_vec & f_vec).sum())
                b = int((g_vec & ~f_vec).sum())
                c = int((~g_vec & f_vec).sum())
                d = int((~g_vec & ~f_vec).sum())
                if b == 0 and c == 0:
                    continue
                odds = (a * d) / (b * c) if (b * c) > 0 else float('inf')
                _, p = fisher_exact([[a, b], [c, d]], alternative='two-sided')
                rate_in = a / (a + b) if (a + b) > 0 else float('nan')
                rate_out = c / (c + d) if (c + d) > 0 else float('nan')
                rows.append({
                    'group': g_col_name,
                    'feature': f_col,
                    'a_group_feat': a,
                    'b_group_nofeat': b,
                    'c_nogroup_feat': c,
                    'd_nogroup_nofeat': d,
                    'rate_in_group': rate_in,
                    'rate_not_group': rate_out,
                    'odds_ratio': odds,
                    'p_value': p,
                })

    _run_tests(grp_arr, grp_cols, feat_arr, feat_cols)

    if include_feature_pairs:
        def _task_prefix(col):
            return col.split(':')[0] if ':' in col else col

        for gi, g_col_name in enumerate(feat_cols):
            g_vec = feat_arr[:, gi:gi+1]  # keep as column
            g_prefix = _task_prefix(g_col_name)
            candidate_feats = [
                (fi, fc) for fi, fc in enumerate(feat_cols)
                if fi != gi and (
                    not cross_task_pairs_only
                    or _task_prefix(fc) != g_prefix
                )
            ]
            if not candidate_feats:
                continue
            f_idx = [fi for fi, _ in candidate_feats]
            f_names = [fc for _, fc in candidate_feats]
            _run_tests(g_vec, [g_col_name], feat_arr[:, f_idx], f_names)

    if not rows:
        return pd.DataFrame(columns=[
            'group', 'feature', 'a_group_feat', 'b_group_nofeat',
            'c_nogroup_feat', 'd_nogroup_nofeat', 'rate_in_group',
            'rate_not_group', 'odds_ratio', 'p_value',
        ])

    return pd.DataFrame(rows).sort_values('p_value').reset_index(drop=True)


def bh_fdr(p_series, alpha=0.05):
    """Benjamini-Hochberg FDR correction.

    Args:
        p_series: Series of p-values (any index).
        alpha:    FDR threshold (used only for reference; returned q-values are
                  unconditional — caller applies their own cutoff).

    Returns:
        Series of q-values aligned to p_series.index, clipped to [0, 1].
    """
    import pandas as pd
    import numpy as np

    p = p_series.values.astype(float)
    n = len(p)
    if n == 0:
        return p_series.copy()

    order = np.argsort(p)
    ranks = np.empty(n, dtype=float)
    ranks[order] = np.arange(1, n + 1)

    q = p * n / ranks
    # Enforce monotonicity: q[i] = min(q[i:]) working right-to-left
    q_sorted = q[order]
    for i in range(n - 2, -1, -1):
        if q_sorted[i] > q_sorted[i + 1]:
            q_sorted[i] = q_sorted[i + 1]
    q[order] = q_sorted
    q = np.clip(q, 0.0, 1.0)

    return pd.Series(q, index=p_series.index)
