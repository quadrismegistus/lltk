import plotnine as p9, pandas as pd, numpy as np

def barplot(df, key, figsize=(8,6), vertical=False):
    if vertical: figsize=tuple(list(reversed(list(figsize))))
    p9.options.figure_size=figsize
    top_l = df[key].value_counts().index.tolist()
    df[key] = pd.Categorical(df[key], categories=reversed(top_l))
    fig = p9.ggplot(p9.aes(x=key, y='..count..', label='..count..'), data=df)
    fig+= p9.geom_bar(alpha=0.5)
    if vertical: fig+= p9.coord_flip()
    fig+=p9.stat_count(geom="text", position=p9.position_stack(vjust=0.5), size=10)
    fig+=p9.theme_classic()
    return fig

def density(df, key, figsize=(8,6), vertical=False):
    p9.options.figure_size=figsize
    fig = p9.ggplot(p9.aes(x=key, y='..count..', label='..count..'), data=df)
    fig+= p9.geom_density(alpha=0.5)
    fig+=p9.theme_classic()
    return fig
