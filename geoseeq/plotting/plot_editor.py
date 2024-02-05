import pandas as pd
from .highcharts_config import HighchartsConfig
from .viz_utils import (
    col_is_numeric,
    col_is_categorical,
    COLORS,
)


def make_highcharts_config(df, col_x, col_y=None, col_color=None):
    """Return the config for a highcharts plot.
    
    Plot type is defined by column types.

    If col_x is numeric:
        If col_y is numeric then the plot is a scatter plot, possibly colored.
        If col_y is categorical then the plot is a horizontal boxplot, possibly colored.
        If col_y is None then the plot is a histogram, possibly colored.

    If col_x is categorical:
        If col_y is numeric then the plot is a vertical boxplot, possibly colored.
        If col_y is categorical then the plot is a heatmap.
            If col_color is numeric then the heatmap is colored by col_color.
            If col_color is None then the heatmap is colored by counts.
            If col_color is categorical then an error is raised.
        If col_y is None then the plot is a bar chart with counts, possibly colored.

    If col_x is neither numeric nor categorical then an error is raised.   
    """
    return _make_highcharts_config(df, col_x, col_y=col_y, col_color=col_color).to_dict()


def _make_highcharts_config(df, col_x, col_y=None, col_color=None):
    """Return the config for a highcharts plot."""
    col_color = df[col_color] if col_color is not None else None
    x_is_numeric, x_numeric_col = col_is_numeric(df[col_x])
    if x_is_numeric:
        if col_y is None:
            return make_histogram_config(x_numeric_col, col_color=col_color)
        y_is_numeric, y_numeric_col = col_is_numeric(df[col_y])
        if y_is_numeric:
            return make_scatter_config(x_numeric_col, y_numeric_col, col_color=col_color)
        y_is_categorical, y_categorical_col = col_is_categorical(df[col_y])
        if y_is_categorical:
            return make_boxplot_config(x_numeric_col, y_categorical_col, col_color=col_color, horizontal=True)
        raise ValueError(f'Column {col_y} is neither numeric nor categorical.')

    x_is_categorical, x_categorical_col = col_is_categorical(df[col_x])
    if x_is_categorical:
        if col_y is None:
            return make_bar_config(x_categorical_col, col_color=col_color)
        y_is_numeric, y_numeric_col = col_is_numeric(df[col_y])
        if y_is_numeric:
            return make_boxplot_config(x_categorical_col, y_numeric_col, col_color=col_color)
        y_is_categorical, y_categorical_col = col_is_categorical(df[col_y])
        if y_is_categorical:
            return make_heatmap_config(x_categorical_col, y_categorical_col)
        raise ValueError(f'Column {col_y} is neither numeric nor categorical.')
    
    raise ValueError(f'Column {col_x} is neither numeric nor categorical.')


def make_bar_config(col_x, col_color=None):
    """Return the config for a highcharts bar plot."""
    x_val_counts = col_x.value_counts()
    config = HighchartsConfig('column')
    config.set_x_axis(col_x.name, categories=x_val_counts.index.tolist())
    config.set_y_axis('Count')
    if col_color is None:
        config.add_series('Count', x_val_counts.tolist)
        return config
    else:
        config.set_colors(col_color.unique())
        for col_val in col_color.unique():
            col_val_counts = col_x[col_color == col_val].value_counts()
            config.add_series(col_val, col_val_counts.tolist())
        return config


def make_heatmap_config(col_x, col_y):
    """Return the config for a highcharts heatmap plot."""
    counts = pd.crosstab(col_x, col_y)
    counts = counts.melt(ignore_index=False).reset_index()
    counts.columns = ['x', 'y', 'value']
    x_cats, y_cats = counts['x'].unique().tolist(), counts['y'].unique().tolist()
    counts['x'] = counts['x'].apply(lambda x: x_cats.index(x))
    counts['y'] = counts['y'].apply(lambda y: y_cats.index(y))
    config = HighchartsConfig('heatmap')
    config.set_x_axis(col_x.name, categories=x_cats)
    config.set_y_axis(col_y.name, categories=y_cats)
    config.set_color_axis('#FFFFFF', COLORS[0])
    config.add_series('Count', [row for row in counts.T.to_dict(orient='list').values()])
    return config

 
def make_boxplot_config(col_x, col_y, col_color=None, horizontal=False):
    """Return the config for a highcharts boxplot plot."""
    config = HighchartsConfig('boxplot')
    config.set_x_axis(col_x.name, categories=col_x.unique().tolist())
    config.set_y_axis(col_y.name)

    def vals_to_boxplot(vals):
        q1 = vals.quantile(0.25)
        q3 = vals.quantile(0.75)
        iqr = q3 - q1
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        return [low, q1, vals.median(), q3, high]
    
    if col_color is None:
        tbl = pd.concat([col_x, col_y], axis=1).dropna()
        tbl = tbl.groupby(col_x).agg(vals_to_boxplot).drop_duplicates()
        config.add_series(
            col_y.name,
            [row[0] for row in tbl.T.to_dict(orient='list').values()],
        )
        return config
    else:
        tbl = pd.concat([col_x, col_y, col_color], axis=1).dropna().drop_duplicates()
        config.set_colors(col_color.unique())
        for col_val in col_color.unique():
            col_val_tbl = tbl[col_color == col_val]
            col_val_tbl = col_val_tbl.groupby(col_x).agg(vals_to_boxplot)
            config.add_series(
                col_val,
                [row[0] for row in col_val_tbl.T.to_dict(orient='list').values()],
            )
        return config


def make_scatter_config(col_x, col_y, col_color=None):
    """Return the config for a highcharts scatter plot."""
    config = HighchartsConfig('scatter')
    config.set_x_axis(col_x.name)
    config.set_y_axis(col_y.name)
    if col_color is None:
        tbl = pd.concat([col_x, col_y], axis=1).dropna().drop_duplicates()
        config.add_series('Count', list(zip(tbl.iloc[:,0].tolist(), tbl.iloc[:,1].tolist())))
        return config
    else:
        tbl = pd.concat([col_x, col_y, col_color], axis=1).dropna().drop_duplicates()
        config.set_colors(col_color.unique())
        for col_val in col_color.unique():
            col_val_tbl = tbl[col_color == col_val]
            config.add_series(
                col_val,
                list(zip(col_val_tbl.iloc[:,0].tolist(), col_val_tbl.iloc[:,1].tolist())),
            )
        return config
    

def make_histogram_config(col_x, col_color=None):
    """Return the config for a highcharts histogram plot."""
    config = HighchartsConfig('column')
    config.set_x_axis(col_x.name)
    config.set_y_axis('Count')
    col_x_bins = pd.cut(col_x, 10)
    if col_color is None:
        col_x_binned = col_x_bins.value_counts().reset_index()
        col_x_binned.columns = ['x', 'y']
        col_x_binned['x'] = col_x_binned['x'].apply(lambda x: x.right)
        config.add_series(
            'Count',
            [row for row in col_x_binned.T.to_dict(orient='list').values() if row[1] > 0],
        )
        return config
    else:
        config.set_colors(col_color.unique())
        for col_val in col_color.unique():
            col_val_tbl = col_x_bins[col_color == col_val]
            col_val_bins = col_val_tbl.value_counts().reset_index()
            col_val_bins.columns = ['x', 'y']
            col_val_bins['x'] = col_val_bins['x'].apply(lambda x: x.right)
            config.add_series(
                col_val,
                [row for row in col_val_bins.T.to_dict(orient='list').values() if row[1] > 0],
            )
        return config