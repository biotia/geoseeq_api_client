"""
Functions related to making highcharts plots on GeoSeeq.

GeoSeeq uses highcharts to make interactive plots.
"""

from .constants import COLORS


def series_is_categorical(col, max_unique=100, min_unique=2, min_not_null=3):
    """Return True if the series is categorical according to these criteria:
    1. The column is of type 'string'
    2. The column has lte 100 unique values
    3. The column has at least 2 unique values
    4. The column has at least 3 values that are not null
    """
    if col.dtype == 'object':
        if col.nunique() <= max_unique:
            if col.nunique() >= min_unique:
                if col.count() >= min_not_null:
                    return True
    return False


def series_is_numeric(col, min_not_null=3):
    """Return True if the series is numeric according to these criteria:
    1. The column can be cast to float
    2. The column has at least 3 values that are not null
    """
    try:
        col.astype(float)
        if col.count() >= min_not_null:
            return True
    except:
        pass
    return False


def _as_col_name(data, col_name):
    """Return the column name of data.
    
    If col_name is not a column name treat it as an index to the column list.
    """
    if col_name is None:
        return None
    if col_name in data.columns:
        return col_name
    else:
        return data.columns[col_name]


def scatter_plot_config(plot_title, data, x_col=0, y_col=1, color_col=None, x_title=None, y_title=None, color_title=None):
    """Return a highcharts config for a scatter plot of the data.
    
    If the color_col is categorical, the points will be colored by category.
    If the color_col is numeric, the points will be colored by a color scale.
    """
    x_col, y_col, color_col = _as_col_name(data, x_col), _as_col_name(data, y_col), _as_col_name(data, color_col)
    base_config = {
        "chart": {
            "type": 'scatter',
            "zoomType": 'xy',
        },
        "title": {
            "text": plot_title,
        },
        "xAxis": {
            "title": {
                "text": x_title or x_col,
            },
        },
        "yAxis": {
            "title": {
                "text": y_title or y_col,
            },
        },
        "tooltip": {
            "format": '<b>X:</b> {point.x} <b>Y:</b> {point.y}'
        },
        "plotOptions": {},
    }

    def to_pt_list(data):
        return list(data.T.to_dict(orient='list').values())
    
    if color_col:
        base_config["legend"] = {
            "title": {
                "text": color_title or color_col,
            },
        }
        base_config["tooltip"]["format"] = '<b>' + (color_title or color_col) + ': </b>{point.value}<br><b>X:</b> {point.x} <b>Y:</b> {point.y}'
        my_data = data[[x_col, y_col, color_col]].dropna()
        if series_is_categorical(data[color_col]):
            series = []
            for i, value in enumerate(my_data[color_col].unique()):
                value_data = my_data[my_data[color_col] == value]
                series.append({
                    'name': value,
                    'data': to_pt_list(value_data),
                    'color': COLORS[i % len(COLORS)],
                })
            base_config["series"] = series
        elif series_is_numeric(data[color_col]):
            minVal, maxVal = my_data[color_col].quantile(0.01) * 0.9, my_data[color_col].quantile(0.99) * 1.1
            base_config["colorAxis"] ={
                'minColor': COLORS[0], 'maxColor': COLORS[1], "min": minVal, "max": maxVal
            }
            base_config["series"] = [{
                "colorKey": "value",
                'name': color_title or color_col,
                'data': to_pt_list(my_data),
            }]
        else:
            raise ValueError(f'color_col "{color_col}" is not categorical or numeric')
    else:
        my_data = data[[x_col, y_col]].dropna()
        base_config["series"] = [{
            'name': 'Data',
            'data': to_pt_list(my_data),
            'color': COLORS[0],
        }]
    return base_config
