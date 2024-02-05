import json
from .viz_utils import (
    unique_values_to_colors,
    COLORS
)

class HighchartsConfig:

    def __init__(self, chart_type):
        self.chart_type = chart_type
        self.title = 'Plot'

    def set_title(self, title):
        self.title = title
        return self
    
    def set_x_axis(self, title, categories=None):
        self.x_axis = {'title': {'text': title}}
        if categories is not None:
            self.x_axis['categories'] = categories
        return self
    
    def set_y_axis(self, title, categories=None):
        self.y_axis = {'title': {'text': title}}
        if categories is not None:
            self.y_axis['categories'] = categories
        return self
    
    def set_color_axis(self, min_color, max_color, minVal=0):
        self.color_axis = {
            'min': minVal,
            'minColor': min_color,
            'maxColor': max_color,
        }
        return self

    def add_series(self, name, data, color=COLORS[0]):
        series = {'name': name, 'data': data}
        if color is not None:
            series['color'] = color
        elif hasattr(self, 'color_map'):
            series['color'] = self.color_map[name]
        if not hasattr(self, 'series'):
            self.series = []
        self.series.append(series)
        return self
    
    def set_colors(self, values):
        self.color_map = unique_values_to_colors(values)
    
    def to_dict(self):
        config = {
            'chart': {
                'type': self.chart_type,
            },
            'title': {
                'text': self.title,
            },
            'xAxis': self.x_axis,
            'yAxis': self.y_axis,
        }
        if hasattr(self, 'color_axis'):
            config['colorAxis'] = self.color_axis
        if hasattr(self, 'series'):
            config['series'] = self.series
        return config
    
    def to_json(self):
        return json.dumps(self.to_dict())