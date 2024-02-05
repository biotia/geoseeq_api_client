import click
from .shared_params import (
    handle_project_id,
    project_id_arg,
    sample_ids_arg,
    handle_multiple_sample_ids,
    use_common_state,
    flatten_list_of_els_and_files,
    yes_option,
)
from geoseeq.plotting.plot_editor import _make_highcharts_config
from geoseeq.plotting.viz_utils import col_is_numeric, col_is_categorical


@click.command('plot')
@use_common_state
@project_id_arg
def cli_plot(state, project_id):
    """Commands for plotting data."""
    knex = state.get_knex()
    proj = handle_project_id(knex, project_id)
    df = proj.get_sample_metadata()
    numeric_cols, categorical_cols = set(), set()
    for col in df.columns:
        if col_is_numeric(df[col])[0]:
            numeric_cols.add(col)
        if col_is_categorical(df[col])[0]:
            categorical_cols.add(col)
    cols = [(col, 'numeric') for col in numeric_cols] + [(col, 'categorical') for col in categorical_cols]
    
    plot_configs = []

    # for x_col, x_col_type in cols:
    #     plot_configs.append(_make_highcharts_config(df, x_col))
    #     for y_col, y_col_type in cols:
    #         plot_configs.append(_make_highcharts_config(df, x_col, col_y=y_col))
    #         for color_col, color_col_type in cols:
    #             plot_configs.append(_make_highcharts_config(df, x_col, col_y=y_col, col_color=color_col))
    # plot_configs = [config.to_json() for config in plot_configs]
    # print(plot_configs)

    for col in numeric_cols:
        print(f'{len(cols)}: {col} (numeric)')
        cols.append(col)
    for col in categorical_cols:
        print(f'{len(cols)}: {col} (categorical)')
        cols.append(col)
    x_col_index = click.prompt('Enter the index of the x column', type=int)
    x_col = cols[x_col_index]
    y_col, color_col = None, None
    if click.confirm('Do you want to use a y column?'):
        y_col_index = click.prompt('Enter the index of the y column', type=int)
        y_col = cols[y_col_index]
    if click.confirm('Do you want to use a color column?'):
        color_col_index = click.prompt('Enter the index of the color column', type=int)
        color_col = cols[color_col_index]

    config = _make_highcharts_config(df, x_col, col_y=y_col, col_color=color_col)
    print(config.to_json())

