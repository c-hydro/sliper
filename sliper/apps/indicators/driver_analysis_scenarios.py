"""
Class Features

Name:          driver_analysis_scenarios
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20200515'
Version:       '1.0.0'
"""

######################################################################################
# Library
import logging
import os
import pandas as pd

from lib_utils_system import fill_tags2string, make_folder
from lib_data_io_pickle import read_obj
from lib_data_io_csv_scenarios import write_file_csv, convert_file_csv2df
from lib_utils_data_point_scenarios import filter_scenarios_dataframe
from lib_utils_plot import plot_scenarios_rain2sm

from lib_info_args import logger_name_scenarios as logger_name

# Logging
log_stream = logging.getLogger(logger_name)

# Debug
import matplotlib.pylab as plt
######################################################################################


# -------------------------------------------------------------------------------------
# Class DriverAnalysis for scenarios
class DriverAnalysis:

    # -------------------------------------------------------------------------------------
    # Initialize class
    def __init__(self, time_run, time_range, anc_dict, dst_dict, graph_dict,
                 alg_ancillary=None, alg_template_tags=None,
                 collections_data_geo=None, collections_data_group=None, collections_data_graph=None,
                 flag_data_dst_indicators='indicators_data', flag_data_dst_scenarios='scenarios_data',
                 flag_graph_rain2sm='rain2sm_graph',
                 flag_dst_updating=True,
                 event_n_min=0, event_n_max=None, event_label=True, filter_season=False):

        self.time_run = time_run
        self.time_range = time_range
        self.time_range_from = time_range[-1]
        self.time_range_to = time_range[0]

        self.anc_dict = anc_dict
        self.dst_dict = dst_dict

        self.file_name_tag = 'file_name'
        self.folder_name_tag = 'folder_name'
        self.obj_active_tag = 'obj_active'

        self.flag_data_dst_indicators = flag_data_dst_indicators
        self.flag_data_dst_scenarios = flag_data_dst_scenarios
        self.flag_graph_rain2sm = flag_graph_rain2sm

        self.data_geo = collections_data_geo
        self.data_group = collections_data_group
        self.data_graph = collections_data_graph

        self.alg_ancillary = alg_ancillary
        self.alg_template_tags = alg_template_tags

        self.file_name_dst_indicators_raw = dst_dict[self.flag_data_dst_indicators][self.file_name_tag]
        self.folder_name_dst_indicators_raw = dst_dict[self.flag_data_dst_indicators][self.folder_name_tag]
        self.file_name_dst_scenarios_raw = dst_dict[self.flag_data_dst_scenarios][self.file_name_tag]
        self.folder_name_dst_scenarios_raw = dst_dict[self.flag_data_dst_scenarios][self.folder_name_tag]

        self.file_name_graph_rain2sm_raw = graph_dict[self.flag_graph_rain2sm][self.file_name_tag]
        self.folder_name_graph_rain2sm_raw = graph_dict[self.flag_graph_rain2sm][self.folder_name_tag]
        self.active_graph_rain2sm_raw = graph_dict[self.flag_graph_rain2sm][self.obj_active_tag]

        self.flag_dst_updating = flag_dst_updating

        if self.data_graph is not None:
            if 'filter_season' in list(self.data_graph.keys()):
                self.filter_season = self.data_graph['filter_season']
            else:
                self.filter_season = filter_season
        else:
            self.filter_season = filter_season

        if self.filter_season:
            self.lut_season = {
                1: 'DJF', 2: 'DJF', 3: 'MAM', 4: 'MAM', 5: 'MAM', 6: 'JJA',
                7: 'JJA', 8: 'JJA', 9: 'SON', 10: 'SON', 11: 'SON', 12: 'DJF'}
        else:
            self.lut_season = {
                1: 'ALL', 2: 'ALL', 3: 'ALL', 4: 'ALL', 5: 'ALL', 6: 'ALL',
                7: 'ALL', 8: 'ALL', 9: 'ALL', 10: 'ALL', 11: 'ALL', 12: 'ALL'}

        self.list_season = list(set(list(self.lut_season.values())))

        file_path_dst_indicators_collections = {}
        for group_key in self.data_group.keys():
            file_path_list = []
            for time_step in time_range:
                file_path_step = collect_file_list(
                    time_step, self.folder_name_dst_indicators_raw, self.file_name_dst_indicators_raw,
                    self.alg_template_tags, alert_area_name=group_key)[0]
                if os.path.exists(file_path_step):
                    file_path_list.append(file_path_step)
            if file_path_list:
                file_path_dst_indicators_collections[group_key] = file_path_list
            else:
                file_path_dst_indicators_collections[group_key] = None
        self.file_path_dst_indicators_collections = file_path_dst_indicators_collections

        file_path_dst_scenarios_collections = {}
        for group_key in self.data_group.keys():
            file_path_list = collect_file_list(
                self.time_run, self.folder_name_dst_scenarios_raw, self.file_name_dst_scenarios_raw,
                self.alg_template_tags, alert_area_name=group_key, season_name=None,
                time_run=self.time_run, time_from=self.time_range_from, time_to=self.time_range_to)
            file_path_dst_scenarios_collections[group_key] = file_path_list
        self.file_path_dst_scenarios_collections = file_path_dst_scenarios_collections

        file_path_graph_rain2sm_collections = {}
        for group_key in self.data_group.keys():
            file_path_list = collect_file_list(
                self.time_run, self.folder_name_graph_rain2sm_raw, self.file_name_graph_rain2sm_raw,
                self.alg_template_tags, alert_area_name=group_key, season_name=self.list_season,
                time_run=self.time_run, time_from=self.time_range_from, time_to=self.time_range_to)
            file_path_graph_rain2sm_collections[group_key] = file_path_list
        self.file_path_graph_rain2sm_collections = file_path_graph_rain2sm_collections

        self.template_rain_point_accumulated = 'rain_accumulated_{:}'
        self.template_rain_point_avg = 'rain_average_{:}'
        self.template_sm_point_first = 'sm_value_first'
        self.template_sm_point_last = 'sm_value_last'
        self.template_sm_point_max = 'sm_value_max'
        self.template_sm_point_avg = 'sm_value_avg'
        self.template_time_index = 'time'

        self.template_indicators_domain = 'domain'
        self.template_indicators_time = 'time'
        self.template_indicators_event = 'event'
        self.template_indicators_data = 'data'

        if self.data_graph is not None:
            if 'filter_event_min' in list(self.data_graph.keys()):
                self.event_n_min = self.data_graph['filter_event_min']
            else:
                self.event_n_min = event_n_min
        else:
            self.event_n_min = event_n_min

        if self.data_graph is not None:
            if 'filter_event_max' in list(self.data_graph.keys()):
                self.event_n_max = self.data_graph['filter_event_max']
            else:
                self.event_n_max = event_n_max
        else:
            self.event_n_max = event_n_max

        self.event_label = event_label

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to plot scenarios data
    def plot_scenarios(self, scenarios_collections):

        log_stream.info(' ----> Plot scenarios [' + str(self.time_run) + '] ... ')

        group_data = self.data_group

        season_list = self.list_season
        event_n_min = self.event_n_min
        event_n_max = self.event_n_max

        scenarios_rain2sm_file_path = self.file_path_graph_rain2sm_collections

        for group_key, group_items in group_data.items():

            log_stream.info(' -----> Plot datasets for reference area "' + group_key + '" ... ')

            file_path_rain2sm = scenarios_rain2sm_file_path[group_key]
            file_data = scenarios_collections[group_key]

            rain_period_list = group_items['rain_datasets']['search_period']

            template_rain_point = []
            for rain_period_step in rain_period_list:
                template_rain_step = self.template_rain_point_accumulated.format(rain_period_step)
                template_rain_point.append(template_rain_step)
            template_sm_point = self.template_sm_point_avg

            if file_data is not None:

                log_stream.info(' ------> Compare "Rain" vs "Soil_Moisture" ... ')

                if self.active_graph_rain2sm_raw:
                    for season_name, file_path_rain2sm_step in zip(season_list, file_path_rain2sm):

                        log_stream.info(' ------> Season "' + season_name + '" ... ')

                        file_data_step = filter_scenarios_dataframe(
                            file_data,
                            tag_column_sm=template_sm_point,
                            tag_column_rain=template_rain_point,
                            filter_rain=True, filter_sm=True, filter_event=True,
                            filter_season=self.filter_season,
                            tag_column_event='event_n', value_min_event=event_n_min, value_max_event=event_n_max,
                            season_lut=self.lut_season, season_name=season_name)

                        if not file_data_step.empty:
                            folder_name_rain2sm, file_name_rain2sm = os.path.split(file_path_rain2sm_step)
                            make_folder(folder_name_rain2sm)

                            plot_scenarios_rain2sm(file_data_step, file_path_rain2sm_step,
                                                   var_x_name=self.template_sm_point_avg,
                                                   var_y_name=self.template_rain_point_accumulated,
                                                   var_z_name='event_index',
                                                   var_time_name='time',
                                                   event_n_min=event_n_min, event_n_max=event_n_max,
                                                   event_label=self.event_label, season_label=season_name,
                                                   figure_dpi=60,
                                                   extra_args={'rain_type': rain_period_list,
                                                               'soil_moisture_type': 'average'})

                            log_stream.info(' ------> Season "' + season_name + '" ... DONE')

                        else:
                            log_stream.info(' ------> Season "' + season_name +
                                            '" ... SKIPPED. Datasets for this season is empty.')

                    log_stream.info(' ------> Compare "Rain" vs "Soil_Moisture" ... DONE')

                else:
                    log_stream.info(' ------> Compare "Rain" vs "Soil_Moisture" ... SKIPPED. Plot is not activated')

                log_stream.info(' -----> Plot datasets for reference area "' + group_key + '" ... DONE')
            else:
                log_stream.info(' -----> Plot datasets for reference area "' + group_key +
                                '" ... SKIPPED. Datasets are undefined.')

        log_stream.info(' ----> Plot scenarios [' + str(self.time_run) + '] ... DONE')

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to dump scenarios data
    def dump_scenarios(self, scenarios_collections):

        log_stream.info(' ----> Dump scenarios [' + str(self.time_run) + '] ... ')

        group_data = self.data_group

        file_path_scenarios_collections = self.file_path_dst_scenarios_collections

        scenarios_file_list, scenario_key_list, scenario_flag_merge = filter_file_obj(file_path_scenarios_collections)

        if self.flag_dst_updating:
            for scenarios_file_name in scenarios_file_list:
                if os.path.exists(scenarios_file_name):
                    os.remove(scenarios_file_name)

        if not scenario_flag_merge:
            for group_key, group_items in group_data.items():

                log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... ')

                file_path = file_path_scenarios_collections[group_key]

                if isinstance(file_path, list):
                    file_path = file_path[0]

                if not os.path.exists(file_path):

                    file_data = scenarios_collections[group_key]
                    if file_data is not None:

                        folder_name, file_name = os.path.split(file_path)
                        make_folder(folder_name)

                        write_file_csv(file_path, file_data)

                        log_stream.info(' -----> Save datasets for reference area "' + group_key + '" ... DONE')
                    else:
                        log_stream.info(' -----> Save datasets for reference area "' + group_key +
                                        '"  ... SKIPPED. Datasets are undefined.')

                else:
                    log_stream.info(' -----> Save datasets for reference area "' + group_key +
                                    '"  ... SKIPPED. Datasets previously saved.')

        elif scenario_flag_merge:

            file_path = scenarios_file_list
            if isinstance(file_path, list):
                file_path = file_path[0]

            log_stream.info(' -----> Save datasets collections ... ')
            if not os.path.exists(file_path):

                merged_data = None
                for group_key in group_data.keys():

                    log_stream.info(' ------> Reference area "' + group_key + '" ... ')

                    file_data = scenarios_collections[group_key]

                    if merged_data is None:
                        merged_data = file_data
                    else:
                        merged_data = merged_data.append(file_data)

                    log_stream.info(' ------> Reference area "' + group_key + '" ... DONE')

                if merged_data is not None:

                    folder_name, file_name = os.path.split(file_path)
                    make_folder(folder_name)

                    write_file_csv(file_path, merged_data)
                    log_stream.info(' -----> Save datasets collections ... DONE')

                else:
                    log_stream.info(' -----> Save datasets collections ... SKIPPED. Datasets are undefined')

            else:
                log_stream.info(' -----> Save datasets collections  ... SKIPPED. Datasets previously saved.')

        log_stream.info(' ----> Dump scenarios [' + str(self.time_run) + '] ... DONE')

    # -------------------------------------------------------------------------------------

    # -------------------------------------------------------------------------------------
    # Method to collect scenarios data
    def collect_scenarios(self):

        log_stream.info(' ----> Collect scenarios [' + str(self.time_run) + '] ... ')

        group_data = self.data_group

        file_path_indicators_collections = self.file_path_dst_indicators_collections
        file_path_scenarios_collections = self.file_path_dst_scenarios_collections

        scenarios_collections = {}
        for group_key, group_items in group_data.items():

            log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... ')

            file_path_indicators = file_path_indicators_collections[group_key]
            file_path_scenarios = file_path_scenarios_collections[group_key]

            if isinstance(file_path_scenarios, list):
                file_path_scenarios = file_path_scenarios[0]

            if self.flag_dst_updating:
                if os.path.exists(file_path_scenarios):
                    os.remove(file_path_scenarios)

            if not os.path.exists(file_path_scenarios):
                if file_path_indicators is not None:

                    scenarios_time, scenarios_dict = [], {}
                    for file_path in file_path_indicators:

                        log_stream.info(' ------> Get datasets file for reference area "' + file_path + '" ... ')

                        file_obj = read_obj(file_path)

                        # check file version keys
                        if 'data' in list(file_obj.keys()) and 'event' in list(file_obj.keys()):
                            # file keys new version
                            pass
                        elif 'indicators_data' in list(file_obj.keys()) and 'indicators_event' in list(file_obj.keys()):
                            # file keys old version
                            file_obj['data'] = file_obj.pop('indicators_data')
                            file_obj['event'] = file_obj.pop('indicators_event')
                            file_obj['domain'] = file_obj.pop('alert_area')
                        else:
                            log_stream.error(' ===> File indicators keys are not supported')
                            raise NotImplemented('Case not implemented yet')

                        obj_domain = file_obj[self.template_indicators_domain]
                        obj_time = file_obj[self.template_indicators_time]
                        obj_data = file_obj[self.template_indicators_data]
                        obj_event = file_obj[self.template_indicators_event]

                        if 'event_features' in list(obj_event.keys()):
                            obj_event.pop('event_features', None)
                        if 'event_domain' not in list(obj_event.keys()):
                            obj_event['event_domain'] = obj_domain

                        if (obj_data is not None) and (obj_event is not None):
                            scenarios_time.append(obj_time)

                            file_scenarios_dict = {**obj_data, **obj_event}
                            for field_key, field_value in file_scenarios_dict.items():
                                if field_key not in list(scenarios_dict.keys()):
                                    scenarios_dict[field_key] = [field_value]
                                else:
                                    field_tmp = scenarios_dict[field_key]
                                    field_tmp.append(field_value)
                                    scenarios_dict[field_key] = field_tmp

                        log_stream.info(' ------> Get datasets file for reference area "' + file_path + '" ... ')

                    log_stream.info(' ------> Create datasets merged ... ')
                    scenarios_df = pd.DataFrame(index=scenarios_time, data=scenarios_dict)
                    scenarios_df.index.name = self.template_time_index
                    log_stream.info(' ------> Create datasets merged ... DONE')

                    log_stream.info(' -----> Get datasets for reference area "' + group_key + '" ... DONE')
                else:
                    scenarios_df = None
                    log_stream.info(' -----> Get datasets for reference area "' + group_key +
                                    '" ... SKIPPED. All datasets are undefined')

                scenarios_collections[group_key] = scenarios_df

            else:
                log_stream.info(' -----> Get datasets for reference area "' + group_key +
                                '" ... SKIPPED. Datasets previously computed')

                file_df = convert_file_csv2df(file_path_scenarios)
                scenarios_collections[group_key] = file_df

        log_stream.info(' ----> Collect scenarios [' + str(self.time_run) + '] ... DONE')

        return scenarios_collections

    # -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to filter file list
def filter_file_obj(file_name_obj):

    file_name_list = []
    file_key_list = []
    for file_key, file_name in file_name_obj.items():
        if isinstance(file_name, list) and (file_name.__len__() == 1):
            file_name = file_name[0]
        else:
            logging.error(' ===> Filename scenarios list in unsupported format')
            raise NotImplementedError('Case not implemented yet')
        file_name_list.append(file_name)
        file_key_list.append(file_key)
    file_name_unique = list(dict.fromkeys(file_name_list))
    file_key_unique = list(dict.fromkeys(file_key_list))

    if (file_name_unique.__len__() == 1) and (file_key_unique.__len__() > file_name_unique.__len__()):
        flag_file_merge = True
    else:
        flag_file_merge = False

    return file_name_unique, file_key_unique, flag_file_merge

# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to collect ancillary file
def collect_file_list(time_range, folder_name_raw, file_name_raw, template_tags,
                      alert_area_name=None, season_name=None,
                      time_run=None, time_from=None, time_to=None):

    if (not isinstance(time_range, pd.DatetimeIndex)) and (isinstance(time_range, pd.Timestamp)):
        time_range = pd.DatetimeIndex([time_range])

    if time_run is None:
        datetime_run = time_range[0]
    else:
        datetime_run = time_run
    if time_from is None:
        datetime_from = time_range[-1]
    else:
        datetime_from = time_from
    if time_to is None:
        datetime_to = time_range[0]
    else:
        datetime_to = time_to

    file_name_list = []
    for datetime_step in time_range:
        template_values_step = {
            'alert_area_name': alert_area_name, 'season_name': None,
            'run_datetime': datetime_run, 'run_sub_path_time': datetime_run,
            'destination_indicators_datetime': datetime_step, 'destination_indicators_sub_path_time': datetime_step,
            'destination_scenarios_datetime': datetime_step, 'destination_scenarios_sub_path_time': datetime_step,
            'destination_scenarios_datetime_from': datetime_from,
            'destination_scenarios_datetime_to': datetime_to,
        }

        template_common_keys = set(template_tags).intersection(template_values_step)
        template_common_tags = {common_key: template_tags[common_key] for common_key in template_common_keys}

        if season_name is None:
            folder_name_def = fill_tags2string(folder_name_raw, template_common_tags, template_values_step)
            file_name_def = fill_tags2string(file_name_raw, template_common_tags, template_values_step)

            file_path_def = os.path.join(folder_name_def, file_name_def)

            file_name_list.append(file_path_def)

        else:

            for season_step in season_name:
                template_values_step['season_name'] = season_step

                folder_name_def = fill_tags2string(folder_name_raw, template_common_tags, template_values_step)
                file_name_def = fill_tags2string(file_name_raw, template_common_tags, template_values_step)

                file_path_def = os.path.join(folder_name_def, file_name_def)

                file_name_list.append(file_path_def)

    return file_name_list

# -------------------------------------------------------------------------------------
