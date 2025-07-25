"""
Library Features:

Name:          lib_analysis_predictors_alert
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""

#######################################################################################
# Library
import logging
from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
#######################################################################################


# -------------------------------------------------------------------------------------
# Method to compute alert level
def compute_alert_level(
        obj_dframe, obj_dict_warn_thr, obj_dict_warn_idx,
        tag_name_domain='event_domain',
        tag_name_soil_slips_prediction='soil_slips_prediction',
        tag_name_alert_level_prediction='alert_level_prediction',
        tag_name_alert_colour_prediction='alert_colour_prediction'):

    obj_dframe[tag_name_alert_level_prediction], obj_dframe[tag_name_alert_colour_prediction] = None, None

    for (obj_key, obj_warn_thr), obj_warn_idx in zip(obj_dict_warn_thr.items(), obj_dict_warn_idx.values()):
        for (warn_key, warn_value_thr), \
                (warn_value_colour, warn_value_idx) in zip(obj_warn_thr.items(), obj_warn_idx.items()):

            warn_value_lower, warn_value_upper = warn_value_thr[0], warn_value_thr[1]

            if (warn_value_lower is None) and (warn_value_upper is not None):

                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] <= warn_value_upper),
                    tag_name_alert_level_prediction] = warn_value_idx
                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] <= warn_value_upper),
                    tag_name_alert_colour_prediction] = warn_value_colour

            elif (warn_value_lower is not None) and (warn_value_upper is None):

                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] >= warn_value_lower),
                    tag_name_alert_level_prediction] = warn_value_idx
                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] >= warn_value_lower),
                    tag_name_alert_colour_prediction] = warn_value_colour

            elif warn_value_lower != warn_value_upper:

                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] >= warn_value_lower) &
                    (obj_dframe[tag_name_soil_slips_prediction] <= warn_value_upper),
                    tag_name_alert_level_prediction] = warn_value_idx
                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] >= warn_value_lower) &
                    (obj_dframe[tag_name_soil_slips_prediction] <= warn_value_upper),
                    tag_name_alert_colour_prediction] = warn_value_colour

            elif warn_value_lower == warn_value_upper:

                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] == warn_value_lower),
                    tag_name_alert_level_prediction] = warn_value_idx
                obj_dframe.loc[
                    (obj_dframe[tag_name_domain] == obj_key) &
                    (obj_dframe[tag_name_soil_slips_prediction] == warn_value_lower),
                    tag_name_alert_colour_prediction] = warn_value_colour

            elif (warn_value_lower is None) and (warn_value_upper is None):
                log_stream.warning(' ===> Warning thresholds are defined by NoneType values')
            else:
                log_stream.error(' ===> Warning thresholds are not expected in this format')
                raise NotImplementedError('Case not implemented yet')

    return obj_dframe
# -------------------------------------------------------------------------------------
