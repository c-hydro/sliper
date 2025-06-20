"""
Library Features:

Name:          lib_utils_system
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20220320'
Version:       '1.0.0'
"""


# -------------------------------------------------------------------------------------
# Libraries
import logging
import os
import operator
import gzip
import tempfile

from random import randint
from copy import deepcopy
from datetime import datetime
from functools import reduce  # forward compatibility for Python 3

from lib_info_args import logger_name_predictors as logger_name

# Logging
log_stream = logging.getLogger(logger_name)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get all keys
def get_dict_nested_all_keys(obj_data, key_list=[], key_collection={}, key_n=None, fx_n=None):

    key_pivot = None
    for n, (key, value) in enumerate(obj_data.items()):

        if key_n is None:
            key_n = n
        if key_pivot is not None:
            if not isinstance(key_pivot, list):
                key_pivot = [key_pivot]
            key_list = deepcopy(key_pivot)
        if fx_n is None:
            fx_n = 0

        if key_collection.__len__() > n + 1:
            n_tmp = n + 1
            while n_tmp < key_collection.__len__():
                n_tmp += 1
            key_n = n_tmp

        first_value = list(obj_data.values())[0]
        key_break = False
        if isinstance(first_value, dict):
            key_list.append(key)
            fx_n += 1
            key_list, key_collection, key_break, key_n, fx_n = get_dict_nested_all_keys(
                obj_data[key], key_list, key_collection, key_n, fx_n)

            if key_break:
                if fx_n > 1:
                    fx_n -= 1
                    key_pivot = key_list[:-fx_n]
                elif fx_n == 1:
                    key_pivot = key_list[0]
        else:
            key_break = True
            key_collection[key_n] = key_list
            key_n = None
            break

    return key_list, key_collection, key_break, key_n, fx_n
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get nested all values
def get_dict_nested_all_values(obj_dictionary):
    for obj_values in obj_dictionary.values():
        if isinstance(obj_values, dict):
            yield from get_dict_nested_all_values(obj_values)
        else:
            yield obj_values
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to get nested values
def get_dict_nested_value(obj_dictionary, obj_keys_list):
    return reduce(operator.getitem, obj_keys_list, obj_dictionary)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to join folder and filename (managing some exceptions)
def join_path(folder_name, file_name):
    if (folder_name is not None) and (file_name is not None):
        path_name = os.path.join(folder_name, file_name)
    elif (folder_name is None) and file_name:
        path_name = file_name
    elif folder_name and (file_name is None):
        path_name = folder_name
    else:
        log_stream.error(' ===> FolderName: "' + str(folder_name) + '" FileName: "' +
                         str(file_name) + '". Join folder and file names ... FAILED. ')
        raise NotImplemented('Case not implemented yet')
    return path_name
# -------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------
# Method to unzip file
def unzip_filename(file_name_zip, file_name_unzip):

    file_handle_zip = gzip.GzipFile(file_name_zip, "rb")
    file_handle_unzip = open(file_name_unzip, "wb")

    file_data_unzip = file_handle_zip.read()
    file_handle_unzip.write(file_data_unzip)

    file_handle_zip.close()
    file_handle_unzip.close()

# --------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create a tmp name
def create_filename_tmp(prefix='tmp_', suffix='.tiff', folder=None):
    if folder is None:
        folder = '/tmp'
    with tempfile.NamedTemporaryFile(dir=folder, prefix=prefix, suffix=suffix, delete=False) as tmp:
        temp_file_name = tmp.name
    return temp_file_name
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to change file extension
def change_extension(file_in, ext_out='bin'):
    file_base, ext_in = os.path.splitext(file_in)
    if not file_base.endswith(ext_out):
        file_out = ''.join([file_base, ext_out])
    else:
        file_out = deepcopy(file_base)
    return file_out
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to make folder
def make_folder(path_folder):
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to flat list of list
def flat_list(lists):
    return [item for sublist in lists for item in sublist]
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to convert coupled list to dictionary
def convert_list2dict(list_keys, list_values):
    dictionary = {k: v for k, v in zip(list_keys, list_values)}
    return dictionary
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to create a random string
def random_string(string_root='temporary', string_separator='_', rand_min=0, rand_max=1000):

    # Generate random number
    rand_n = str(randint(rand_min, rand_max))
    # Generate time now
    time_now = datetime.now().strftime('%Y%m%d-%H%M%S_%f')
    # Concatenate string(s) with defined separator
    rand_string = string_separator.join([string_root, time_now, rand_n])

    return rand_string
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# Method to add format(s) string (path or filename)
def fill_tags2string(string_raw, tags_format=None, tags_filling=None):

    apply_tags = False
    if string_raw is not None:
        for tag in list(tags_format.keys()):
            if tag in string_raw:
                apply_tags = True
                break

    if apply_tags:

        tags_format_tmp = deepcopy(tags_format)
        for tag_key, tag_value in tags_format.items():
            tag_key_tmp = '{' + tag_key + '}'
            if tag_value is not None:
                if tag_key_tmp in string_raw:
                    string_filled = string_raw.replace(tag_key_tmp, tag_value)
                    string_raw = string_filled
                else:
                    tags_format_tmp.pop(tag_key, None)

        dim_max = 1
        for tags_filling_values_tmp in tags_filling.values():
            if isinstance(tags_filling_values_tmp, list):
                dim_tmp = tags_filling_values_tmp.__len__()
                if dim_tmp > dim_max:
                    dim_max = dim_tmp

        string_filled_list = [string_filled] * dim_max

        string_filled_def = []
        for string_id, string_filled_step in enumerate(string_filled_list):
            for tag_format_name, tag_format_value in list(tags_format_tmp.items()):

                if tag_format_name in list(tags_filling.keys()):
                    tag_filling_value = tags_filling[tag_format_name]

                    if isinstance(tag_filling_value, list):
                        tag_filling_step = tag_filling_value[string_id]
                    else:
                        tag_filling_step = tag_filling_value

                    if tag_filling_step is not None:

                        if isinstance(tag_filling_step, datetime):
                            tag_filling_step = tag_filling_step.strftime(tag_format_value)

                        if isinstance(tag_filling_step, (float, int)):
                            tag_filling_step = tag_format_value.format(tag_filling_step)

                        string_filled_step = string_filled_step.replace(tag_format_value, tag_filling_step)

            string_filled_def.append(string_filled_step)

        if dim_max == 1:
            string_filled_out = string_filled_def[0].replace('//', '/')
        else:
            string_filled_out = []
            for string_filled_tmp in string_filled_def:
                string_filled_out.append(string_filled_tmp.replace('//', '/'))

        return string_filled_out
    else:
        return string_raw
# -------------------------------------------------------------------------------------
