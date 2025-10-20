"""
Library Features:

Name:          lib_utils_generic
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20250616'
Version:       '1.0.0'
"""
# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import re
import os
import tempfile
import numpy as np
import xarray as xr
import pandas as pd
from pathlib import Path

from collections import OrderedDict
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from datetime import datetime
from typing import Dict, Any, Optional, Union, List

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)

# initialize variables
attrs_decoded = []
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to detect token
def detect_token(s: str, tokens: list[str]):
    found = [tok for tok in tokens if tok in s]
    if len(found) > 1:
        raise ValueError(f"Multiple tokens found: {found}")
    elif len(found) == 0:
        return None
    else:
        return found[0]
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to validate datetime index
def validate_time_index(*indexes):
    return all(
        isinstance(idx, pd.DatetimeIndex) and not idx.empty
        for idx in indexes if idx is not None
    )
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to get common keys of dict(s)
def get_common_time_index(*indexes, reverse=True):

    if not indexes:
        return pd.DatetimeIndex([])

    # ensure all inputs are DatetimeIndex
    for i, idx in enumerate(indexes):
        if not isinstance(idx, pd.DatetimeIndex):
            raise TypeError(f"Argument {i + 1} is not a pandas.DatetimeIndex")

    # intersection across all indexes
    common = indexes[0]
    for idx in indexes[1:]:
        common = common.intersection(idx)

    return common.sort_values(ascending=not reverse)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to extrac timestamp from filenames
def extract_timestamps_from_filenames(files, date_ref=None, date_format="%Y%m%d%H%M", date_ascending=False):
    """
    Extract timestamps from filenames according to a specified datetime format.

    Expected pattern: the date/time token is the 3rd element (index 2) when splitting
    the filename by underscores, e.g.:
        indicators_rain_202510091500_alert_area_a.csv

    Args:
        files (list[Path] | list[str]): list of file paths
        date_format (str): datetime format string (default "%Y%m%d%H%M")
        date_ref (pd.Timestamp): datetime reference
        date_ascending (bool): timestamps sorting

    Returns:
        pd.Series: Series mapping each Path to its parsed datetime, sorted by datetime.
    """
    timestamps = []
    for f in files:
        f = Path(f)
        parts = f.stem.split('_')
        try:
            date_str = parts[2]
            ts = datetime.strptime(date_str, date_format)

            if date_ref is not None:
                if ts <= date_ref:
                    timestamps.append(pd.Timestamp(ts))
            else:
                timestamps.append(pd.Timestamp(ts))
        except (IndexError, ValueError):
            # skip files that don't match expected format
            continue

    timestamps = pd.DatetimeIndex(timestamps)
    timestamps = timestamps.sort_values(ascending=date_ascending)

    if timestamps.empty:
        timestamps = None

    return timestamps
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to list file and filter by tags
def get_files_with_tags(folder_path, tags=None, recursive=True):
    """
    List all files inside a folder and filter by a list of strings (tags).

    Args:
        folder_path (str | Path): The path to the folder.
        tags (list[str], optional): List of substrings that must appear in the filename.
                                    If None or empty, returns all files.
        recursive (bool, optional): If True, searches subdirectories recursively.

    Returns:
        list[Path]: List of matching file paths.
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Path not found: {folder_path}")

    pattern = "**/*" if recursive else "*"
    files = [f for f in folder.glob(pattern) if f.is_file()]

    if tags:
        files = [
            f for f in files
            if all(tag.lower() in f.name.lower() for tag in tags)
        ]

    return files
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to split string by token
def split_string_by_token(full_string: str, token: str):
    """
    Split a path string into two parts based on a placeholder token.

    Args:
        full_string (str): The complete path string.
        token (str): The placeholder token to split by (e.g. "{run_sub_path_time}").

    Returns:
        tuple[str, str]: (base, sub)
            - base: path up to and including the token
            - sub:  remaining part (starts with '/' if present)
        If the token is not found, returns (full_string, '').

    Example:
        >>> split_string_by_token("/data/path/{run_sub_path_time}/2024/01", "{run_sub_path_time}")
        ('/data/path/{run_sub_path_time}', '/2024/01')
        >>> split_string_by_token("/data/path/{run_sub_path_time}/2024/01", "run_sub_path_time")
        ('/data/path/{run_sub_path_time}', '/2024/01')
        >>> split_string_by_token("/data/path/static/2024/01", "{run_sub_path_time}")
        ('/data/path/static/2024/01', '')
    """
    # --- Normalize token to ensure it has { } ---
    token_stripped = token.strip()
    if not token_stripped.startswith("{"):
        token_stripped = "{" + token_stripped
    if not token_stripped.endswith("}"):
        token_stripped = token_stripped + "}"

    token = token_stripped

    # --- Check if token exists in full_string ---
    if token not in full_string:
        # Return original unchanged path and empty subpath
        return full_string, ''

    # --- Perform the split ---
    base, _, remainder = full_string.partition(token)
    base = base + token
    sub = remainder if remainder.startswith('/') else '/' + remainder if remainder else ''
    return base, sub
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to format sub path by time
def format_sub_path_by_time(ts: pd.Timestamp, pattern: str) -> str:
    """
    Format a run_sub_path_time pattern (like %Y/%m/%d/%H00 or %Y/%m/%d/*00)
    using a pandas Timestamp.

    Args:
        ts (pd.Timestamp): The timestamp to use for formatting.
        pattern (str): A strftime-like pattern that may include '*' wildcards,
                       e.g. "%Y/%m/%d/%H00/" or "%Y/%m/%d/*00/"

    Returns:
        str: The formatted sub-path string.
    """
    # ensure Timestamp object
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)

    # only strftime parts will be replaced; '*' remains as-is
    return ts.strftime(pattern)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to extract time stamps from paths
def extract_timestamps_from_paths(path_template: str, tokens: dict,
                             ascending: bool = True, path_format="%Y/%m/%d/%H%M") -> (pd.DatetimeIndex, None):

    # --- Timezone setup ---
    tz_name = tokens.get("tz")
    tz = None
    if tz_name and ZoneInfo:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = None

    found = False
    for token in tokens:
        if token in path_template:
            found = True
            break

    if found:

        # --- Fill template ---
        try:
            filled = os.path.expanduser(path_template.format(**tokens))
        except KeyError as e:
            raise ValueError(f"Missing token for template: {e}")

        filled = os.path.normpath(filled) if "*" not in filled else os.path.normpath(
            filled.replace("*", "*")
        )
    else:
        filled = path_template

    def format_to_regex(
            fmt: str,
            *,
            anchor: str = "exact",  # "exact" | "end" | "none"
            allow_trailing_slash: bool = False,
    ) -> str:
        """
        Convert a strftime-like format (even partial ones) into a regex with named captures.

        Supported tokens:
          %Y  ->  4-digit year      (?P<Y>\\d{4})
          %m  ->  2-digit month     (?P<m>\\d{2})
          %d  ->  2-digit day       (?P<d>\\d{2})
          %H  ->  2-digit hour      (?P<H>\\d{2})
          %M  ->  2-digit minute    (?P<M>\\d{2})

        anchor: "exact" (default) -> ^...$
                "end"             -> ...$
                "none"            -> no anchors (good for re.search)
        allow_trailing_slash: if True, makes a final '/' optional

        Examples:
            format_to_regex("%Y/%m/", anchor="exact")
                -> ^(?P<Y>\\d{4})/(?P<m>\\d{2})/$
            format_to_regex("%Y/%m/%d/%H", anchor="end")
                -> (?P<Y>\\d{4})/(?P<m>\\d{2})/(?P<d>\\d{2})/(?P<H>\\d{2})$
            format_to_regex("%Y/%m/%d/%H00", anchor="none")
                -> (?P<Y>\\d{4})/(?P<m>\\d{2})/(?P<d>\\d{2})/(?P<H>\\d{2})00
        """

        mapping = {
            "%Y": r"(?P<Y>\d{4})",
            "%m": r"(?P<m>\d{2})",
            "%d": r"(?P<d>\d{2})",
            "%H": r"(?P<H>\d{2})",
            "%M": r"(?P<M>\d{2})",
        }

        # Preserve literal percent signs written as '%%'
        PCT = "\x00PCT\x00"
        fmt = fmt.replace("%%", PCT)

        # Escape all literal characters
        regex = re.escape(fmt)

        # Replace supported tokens with their regex equivalents
        for token, repl in mapping.items():
            regex = regex.replace(re.escape(token), repl)

        # Normalize forward slashes (re.escape escapes them)
        regex = regex.replace(r"\/", "/")

        # Restore literal percent signs
        regex = regex.replace(PCT, "%")

        # Optionally allow a trailing slash
        if allow_trailing_slash and regex.endswith("/"):
            # If fmt ends with '/', make it optional
            regex = f"{regex[:-1]}/?"

        # Apply anchoring
        if anchor == "exact":
            regex = f"^{regex}$"
        elif anchor == "end":
            regex = f"{regex}$"
        elif anchor == "none":
            pass
        else:
            raise ValueError("anchor must be 'exact', 'end', or 'none'")

        # Sanity check: no stray % tokens remain
        if "%" in regex:
            # There is at least one unsupported strftime-like token
            raise ValueError("Unsupported format token found in fmt.")

        return regex

    def extract_timestamp(
            path: str, fmt="%Y/%m/%d/%H%M", tz="Europe/Rome", include_offset=False) -> dict:
        """
        Extract a timestamp from a file path based on a strftime-like format.
        - Automatically ignores missing parts (e.g., no %H or no %Y)
        - Optionally appends the timezone offset (e.g. '+0200') if include_offset=True
        """
        pattern = format_to_regex(fmt, anchor='none', allow_trailing_slash=True)
        m = re.search(pattern, path)
        if not m:
            return None

        # Rebuild matched string using only available parts in fmt
        matched = fmt
        for key, value in m.groupdict().items():
            if value is not None:
                matched = matched.replace(f"%{key}", value)

        # Convert to datetime safely
        try:
            ts = pd.to_datetime(matched, format=fmt)
        except ValueError:
            ts = pd.to_datetime(matched, errors="coerce")

        if pd.isnull(ts):
            return None

        # Optionally add the offset explicitly
        if include_offset:
            # Localize to timezone
            ts = ts.tz_localize(tz)
            # get offset string like +0200
            offset = ts.strftime("%z")
            # append it to timestamp string
            ts_str = ts.strftime(f"{fmt} {offset}")
            # parse back into timezone-aware Timestamp
            ts = pd.to_datetime(ts_str, format=f"{fmt} %z")

        return ts

    # --- No wildcard case ---
    if "*" not in filled:

        abs_path = os.path.abspath(filled)
        if not os.path.exists(abs_path):
            return None
        time_key = extract_timestamp(abs_path, fmt=path_format)

        time_obj = {pd.Timestamp(time_key): abs_path}
        return time_obj

    # --- Wildcard handling ---
    prefix = filled.split("*", 1)[0]
    if prefix.endswith(os.sep):
        parent = prefix.rstrip(os.sep) or os.sep
        name_prefix = ""
    else:
        parent = os.path.dirname(prefix) or os.sep
        name_prefix = os.path.basename(prefix)

    if not os.path.isdir(parent):
        return None

    # --- Collect candidates ---
    collected = []
    for name in os.listdir(parent):
        if not name.startswith(name_prefix):
            continue
        path = os.path.join(parent, name)
        if not os.path.isdir(path):
            continue

        # to remove * from string according to the expected folders tree
        filled_format = fill_star_with_format(path_format)

        ts = extract_timestamp(path, fmt=filled_format)

        collected.append((ts, os.path.abspath(path)))

    candidates = []
    for c in collected:
        if c[0] is not None:
            candidates.append(c)
        else:
            pass

    candidates.sort(key=lambda t: t[0])

    # --- Sort and ensure unique timestamps ---
    time_obj = {}
    for ts, path in candidates:
        if ts not in list(time_obj.keys()):
            time_obj[ts] = path

    time_obj = dict(sorted(time_obj.items(), reverse=ascending))

    return time_obj

# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to fill star by the time tag
def fill_star_with_format(pattern: str) -> str:
    """
    Replace '*' in a date-like pattern with the appropriate strftime tag
    (%Y, %m, %d, %H) inferred by its position.
    """
    parts = pattern.split('/')
    for i, part in enumerate(parts):
        if '*' in part:
            if i == 0:
                parts[i] = part.replace('*', '%Y')
            elif i == 1:
                parts[i] = part.replace('*', '%m')
            elif i == 2:
                parts[i] = part.replace('*', '%d')
            else:
                # time component, like "*00"
                parts[i] = part.replace('*', '%H')
    return '/'.join(parts)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to extract subkeys from a dictionary
def extract_subkeys(data, subkeys, invert=False, keep_keys=False):
    if isinstance(subkeys, str):
        subkeys = [subkeys]

    result = {}
    for k, v in data.items():
        extracted = {s: v[s] for s in subkeys if s in v}
        if not extracted:
            continue

        # Decide the value format
        if keep_keys:
            value = extracted if len(extracted) > 1 else list(extracted.items())[0]
        else:
            value = tuple(extracted.values()) if len(extracted) > 1 else list(extracted.values())[0]

        result[k] = value

    if invert:
        result = {v: k for k, v in result.items()}

    return result
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to get list elements
def get_list_elements(lst, expected_count=None, allow_multiple=True):

    list_elements = list(set(lst))
    count = len(list_elements)

    if expected_count is not None and count != expected_count:
        raise ValueError(f"Expected {expected_count} unique elements, but found {count}.")

    if count == 1:
        return list_elements[0]

    if not allow_multiple:
        raise ValueError(f"Multiple unique elements found ({count}), but allow_multiple is False.")

    return list_elements
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to add fields to a flattened dictionary
def fields2dict(fields: dict, extra_fields: dict = None, extra_formats: dict = None) -> Dict[str, Any]:
    # Add extra fields
    if (extra_fields is not None) and extra_fields:
        for key, value in extra_fields.items():

            fmt = None
            if key in list(extra_formats.keys()):
                fmt = extra_formats[key]

            if isinstance(value, (datetime, pd.Timestamp)):
                if fmt is not None:
                    value = value.strftime(fmt)
            fields[key] = value

    return fields
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert a nested dictionary to a flattened dictionary
def dict2flat(d, parent_key='', sep=':'):
    """Flatten a nested dictionary using a delimiter between keys."""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(dict2flat(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to convert a flattened dictionary back to a nested dictionary
def flat2dict(d, sep=':'):
    """Convert a flattened dictionary back to a nested dictionary."""
    result = {}
    for flat_key, value in d.items():
        keys = flat_key.split(sep)
        current = result
        for part in keys[:-1]:
            if part not in current or not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]
        current[keys[-1]] = value
    return result
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to extract subpart of a dictionary
def extract_subpart(group: Dict[str, Dict[str, Any]], sub_keys: Union[str, List[str]]) -> Dict[str, Any]:
    if isinstance(sub_keys, str):
        sub_keys = [sub_keys]

    result = {}
    for key, area in group.items():
        current = area
        for sub_key in sub_keys:
            if isinstance(current, dict) and sub_key in current:
                current = current[sub_key]
            else:
                log_stream.warning(f" ===> Key path {sub_keys} not found for group '{key}' at '{sub_key}'")
                current = {}
                break
        result[key] = current

    return result
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create a temporary file name
def create_filename_tmp(prefix='tmp_', suffix='.tiff', folder=None):
    if folder is None:
        folder = '/tmp'
    with tempfile.NamedTemporaryFile(dir=folder, prefix=prefix, suffix=suffix, delete=False) as tmp:
        temp_file_name = tmp.name
    return temp_file_name
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# method to create a data array
def create_darray(data: np.ndarray,
                  geo_x: Union[np.ndarray, xr.DataArray],
                  geo_y: Union[np.ndarray, xr.DataArray],
                  geo_1d: bool = True,
                  time: Optional[pd.DatetimeIndex] = None,
                  coord_name_x: str = 'longitude',
                  coord_name_y: str = 'latitude',
                  coord_name_time: str = 'time',
                  dim_name_x: str = 'longitude',
                  dim_name_y: str = 'latitude',
                  dim_name_time: str = 'time',
                  dims_order: Optional[List[str]] = None) -> xr.DataArray:

    if dims_order is None:
        dims_order = [dim_name_y, dim_name_x]
    if time is not None:
        if dims_order is None:
            dims_order = [dim_name_time, dim_name_y, dim_name_x]

    if time is not None:
        time_detected = False
        for dim_len in list(data.shape):
            if dim_len == len(time):
                time_detected = True
    else:
        time_detected = None

    if time_detected is not None:
        if not time_detected:
            log_stream.error(' ===> Data time dimension does not match time coordinates')
            raise ValueError('Mismatch between data and time dimension')

    if geo_1d:
        if geo_x.ndim == 2:
            geo_x = geo_x[0, :]
        if geo_y.ndim == 2:
            geo_y = geo_y[:, 0]

        if isinstance(geo_x, xr.DataArray):
            geo_x = geo_x.values
        elif not isinstance(geo_x, np.ndarray):
            log_stream.error(' ===> Geographical object x format is not supported')
            raise NotImplementedError('Case not implemented yet')

        if isinstance(geo_y, xr.DataArray):
            geo_y = geo_y.values
        elif not isinstance(geo_y, np.ndarray):
            log_stream.error(' ===> Geographical object y format is not supported')
            raise NotImplementedError('Case not implemented yet')

        # check if time is defined or not
        if time is None:

            # create 2d data array
            data_da = xr.DataArray(data,
                                   dims=dims_order,
                                   coords={coord_name_x: (dim_name_x, geo_x),
                                           coord_name_y: (dim_name_y, geo_y)})
        elif isinstance(time, pd.DatetimeIndex) or isinstance(time, list):

            if isinstance(time, list):
                time = pd.DatetimeIndex(time)

            if data.ndim == 2:
                data = np.expand_dims(data, axis=0)

            # create 3d data array
            data_da = xr.DataArray(data,
                                   dims=dims_order,
                                   coords={coord_name_x: (dim_name_x, geo_x),
                                           coord_name_y: (dim_name_y, geo_y),
                                           coord_name_time: (dim_name_time, time)})
        else:
            log_stream.error(' ===> Time obj is in wrong format')
            raise IOError('Variable time format not valid')

    else:
        log_stream.error(' ===> Longitude and Latitude must be 1d')
        raise IOError('Variable shape is not valid')

    return data_da
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# method to fill template string
def fill_template_string(template_str: str,
                         template_map: dict,
                         value_map: dict) -> str:
    """
    Fill a template string using values from template_map (providing formats) and
    value_map (providing actual values). Only placeholders present in both maps are used.

    Parameters:
        template_str (str): Template string with placeholders (e.g., '{source_date}', '{region}')
        template_map (dict): Dict with keys as placeholders and values as format strings or literals
        value_map (dict): Dict with keys and actual values to be formatted

    Returns:
        str: Final string with placeholders filled. Skips unresolved keys with warnings.

    Raises:
        ValueError: If template_str is None.
    """

    if template_str is None:
        log_stream.error(" ===> The variable 'template_str' should not be None.")
        raise ValueError("Check the template_str variable and provide a valid string")

    matches = re.findall(r"{(\w+)}", template_str)
    filled_values = {}

    for key in matches:
        if key in template_map and key in value_map:
            fmt = template_map[key]
            val = value_map[key]

            if isinstance(fmt, str) and "%" in fmt:
                # Treat as datetime formatting
                if isinstance(val, pd.Timestamp):
                    filled_values[key] = val.strftime(fmt)
                else:
                    log_stream.warning(
                        f" ===> Expected pd.Timestamp for key '{key}' to apply datetime format '{fmt}'."
                    )
                    filled_values[key] = str(val)
            else:
                # Use as literal format override
                filled_values[key] = val
        else:
            log_stream.warning(f" ===> Skipping placeholder '{{{key}}}' — not present in both template_map and value_map.")
            filled_values[key] = '{' + key + '}'  # Keep unresolved placeholders as-is

    try:
        return template_str.format(**filled_values)
    except KeyError as e:
        log_stream.error(f" ===> Formatting error: missing key {e}")
        raise
# ----------------------------------------------------------------------------------------------------------------------

