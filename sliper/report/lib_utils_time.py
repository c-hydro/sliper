"""
Library Features:

Name:          lib_utils_time
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20251021'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# libraries
import logging
import pandas as pd
from typing import Optional, Tuple

from lib_info_args import logger_name

# logging
log_stream = logging.getLogger(logger_name)
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# Method to set time run
def set_time(time_alg: Optional[str] = None) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:

    # Get reference time
    time_now = pd.Timestamp.now().floor("h")
    # Set time run (fixed or current)
    time_run = pd.Timestamp(time_alg).floor("h") if time_alg else time_now
    # Set time day (00:00)
    time_day = time_run.normalize()

    return time_now, time_day, time_run
# ----------------------------------------------------------------------------------------------------------------------
