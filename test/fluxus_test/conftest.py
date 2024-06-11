"""
Global test configuration
"""

import logging
import os

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DIR_TEST_ROOT = "test"
DIR_TEST_SOURCES = os.path.join(DIR_TEST_ROOT, "fluxus_test")

# validate that the test sources directory exists
if not os.path.exists(DIR_TEST_SOURCES):
    raise FileNotFoundError(
        f"Test sources directory does not exist: {DIR_TEST_SOURCES}. "
        "Make sure to set the working directory to the project root directory."
    )
