# -----------------------------------------------------------------------------
# © 2024 Boston Consulting Group. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------

"""
Conduits at the core of the functional flow API.

The conduits are:

- :class:`.DictProducer`
    A producer of dictionary products, as the starting point of a
    functional flow.
- :class:`.DictConsumer`
    A consumer of dictionary products, combining all produced
    dictionaries into a list of dictionaries.
- :class:`.Step`
    A step that transforms a dictionary product into one or more
    dictionary products.
"""

from ._consumer import *
from ._producer import *
from ._step import *
