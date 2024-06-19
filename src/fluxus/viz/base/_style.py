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
Implementation of visualization base classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from typing import Any

from pytools.viz import DrawingStyle

from ...core import Conduit

log = logging.getLogger(__name__)

__all__ = [
    "FlowStyle",
    "TimelineStyle",
]


#
# CLasses
#


class FlowStyle(DrawingStyle, metaclass=ABCMeta):
    """
    A style for rendering flows.
    """

    @abstractmethod
    def render_flow(self, flow: Conduit[Any]) -> None:
        """
        Render the given flow.

        :param flow: the flow to render
        """


class TimelineStyle(DrawingStyle, metaclass=ABCMeta):
    """
    A style for rendering timelines.
    """

    @abstractmethod
    def render_timeline(
        self, output_index: int, timeline: Iterable[tuple[int, str, float, float]]
    ) -> None:
        """
        Render the timeline of steps leading up to individual flow outputs.

        :param output_index: the index of the output
        :param timeline: the timeline to render, as an iterable of quadruples specifying
            the path index, step name, the start time, and the end time
        """
