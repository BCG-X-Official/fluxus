"""
Implementation of visualization base classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from typing import Any

from pytools.viz import DrawingStyle

from ...base import Conduit

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
