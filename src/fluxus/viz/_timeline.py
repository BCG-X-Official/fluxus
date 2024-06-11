"""
Implementation of the Timeline drawer.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable
from typing import Any

from pytools.api import inheritdoc
from pytools.text import format_table
from pytools.viz import Drawer, MatplotStyle, TextStyle
from pytools.viz.util import FittedText

from ..functional import RunResult
from ..functional.product import DictProduct
from .base import TimelineStyle

log = logging.getLogger(__name__)

__all__ = [
    "TimelineDrawer",
    "TimelineTextStyle",
    "TimelineMatplotStyle",
]


@inheritdoc(match="[see superclass]")
class TimelineDrawer(Drawer[RunResult, TimelineStyle]):
    """
    A drawer for rendering timelines.
    """

    @classmethod
    def get_style_classes(cls) -> Iterable[type[TimelineStyle]]:
        """[see superclass]"""
        return [TimelineTextStyle, TimelineMatplotStyle]

    @classmethod
    def get_default_style(cls) -> TimelineStyle:
        """[see superclass]"""
        return TimelineTextStyle()

    def _draw(self, data: RunResult) -> None:
        outputs: list[
            # a list of individual flow outputs
            list[
                # a list of steps in a single flow output
                tuple[
                    # a single step in a flow output
                    int,  # the path index
                    str,  # the step name
                    float,  # the start time
                    float,  # the end time
                ]
            ]
        ] = [
            [
                (
                    # the path index
                    path_index,
                    # the step name
                    step,
                    # the start time
                    step_result[DictProduct.KEY_START_TIME],
                    # the end time
                    step_result[DictProduct.KEY_END_TIME],
                )
                for step, step_result in output.items()
            ]
            for path_index, output in (
                (path_index, output)
                for path_index, path_outputs in enumerate(data.get_outputs_per_path())
                for output in path_outputs
            )
        ]

        style = self.style

        # the index of the flow output
        output_index: int
        # a list of steps leading up to a single output
        execution: list[tuple[int, str, float, float]]

        for output_index, execution in enumerate(outputs):
            style.render_timeline(output_index, execution)


@inheritdoc(match="[see superclass]")
class TimelineTextStyle(TimelineStyle, TextStyle):
    """
    A style for rendering timelines as plain text.
    """

    #: The header of the timeline table
    TABLE_HEADER = ["Output", "Path", "Step", "Start", "End"]

    #: The format of the columns in the timeline table
    COLUMN_FORMATS = ["d", "d", "s", "g", "g"]

    #: The alignment of the columns in the timeline table
    COLUMN_ALIGNMENT = [">", ">", "<", ">", ">"]

    #: The rows of the timeline table
    _table_rows: list[tuple[int, int, str, float, float]]

    def start_drawing(self, *, title: str, **kwargs: Any) -> None:
        """[see superclass]"""
        super().start_drawing(title=title, **kwargs)
        self._table_rows = []

    def render_timeline(
        self, output_index: int, timeline: Iterable[tuple[int, str, float, float]]
    ) -> None:
        """[see superclass]"""
        self._table_rows.extend(
            (output_index, path_index, step, start, end)
            for path_index, step, start, end in timeline
        )

    def finalize_drawing(self, **kwargs: Any) -> None:
        """[see superclass]"""
        self.out.write(
            format_table(
                headings=self.TABLE_HEADER,
                data=self._table_rows,
                formats=self.COLUMN_FORMATS,
                alignment=self.COLUMN_ALIGNMENT,
            )
        )
        del self._table_rows
        super().finalize_drawing(**kwargs)


@inheritdoc(match="[see superclass]")
class TimelineMatplotStyle(TimelineStyle, MatplotStyle):  # pragma: no cover
    """
    A style for rendering timelines as a horizontal bar chart in a Matplotlib plot.
    """

    # The minimum time in the timeline
    _min_time: float
    # The maximum time in the timeline
    _max_time: float
    # The maximum output index
    _max_output_index: int

    def start_drawing(self, *, title: str, **kwargs: Any) -> None:
        """[see superclass]"""
        super().start_drawing(title=title, **kwargs)
        self._min_time = math.inf
        self._max_time = -math.inf
        self._max_output_index = 0

    def render_timeline(
        self, output_index: int, timeline: Iterable[tuple[int, str, float, float]]
    ) -> None:
        """[see superclass]"""

        start_time_all: tuple[float, ...]
        end_time_all: tuple[float, ...]
        start_time_all, end_time_all, step_all = zip(
            *(
                (start_time, end_time, step)
                for _, step, start_time, end_time in timeline
            )
        )

        self._min_time = min(self._min_time, *start_time_all)
        self._max_time = max(self._max_time, *end_time_all)
        self._max_output_index = max(self._max_output_index, output_index)

        colors = self.colors
        ax = self.ax

        # Calculate the coordinates of the bars
        n = len(start_time_all)
        y = -output_index
        x_all = start_time_all
        width_all = [
            end_time - start_time
            for start_time, end_time in zip(start_time_all, end_time_all)
        ]
        color_all = [
            colors.accent_1 if i % 2 == 0 else colors.accent_2 for i in range(n)
        ]

        # Draw the bars
        ax.barh(
            y=y,
            width=width_all,
            left=x_all,
            height=0.8,
            # fill color is accent 1 for even steps, accent 2 for odd steps
            color=color_all,
            # black edge color, so that the bars are visually separated
            edgecolor=colors.background,
        )

        # Draw the text
        for x, width, color, step in zip(x_all, width_all, color_all, step_all):
            ax.add_artist(
                FittedText(
                    x=x + width / 2,
                    y=y,
                    width=width,
                    height=0.8,
                    text=step,
                    ha="center",
                    va="center",
                    color=colors.contrast_color(color),
                )
            )

    def finalize_drawing(self, **kwargs: Any) -> None:
        """[see superclass]"""

        ax = self.ax

        # Set the x axis limits to the minimum and maximum times
        if self._min_time != math.inf and self._max_time != -math.inf:
            ax.set_xlim(self._min_time, self._max_time)

        # Set axis labels
        ax.set_xlabel("Seconds")
        ax.set_ylabel("Output")

        # Set the y-axis ticks to the output indices, in descending order
        ax.set_yticks(list(range(-self._max_output_index, 1)))
        ax.set_yticklabels(list(map(str, reversed(range(self._max_output_index + 1)))))

        super().finalize_drawing(**kwargs)
