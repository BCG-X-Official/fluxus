"""
Implementation of drawers for conduits and flows.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from io import BytesIO
from typing import Any, Generic, TypeVar

from pytools.api import inheritdoc
from pytools.viz import ColoredStyle, Drawer, TextStyle
from pytools.viz.color import ColorScheme

from ..base import Conduit
from ._graph import FlowGraph
from .base import FlowStyle

log = logging.getLogger(__name__)

__all__ = [
    "FlowDrawer",
    "FlowGraphStyle",
    "FlowTextStyle",
]

#
# Type variables
#


T_ColorScheme = TypeVar("T_ColorScheme", bound=ColorScheme)


#
# Classes
#


@inheritdoc(match="[see superclass]")
class FlowTextStyle(FlowStyle, TextStyle):
    """
    A style for rendering flows as text.
    """

    @classmethod
    def get_default_style_name(cls) -> str:
        return "text"

    def render_flow(self, flow: Conduit[Any]) -> None:
        """[see superclass]"""
        print(flow.to_expression(compact=True), file=self.out)


@inheritdoc(match="[see superclass]")
class FlowGraphStyle(FlowStyle, ColoredStyle[T_ColorScheme], Generic[T_ColorScheme]):
    """
    A style for rendering flows as graphs.

    The graph is rendered using the `graphviz` package, which must be installed
    for this style to work.

    If no filename is given, displays the graph using the `IPython.display`
    package which, when used in a Jupyter notebook, will display the graph
    inline.

    If a filename is given, writes the graph to a file.
    """

    #: A filename or file-like object to write the graph to (optional)
    file: str | BytesIO | None

    #: The format of the graph file (optional, defaults to "png")
    format: str | None

    def __init__(
        self,
        file: str | BytesIO | None = None,
        *,
        format: str | None = None,
        colors: T_ColorScheme | None = None,
    ) -> None:
        """
        :param file: a filename or file-like object to write the graph to (optional)
        :param format: the format of the graph file (optional, defaults to "png")
        :param colors: the color scheme to use (optional)
        """
        super().__init__(colors=colors)
        self.file = file
        self.format = format or "png"

    @classmethod
    def get_default_style_name(cls) -> str:
        return "graph"

    def render_flow(self, flow: Conduit[Any]) -> None:  # pragma: no cover
        """[see superclass]"""
        import graphviz

        # get the color scheme
        color_scheme = self.colors

        graph: graphviz.Source = graphviz.Source(
            FlowGraph.from_conduit(flow).to_dot(
                font="Monaco, Consolas, monospace",
                fontcolor=color_scheme.contrast_color(color_scheme.accent_1),
                fontsize=10,
                foreground=color_scheme.foreground,
                background=color_scheme.background,
                fill=color_scheme.accent_1,
                stroke=color_scheme.accent_2,
            )
        )
        # set the foreground color of the graph

        if self.file is None:
            from IPython.display import display

            display(graph)
        elif isinstance(self.file, str):
            graph.render(self.file, format=self.format, cleanup=True)
        else:
            self.file.write(graph.pipe(format=self.format))


@inheritdoc(match="[see superclass]")
class FlowDrawer(Drawer[Conduit[Any], FlowStyle]):
    """
    A drawer for flows.

    Available styles:

    - :class:`FlowTextStyle` or "text"
    - :class:`FlowGraphStyle` or one of "graph", "graph_dark"
    """

    @classmethod
    def get_style_classes(cls) -> Iterable[type[FlowStyle]]:
        """[see superclass]"""
        return [FlowTextStyle, FlowGraphStyle]

    @classmethod
    def get_default_style(cls) -> FlowStyle:
        """[see superclass]"""
        return FlowGraphStyle()

    def _draw(self, data: Conduit[Any]) -> None:
        self.style.render_flow(data)
