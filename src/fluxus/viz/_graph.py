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
Implementation of flow graphs.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator
from typing import Any, Literal

from typing_extensions import Never

from pytools.api import inheritdoc
from pytools.viz.color import RgbColor, RgbaColor

from .. import Consumer, Producer
from ..core import Conduit, SerialConduit
from ..core.producer import BaseProducer
from ..core.transformer import BaseTransformer
from ..util import simplify_repr_attributes

log = logging.getLogger(__name__)

__all__ = [
    "FlowGraph",
]


#
# Classes
#


class FlowGraph:
    """
    A graph representation of a :class:`.Flow`, or a partial flow represented
    by any type of :class:`.Conduit`.
    """

    #: The set of all single, unconnected conduits in the graph.
    single_conduits: set[SerialConduit[Any]]

    #: The set of all connections between conduits in the graph.
    connections: set[tuple[SerialConduit[Any], SerialConduit[Any]]]

    def __init__(
        self,
        *,
        single_conduits: set[SerialConduit[Any]] | None = None,
        connections: set[tuple[SerialConduit[Any], SerialConduit[Any]]] | None = None,
    ) -> None:
        """
        :param single_conduits: the set of all single, unconnected conduits in the graph
            (optional)
        :param connections: the set of all connections between conduits in the graph
            (optional)
        """
        self.single_conduits = single_conduits or set()
        self.connections = connections or set()

    @classmethod
    def from_conduit(cls, conduit: Conduit[Any]) -> FlowGraph:
        """
        Create a flow graph from a conduit.

        :param conduit: the conduit to create the flow graph from
        :return: the flow graph
        """
        # complete the flow if it is not complete

        # we have no leading producer, so we add one
        if isinstance(conduit, (BaseTransformer, Consumer)):
            # noinspection PyTypeChecker
            conduit = _StartNode() >> conduit

        # we have no trailing consumer, so we add one
        if isinstance(conduit, BaseProducer):
            conduit = conduit >> _EndNode()

        connections: set[tuple[SerialConduit[Any], SerialConduit[Any]]] = set(
            conduit.get_connections(ingoing=[])
        )

        single_conduits: set[SerialConduit[Any]] = {
            conduit
            for conduit in conduit.iter_concurrent_conduits()
            if conduit.is_atomic
        }

        return FlowGraph(connections=connections, single_conduits=single_conduits)

    def to_dot(
        self,
        *,
        width: float | None = 7.5,
        font: str | None = None,
        fontsize: float | None = None,
        fontcolor: RgbColor | RgbaColor | None = None,
        fontcolor_terminal: RgbColor | RgbaColor | None = None,
        background: RgbColor | RgbaColor | None = None,
        foreground: RgbColor | RgbaColor | None = None,
        fill: RgbColor | RgbaColor | None = None,
        stroke: RgbColor | RgbaColor | None = None,
    ) -> str:
        """
        Convert this flow graph to a DOT graph representation.

        :param width: the width of the graph in inches, or ``None`` for unconstrained
            width (defaults to 7.5 inches, fitting letter or A4 pages in portrait mode)
        :param font: the font of the graph (optional)
        :param fontsize: the font size (optional)
        :param fontcolor: the text color of the graph (defaults to the foreground color)
        :param fontcolor_terminal: the text color of the terminal nodes (defaults to the
            font color of regular nodes)
        :param background: the background color of the graph (optional)
        :param foreground: the foreground color of the graph (optional)
        :param fill: the fill color of the nodes (optional)
        :param stroke: the stroke color of the nodes and edges (optional)
        :return: the DOT graph representation
        """

        # get all connections
        connections = self.connections
        # get the sets of source nodes and processor nodes
        nodes_source = {producer for producer, _ in connections}
        nodes_processor = {processor for _, processor in connections}
        # all nodes are the union of the two sets, plus the single conduits
        nodes = nodes_source | nodes_processor | self.single_conduits

        def _node_id(_node: Conduit[Any]) -> str:
            return f"{type(_node).__name__}_{id(_node)}"

        globals = dict(
            rankdir="LR",
            labeljust="l",
        )
        if width is not None:
            globals["width"] = f"{width:g}"
        node_defaults = dict(
            shape="box",
            style="rounded",
        )
        edge_defaults = dict()

        if background:
            globals["bgcolor"] = background.hex
        if foreground:
            node_defaults["color"] = node_defaults["fontcolor"] = edge_defaults[
                "color"
            ] = foreground.hex
        if fontcolor:
            node_defaults["fontcolor"] = fontcolor.hex
        if fontsize:
            node_defaults["fontsize"] = f"{fontsize:g}"
        if fill:
            node_defaults["style"] += ",filled"
            node_defaults["fillcolor"] = fill.hex
        if stroke:
            node_defaults["color"] = edge_defaults["color"] = stroke.hex
        if font:
            node_defaults["fontname"] = font

        digraph = _DotGraph(
            "Flow",
            globals=globals,
            node_defaults=node_defaults,
            edge_defaults=edge_defaults,
        )

        for node in nodes:
            node_attrs = {}
            node_name = node.name
            if isinstance(node, _SpecialNode):
                # Special nodes at the start and end of the flow
                node_attrs["label"] = f"{node.name}\\n"
                node_attrs["shape"] = node.shape
                node_attrs["style"] = "dashed"
                if stroke:
                    # same color as the lines
                    node_attrs["fontcolor"] = node_defaults["color"]
            else:
                conduit_attributes = simplify_repr_attributes(
                    {k: v for k, v in node.get_repr_attributes().items() if k != "name"}
                )
                if conduit_attributes:
                    node_attrs["shape"] = "record"
                    attributes = r"\l".join(
                        f"{k}={v}" for k, v in conduit_attributes.items()
                    )
                    node_attrs["label"] = f"{node_name}|{attributes}\\l"
                else:
                    node_attrs["label"] = node_name

                if isinstance(node, (BaseProducer, Consumer)):
                    # Producers and consumers don't get filled, just rounded
                    node_attrs["style"] = "rounded"
                    if fontcolor_terminal:
                        node_attrs["fontcolor"] = fontcolor_terminal.hex

            digraph.add_node(_node_id(node), **node_attrs)

        for source, processor in connections:
            if isinstance(source, _SpecialNode) or isinstance(processor, _SpecialNode):
                edge_attrs = {"style": "dashed"}
            else:
                edge_attrs = {}
            digraph.add_edge(_node_id(source), _node_id(processor), **edge_attrs)

        return str(digraph)


#
# Auxiliary classes and functions
#


class _DotGraph:
    """
    A DOT graph representation.
    """

    #: The name of the graph.
    name: str

    #: The type of the graph, either "graph" or "digraph".
    graph_type: Literal["graph", "digraph"]

    #: The global attributes of the graph.
    globals: dict[str, str]

    #: The default attributes for nodes.
    node_defaults: dict[str, str]

    #: The default attributes for edges.
    edge_defaults: dict[str, str]

    #: The nodes of the graph, each with a dictionary of attributes.
    nodes: dict[str, dict[str, str]]

    #: The edges of the graph, each with a dictionary of attributes.
    edges: dict[tuple[str, str], dict[str, str]]

    def __init__(
        self,
        name: str,
        *,
        graph_type: Literal["graph", "digraph"] = "digraph",
        globals: dict[str, str] | None = None,
        node_defaults: dict[str, str] | None = None,
        edge_defaults: dict[str, str] | None = None,
    ) -> None:
        """
        :param name: the name of the graph
        :param graph_type: the type of the graph, either "graph" or "digraph"
            (default: "digraph")
        :param globals: global attributes of the graph (optional)
        :param node_defaults: default attributes for nodes (optional)
        :param edge_defaults: default attributes for edges (optional)
        """
        self.name = name
        self.graph_type = graph_type
        self.globals = globals or {}
        self.node_defaults = node_defaults or {}
        self.edge_defaults = edge_defaults or {}

        self.nodes = {}
        self.edges = {}

    def add_node(self, name: str, **attrs: str) -> None:
        """
        Add a node to the graph.

        :param name: the name of the node
        :param attrs: the attributes of the node, as additional keyword arguments
            (optional)
        """
        self.nodes[name] = attrs

    def add_edge(self, source: str, target: str, **attrs: str) -> None:
        """
        Add an edge to the graph.

        :param source: the source node of the edge
        :param target: the target node of the edge
        :param attrs: the attributes of the edge, as additional keyword arguments
            (optional)
        """
        self.edges[(source, target)] = attrs

    def __str__(self) -> str:

        def _escape_string(s: str) -> str:
            if s and all(c.isalnum() or c in "_-" for c in s):
                return s
            else:
                s.replace('"', '\\"')
                return f'"{s}"'

        def _attr_string(_attrs: dict[str, str]) -> str:
            return ",".join(
                f"{_attr}={_escape_string(_value)}" for _attr, _value in _attrs.items()
            )

        dot: str = f'{self.graph_type} "{self.name}" {{'
        for attr, value in self.globals.items():
            dot += f"\n    {attr}={_escape_string(value)};"  # noqa: E702
        if self.node_defaults:
            dot += f"\n    node [{_attr_string(self.node_defaults)}];"  # noqa: E702
        if self.edge_defaults:
            dot += f"\n    edge [{_attr_string(self.edge_defaults)}];"  # noqa: E702
        for node, attrs in self.nodes.items():
            dot += f"\n    {_escape_string(node)}"
            if attrs:
                dot += f" [{_attr_string(attrs)}];"  # noqa: E702
        for (source, target), attrs in self.edges.items():
            dot += f"\n    {_escape_string(source)} -> {_escape_string(target)}"
            if attrs:
                dot += f"[{_attr_string(attrs)}];"  # noqa: E702

        dot += "\n}"
        return dot


class _SpecialNode(Conduit[Never], metaclass=ABCMeta):
    """
    A special node that is not part of the flow graph.
    """

    @property
    @abstractmethod
    def shape(self) -> str:
        """
        The shape of the node.
        """


@inheritdoc(match="[see superclass]")
class _StartNode(_SpecialNode, Producer[Never]):
    """
    An input producer that produces a single product.
    """

    @property
    def name(self) -> str:
        """[see superclass]"""
        # A greater than symbol evokes 'start'
        return ">"

    @property
    def shape(self) -> str:
        """[see superclass]"""
        return "circle"

    def produce(self) -> Iterator[Never]:  # pragma: no cover
        """
        Yield nothing.
        """
        yield from ()


@inheritdoc(match="[see superclass]")
class _EndNode(_SpecialNode, Consumer[Any, Never]):
    """
    An output consumer that consumes a single product.
    """

    @property
    def name(self) -> str:
        """[see superclass]"""
        # We use mathematical angle brackets to make this node visually distinct from
        # other nodes.

        # symbol evokes 'pause' or 'stop'
        return "||"

    @property
    def shape(self) -> str:
        """[see superclass]"""
        return "doublecircle"

    def consume(self, products: Iterable[Any]) -> Never:  # pragma: no cover
        """
        Consume anything, return nothing.

        :param products: the products to consume
        :return: never returns
        :raises NotImplementedError: always
        """
        raise NotImplementedError("Output consumer cannot consume any products")
