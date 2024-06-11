import asyncio
import logging
import re
from abc import ABCMeta
from collections import defaultdict
from collections.abc import AsyncIterable, Iterable, Iterator
from io import StringIO
from typing import Any, cast

import pytest
from fluxus import AsyncConsumer, Consumer, Passthrough, Producer, Transformer
from fluxus.base import Conduit
from fluxus.base.producer import ConcurrentProducer
from fluxus.base.transformer import BaseTransformer, ConcurrentTransformer
from fluxus.functional import parallel
from fluxus.viz import FlowGraph, FlowGraphStyle, FlowTextStyle

from pytools.expression import freeze
from pytools.expression.atomic import Id
from pytools.viz.color import RgbColor

log = logging.getLogger(__name__)


class NumberProducer(Producer[int]):
    """
    A simple producer of a range of integers.
    """

    def __init__(self, start: int, stop: int) -> None:
        """
        :param start: the first integer in the range
        :param stop: the last integer in the range
        """
        super().__init__()
        self.start = start
        self.stop = stop

    def iter(self) -> Iterator[int]:
        return iter(range(self.start, self.stop))


class NumberTransformer(Transformer[int, int], metaclass=ABCMeta):
    """
    A simple transformer that increments the input.
    """


class DoublingTransformer(NumberTransformer):
    """
    A simple transformer that doubles the input.
    """

    def transform(self, source_product: int) -> Iterator[int]:
        yield source_product
        yield source_product * 2


class IncrementingTransformer(NumberTransformer):
    """
    A simple transformer that increments the input.
    """

    def transform(self, source_product: int) -> Iterator[int]:
        yield source_product + 1


class StringConsumer(Consumer[str, str]):
    """
    Concatenates all strings as separate lines.
    """

    @property
    def input_type(self) -> type[str]:
        return str

    def consume(
        self,
        products: Iterable[tuple[int, str]],
    ) -> str:
        result: dict[int, list[str]] = defaultdict(list)

        for producer_index, product in products:
            result[producer_index].append(product)

        # sort the results by the key, then return the values as a list
        return "\n".join(line for key in sorted(result.keys()) for line in result[key])


class NumberConsumer(AsyncConsumer[int, list[list[int]]]):
    """
    A simple consumer that collects integers into a list.
    """

    @property
    def input_type(self) -> type[int]:
        return int

    async def aconsume(
        self,
        products: AsyncIterable[tuple[int, int]],
    ) -> list[list[int]]:
        result = defaultdict(list)

        async for producer_index, product in products:
            result[producer_index].append(product)

        # sort the results by the key, then return the values as a list
        return [result[key] for key in sorted(result.keys())]


def test_eq_hash() -> None:
    """
    Test the equality and hash methods.
    """

    assert NumberProducer(0, 4) != NumberProducer(0, 4)
    assert hash(NumberProducer(0, 4)) != hash(NumberProducer(0, 4))


def test_group_construction() -> None:
    """
    Test the construction of a group.
    """

    producer_group: ConcurrentProducer[int] = NumberProducer(0, 4) & NumberProducer(
        8, 12
    )
    assert isinstance(producer_group, ConcurrentProducer)
    assert producer_group.n_concurrent_conduits == 2
    producers = cast(
        tuple[NumberProducer, ...], tuple(producer_group.iter_concurrent_conduits())
    )
    assert len(producers) == 2
    assert all(isinstance(prod, NumberProducer) for prod in producers)
    assert producers[0].start == 0
    assert producers[0].stop == 4
    assert producers[1].start == 8
    assert producers[1].stop == 12

    transformer_group: BaseTransformer[int, int] = (
        DoublingTransformer() & DoublingTransformer() >> DoublingTransformer()
    )
    assert isinstance(transformer_group, ConcurrentTransformer)
    assert transformer_group.n_concurrent_conduits == 2
    transformers = tuple(transformer_group.iter_concurrent_conduits())
    assert len(transformers) == 2
    assert isinstance(transformers[0], DoublingTransformer)


def test_producer_group_construction() -> None:
    """
    Test the construction of a group.
    """

    producer_1 = NumberProducer(0, 4)
    producer_2 = NumberProducer(8, 12)
    number_producers = producer_1 & producer_2
    number_consumer = NumberConsumer()
    flow = number_producers >> DoublingTransformer() >> number_consumer

    expected_result = [[0, 0, 1, 2, 2, 4, 3, 6], [8, 16, 9, 18, 10, 20, 11, 22]]
    assert flow.run() == expected_result
    assert asyncio.run(flow.arun()) == expected_result

    assert flow.final_conduit is number_consumer


def test_transformer_group() -> None:
    flow = (
        NumberProducer(0, 3)
        >> (DoublingTransformer() & DoublingTransformer() >> DoublingTransformer())
        >> DoublingTransformer()
        >> NumberConsumer()
    )

    expected_result = [
        [0, 0, 0, 0, 1, 2, 2, 4, 2, 4, 4, 8],
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 2, 4, 2, 4, 4, 8, 2, 4, 4, 8, 4, 8, 8, 16],
    ]
    assert flow.run() == expected_result
    assert asyncio.run(flow.arun()) == expected_result


def test_chain_of_groups() -> None:
    flow = (
        (NumberProducer(0, 2) & NumberProducer(10, 11))
        >> (
            (
                (DoublingTransformer() & Passthrough())
                >> (IncrementingTransformer() & DoublingTransformer())
            )
            >> (IncrementingTransformer() & Passthrough())
        )
        >> NumberConsumer()
    )

    expected_result = [
        [2, 2, 3, 4],
        [12, 22],
        [1, 1, 2, 3],
        [11, 21],
        [1, 1, 1, 1, 2, 3, 3, 5],
        [11, 21, 21, 41],
        [0, 0, 0, 0, 1, 2, 2, 4],
        [10, 20, 20, 40],
        [2, 3],
        [12],
        [1, 2],
        [11],
        [1, 1, 2, 3],
        [11, 21],
        [0, 0, 1, 2],
        [10, 20],
    ]
    assert flow.run() == expected_result
    assert asyncio.run(flow.arun()) == expected_result


def test_expression() -> None:
    _DoublingTransformer = Id(DoublingTransformer)
    _NumberConsumer = Id(NumberConsumer)
    _NumberProducer = Id(NumberProducer)

    assert freeze(
        (
            (NumberProducer(0, 4) & NumberProducer(8, 12))
            >> (DoublingTransformer() & DoublingTransformer() >> DoublingTransformer())
            >> DoublingTransformer()
            >> NumberConsumer()
        ).to_expression()
    ) == freeze(
        (_NumberProducer(start=0, stop=4) & _NumberProducer(start=8, stop=12))
        >> (_DoublingTransformer() & _DoublingTransformer() >> _DoublingTransformer())
        >> _DoublingTransformer()
        >> _NumberConsumer()
    )


# use different flow variants to test the graph, using pytest arguments
@pytest.mark.parametrize(
    "transformer_concurrent",
    [(Passthrough() & DoublingTransformer()), (DoublingTransformer() & Passthrough())],
)
def test_graph(transformer_concurrent: Transformer[int, int]) -> None:
    _check_dot_notation(
        flow=(
            (NumberProducer(0, 4) & NumberProducer(8, 12))
            >> transformer_concurrent
            >> NumberConsumer()
        ),
        expected_nodes_and_edges="""\
    DoublingTransformer_# -> NumberConsumer_#
    DoublingTransformer_# [label=DoublingTransformer];
    NumberConsumer_# [label=NumberConsumer,style=rounded];
    NumberProducer_# -> DoublingTransformer_#
    NumberProducer_# -> DoublingTransformer_#
    NumberProducer_# -> NumberConsumer_#
    NumberProducer_# -> NumberConsumer_#
    NumberProducer_# \
[shape=record,label="NumberProducer|start=0\\lstop=4\\l",style=rounded];
    NumberProducer_# \
[shape=record,label="NumberProducer|start=8\\lstop=12\\l",style=rounded];""",
    )


def test_concurrent_graph() -> None:
    _check_dot_notation(
        flow=(NumberProducer(0, 4) & (NumberProducer(8, 12) >> DoublingTransformer())),
        expected_nodes_and_edges="""\
    DoublingTransformer_# -> _EndNode_#[style=dashed];
    DoublingTransformer_# [label=DoublingTransformer];
    NumberProducer_# -> DoublingTransformer_#
    NumberProducer_# -> _EndNode_#[style=dashed];
    NumberProducer_# \
[shape=record,label="NumberProducer|start=0\\lstop=4\\l",style=rounded];
    NumberProducer_# \
[shape=record,label="NumberProducer|start=8\\lstop=12\\l",style=rounded];
    _EndNode_# [label="||\\n",shape=doublecircle,style=dashed];""",
    )


def test_flow_drawers() -> None:
    from fluxus.viz import FlowDrawer

    flow = (
        (NumberProducer(0, 4) & NumberProducer(8, 12))
        >> (DoublingTransformer() & DoublingTransformer() >> DoublingTransformer())
        >> DoublingTransformer()
        >> NumberConsumer()
    )

    assert FlowDrawer.get_named_styles() == {"graph", "graph_dark", "text"}
    assert type(FlowDrawer.get_style("text")) is FlowTextStyle
    assert type(FlowDrawer.get_default_style()) is FlowGraphStyle
    io = StringIO()
    drawer = FlowDrawer(style=FlowTextStyle(out=io))
    drawer.draw(flow, title="Test")

    assert (
        io.getvalue()
        == """\
===================================== Test =====================================

(
    (NumberProducer(start=0, stop=4) & NumberProducer(start=8, stop=12))
    >> (DoublingTransformer() & DoublingTransformer() >> DoublingTransformer())
    >> DoublingTransformer()
    >> NumberConsumer()
)

"""
    )


HEADER_EXPECTED = [
    'digraph "Flow" {',
    "    rankdir=LR;",
    "    labeljust=l;",
    '    width="7.5";',
    "    node [shape=box,style=rounded];",
]


def _check_dot_notation(
    flow: Conduit[Any], expected_nodes_and_edges: str, **dot_params: Any
) -> None:
    # Replace _###... with _1234
    dot_repr = re.sub(
        r"_\d+", "_#", FlowGraph.from_conduit(flow).to_dot(**dot_params)
    ).split("\n")

    # Check the graph header
    assert dot_repr[: len(HEADER_EXPECTED)] == HEADER_EXPECTED

    # Check the graph footer
    assert dot_repr[-1] == "}"

    # Check the nodes and edges
    assert (
        "\n".join(sorted(dot_repr[len(HEADER_EXPECTED) : -1]))
        == expected_nodes_and_edges
    )


def test_dot_params() -> None:
    dot = re.sub(
        r"_\d+",
        "_#",
        FlowGraph.from_conduit(DoublingTransformer()).to_dot(
            width=5,
            font="serif",
            fontsize=12,
            fontcolor=RgbColor("red"),
            background=RgbColor("blue"),
            foreground=RgbColor("green"),
            fill=RgbColor("yellow"),
            stroke=RgbColor("purple"),
        ),
    )

    assert sorted(dot.split("\n")) == [
        "    DoublingTransformer_# -> _EndNode_#[style=dashed];",
        "    DoublingTransformer_# [label=DoublingTransformer];",
        "    _EndNode_# "
        '[label="||\\n",shape=doublecircle,style=dashed,fontcolor="#800080"];',
        "    _StartNode_# -> DoublingTransformer_#[style=dashed];",
        "    _StartNode_# "
        '[label=">\\n",shape=circle,style=dashed,fontcolor="#800080"];',
        '    bgcolor="#0000ff";',
        '    edge [color="#800080"];',
        "    labeljust=l;",
        "    node "
        '[shape=box,style="rounded,filled",color="#800080",fontcolor="#ff0000",'
        'fontsize=12,fillcolor="#ffff00",fontname=serif];',
        "    rankdir=LR;",
        "    width=5;",
        'digraph "Flow" {',
        "}",
    ]


def test_flow_error() -> None:
    with pytest.raises(
        TypeError,
        match=(
            r"^StringConsumer consumer is not compatible with NumberProducer source\. "
            r"Source product type <class 'int'> is not a subtype of consumer input "
            r"type <class 'str'>\.$"
        ),
    ):
        NumberProducer(0, 3) >> StringConsumer()  # type: ignore

    with pytest.raises(
        TypeError,
        match=(
            r"^unsupported operand type\(s\) for >>: 'StringConsumer' and "
            r"'StringConsumer'$"
        ),
    ):
        StringConsumer() >> StringConsumer()  # type: ignore

    with pytest.raises(
        TypeError,
        match=(
            r"^parallel\(\) can only be used with producer, transformer, or "
            r"passthrough steps, but got: StringConsumer\(\), StringConsumer\(\)$"
        ),
    ):
        parallel(StringConsumer(), StringConsumer())  # type: ignore


def test_passthrough() -> None:
    passthrough = Passthrough()

    assert freeze(
        (
            NumberProducer(0, 4)
            # Chain a producer group with a transformer group (which contains a
            # passthrough)
            >> (DoublingTransformer() & passthrough)
            >> DoublingTransformer()
            # Close with a consumer
            >> NumberConsumer()
        ).to_expression()
    ) == freeze(
        (
            NumberProducer(start=0, stop=4)
            >> (DoublingTransformer() & Passthrough())
            >> DoublingTransformer()
            >> NumberConsumer()
        ).to_expression()
    )

    with pytest.raises(
        TypeError,
        match=r"^unsupported operand type\(s\) for >>:",
    ):
        NumberProducer(0, 4) >> passthrough  # type: ignore[operator]

    with pytest.raises(
        TypeError,
        match=r"^unsupported operand type\(s\) for >>:",
    ):
        passthrough >> DoublingTransformer()  # type: ignore[operator]

    with pytest.raises(
        TypeError,
        match=r"^unsupported operand type\(s\) for >>:",
    ):
        DoublingTransformer() >> passthrough  # type: ignore[operator]

    with pytest.raises(
        TypeError,
        match=r"^unsupported operand type\(s\) for >>:",
    ):
        passthrough >> passthrough  # type: ignore[operator]

    assert not passthrough.is_chained
    with pytest.raises(
        NotImplementedError, match=r"^Final conduit is not defined for passthroughs$"
    ):
        _ = passthrough.final_conduit
    with pytest.raises(
        NotImplementedError, match=r"^Connections are not defined for passthroughs$"
    ):
        passthrough.get_connections(ingoing=[])


def test_flow_construction() -> None:
    flow = (
        NumberProducer(0, 3)
        >> parallel(
            Passthrough(),
            DoublingTransformer(),
            DoublingTransformer() >> DoublingTransformer(),
        )
        >> DoublingTransformer()
        >> NumberConsumer()
    )

    assert freeze(flow.to_expression()) == freeze(
        (
            NumberProducer(0, 3)
            >> (
                Passthrough()
                & DoublingTransformer()
                & DoublingTransformer() >> DoublingTransformer()
            )
            >> DoublingTransformer()
            >> NumberConsumer()
        ).to_expression()
    )


def test_large_flows() -> None:
    """
    Test a flow with 1000 parallel steps.
    """

    parallel_transformer = parallel(*[DoublingTransformer() for _ in range(1000)])

    assert parallel_transformer.n_concurrent_conduits == 1000

    parallel_producer = parallel(*[NumberProducer(0, i + 1) for i in range(1000)])

    assert parallel_producer.n_concurrent_conduits == 1000
