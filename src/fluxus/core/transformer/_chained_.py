"""
Implementation of composition classes.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABCMeta
from collections.abc import AsyncIterable, AsyncIterator, Collection, Iterator
from typing import Any, Generic, Literal, TypeVar, cast

from pytools.api import inheritdoc
from pytools.asyncio import async_flatten

from ..._passthrough import Passthrough
from .._base import Processor, Source
from .._chained_base_ import _ChainedConduit, _SerialChainedConduit
from .._conduit import AtomicConduit, SerialConduit
from ..producer import BaseProducer, ConcurrentProducer, SerialProducer
from ._transformer_base import BaseTransformer, ConcurrentTransformer, SerialTransformer

log = logging.getLogger(__name__)


#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_Output_ret = TypeVar("T_Output_ret", covariant=True)
T_Product_ret = TypeVar("T_Product_ret", covariant=True)
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)
T_SourceProduct_ret = TypeVar("T_SourceProduct_ret", covariant=True)
T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)


#
# Classes
#


@inheritdoc(match="[see superclass]")
class _ChainedProducer(
    _SerialChainedConduit[T_SourceProduct_ret, T_TransformedProduct_ret],
    SerialProducer[T_TransformedProduct_ret],
    Generic[
        # the product type of the producer (`source`) in this chain
        T_SourceProduct_ret,
        # the product type of the transformer in this chain
        T_TransformedProduct_ret,
    ],
):
    """
    A sequential composition of a producer and a transformer, with the output of the
    producer serving as input to the transformer.
    """

    #: The source producer that this transformer is bound to.
    _producer: SerialProducer[T_SourceProduct_ret]

    #: The underlying unbound transformer.
    transformer: SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]

    def __init__(
        self,
        *,
        producer: SerialProducer[T_SourceProduct_ret],
        transformer: SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret],
    ) -> None:
        """
        :param producer: the source to use as input
        :param transformer: the transformer to apply to the source
        :raises TypeError: if the source is not compatible with the transformer
        """
        super().__init__()
        if not transformer.is_valid_source(producer.final_conduit):
            raise TypeError(
                f"SerialTransformer {type(transformer).__name__} is not compatible "
                f"with source {type(producer.final_conduit).__name__}: "
                f"Source product type {producer.product_type} is not a subtype of "
                f"transformer input type {transformer.input_type}."
            )
        self._producer = producer
        self.transformer = transformer

    @property
    def product_type(self) -> type[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer.product_type

    @property
    def _source(self) -> SerialProducer[T_SourceProduct_ret]:
        """[see superclass]"""
        return self._producer

    @property
    def _processor(
        self,
    ) -> SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer

    def produce(self) -> Iterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer.iter(self._producer)

    def aproduce(self) -> AsyncIterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer.aiter(self._producer)


@inheritdoc(match="[see superclass]")
class _ChainedTransformer(
    _SerialChainedConduit[T_SourceProduct_ret, T_TransformedProduct_ret],
    SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_SourceProduct_ret, T_TransformedProduct_ret],
):
    """
    A sequential composition of two transformers, with the output of the first serving
    as input to the second.
    """

    #: The first transformer in the chain.
    first: SerialTransformer[T_SourceProduct_arg, T_SourceProduct_ret]

    #: The second transformer in the chain.
    second: SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]

    def __init__(
        self,
        first: SerialTransformer[T_SourceProduct_arg, T_SourceProduct_ret],
        second: SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret],
    ) -> None:
        """
        :param first: the first transformer in the chain
        :param second: the second transformer in the chain
        """
        super().__init__()
        self.first = first
        self.second = second

    @property
    def input_type(self) -> type[T_SourceProduct_arg]:
        """[see superclass]"""
        return self.first.input_type

    @property
    def product_type(self) -> type[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.second.product_type

    @property
    def _source(self) -> SerialTransformer[T_SourceProduct_arg, T_SourceProduct_ret]:
        """[see superclass]"""
        return self.first

    @property
    def _processor(
        self,
    ) -> SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.second

    def transform(
        self, source_product: T_SourceProduct_arg
    ) -> Iterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        for tx_first in self.first.transform(source_product):
            yield from self.second.transform(tx_first)

    def atransform(
        self, source_product: T_SourceProduct_arg
    ) -> AsyncIterator[T_TransformedProduct_ret]:
        """[see superclass]"""

        # noinspection PyTypeChecker
        return async_flatten(
            self.second.atransform(tx_first)
            async for tx_first in self.first.atransform(source_product)
        )


@inheritdoc(match="[see superclass]")
class _ChainedConcurrentProducer(
    _ChainedConduit[T_SourceProduct_ret, T_TransformedProduct_ret],
    ConcurrentProducer[T_TransformedProduct_ret],
    Generic[T_SourceProduct_ret, T_TransformedProduct_ret],
):
    """
    A sequential composition of a group and a transformer, with the output of
    each source serving as input to the transformer.
    """

    #: The group that this transformer is bound to.
    _producer: BaseProducer[T_SourceProduct_ret]

    #: The underlying unbound transformer.
    transformer: BaseTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]

    def __init__(
        self,
        *,
        source: BaseProducer[T_SourceProduct_ret],
        transformer: BaseTransformer[T_SourceProduct_ret, T_TransformedProduct_ret],
    ) -> None:
        """
        :param source: the group to use as input
        :param transformer: the transformer to apply to the sources
        """
        super().__init__()
        self._producer = source
        self.transformer = transformer

    @property
    def _source(self) -> BaseProducer[T_SourceProduct_ret]:
        """[see superclass]"""
        return self._producer

    @property
    def _processor(self) -> Processor[T_SourceProduct_ret, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return (
            self._producer.n_concurrent_conduits
            * self.transformer.n_concurrent_conduits
        )

    def iter_concurrent_conduits(
        self,
    ) -> Iterator[SerialProducer[T_TransformedProduct_ret]]:
        """[see superclass]"""

        def _iter_chained_producers(
            tx: (
                SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]
                | Passthrough
            ),
        ) -> Iterator[SerialProducer[T_TransformedProduct_ret]]:
            if isinstance(tx, Passthrough):
                # Passthrough does not change the type, so we can cast the input type
                # to the type of the transformed product
                yield from cast(
                    Iterator[SerialProducer[T_TransformedProduct_ret]],
                    self._source.iter_concurrent_conduits(),
                )
            else:
                for source in self._producer.iter_concurrent_conduits():
                    yield _ChainedProducer(producer=source, transformer=tx)

        for transformer in self.transformer.iter_concurrent_conduits():
            yield from _iter_chained_producers(transformer)

    def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[SerialProducer[T_TransformedProduct_ret]]:
        """[see superclass]"""

        async def _aiter_chained_producers(
            tx: (
                SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]
                | Passthrough
            ),
        ) -> AsyncIterator[SerialProducer[T_TransformedProduct_ret]]:
            if isinstance(tx, Passthrough):
                async for source in self._producer.aiter_concurrent_conduits():
                    # Passthrough does not change the type, so we can cast the
                    # input type to the type of the transformed product
                    yield cast(SerialProducer[T_TransformedProduct_ret], source)
            else:
                async for source in self._producer.aiter_concurrent_conduits():
                    yield _ChainedProducer(producer=source, transformer=tx)

        # noinspection PyTypeChecker
        return async_flatten(
            _aiter_chained_producers(transformer)
            async for transformer in self.transformer.aiter_concurrent_conduits()
        )


@inheritdoc(match="[see superclass]")
class _ChainedConcurrentTransformedProducer(
    _ChainedConduit[T_SourceProduct_ret, T_Product_ret],
    ConcurrentProducer[T_Product_ret],
    Generic[T_SourceProduct_ret, T_Product_ret],
):
    """
    A sequential composition of a producer and a transformer group, with the output of
    the producer serving as input to the transformer group.

    The result is a group of producers, each of which is the result of applying one of
    the transformers in the group to the source producer.

    This class is useful for applying a sequence of transformers to a single source
    producer.

    Care is taken to ensure that the source producer is only iterated once.
    For synchronous iteration, the source products are materialized and then passed to
    each transformer in the group.
    For asynchronous iteration, concurrent synchronized iterators are created for the
    source products that block upon each iterated item until all transformers in the
    group have processed it.
    """

    #: The source producer
    _producer: SerialProducer[T_SourceProduct_ret]

    #: The transformer group to apply to the producer
    transformer_group: BaseTransformer[T_SourceProduct_ret, T_Product_ret]

    def __init__(
        self,
        *,
        source: SerialProducer[T_SourceProduct_ret],
        transformer_group: BaseTransformer[T_SourceProduct_ret, T_Product_ret],
    ) -> None:
        """
        :param source: the producer to use as input
        :param transformer_group: the transformer group to apply to the producer
        """
        super().__init__()
        self._producer = source
        self.transformer_group = transformer_group

    @property
    def _source(self) -> SerialProducer[T_SourceProduct_ret]:
        """[see superclass]"""
        return self._producer

    @property
    def _processor(self) -> BaseTransformer[T_SourceProduct_ret, T_Product_ret]:
        """[see superclass]"""
        return self.transformer_group

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return self.transformer_group.n_concurrent_conduits

    def iter_concurrent_conduits(self) -> Iterator[SerialProducer[T_Product_ret]]:
        """[see superclass]"""

        # create one shared buffered producer for synchronous iteration
        producer = _BufferedProducer(self._producer)

        # for synchronous iteration, we need to materialize the source products
        for transformer in self.transformer_group.iter_concurrent_conduits():
            if isinstance(transformer, Passthrough):
                # We cast to T_Product_ret, since the Passthrough does not change the
                # type of the source
                yield cast(SerialProducer[T_Product_ret], producer)
            else:
                yield producer >> transformer

    async def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[SerialProducer[T_Product_ret]]:
        """[see superclass]"""

        # Create parallel synchronized iterators for the source products
        concurrent_producers = _AsyncBufferedProducer.create(
            source=self._producer, n=self.transformer_group.n_concurrent_conduits
        )

        async for transformer in self.transformer_group.aiter_concurrent_conduits():
            producer = next(concurrent_producers)
            if isinstance(transformer, Passthrough):
                # We cast to T_Product_ret, since the Passthrough does not change the
                # type of the source
                yield cast(SerialProducer[T_Product_ret], producer)
            else:
                yield producer >> transformer


@inheritdoc(match="[see superclass]")
class _ChainedConcurrentTransformer(
    _ChainedConduit[T_SourceProduct_ret, T_TransformedProduct_ret],
    ConcurrentTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_SourceProduct_ret, T_TransformedProduct_ret],
):
    """
    A sequential composition of transformers including a transformer group, with the
    output of the source serving as input to the transformer group.
    """

    #: The first transformer or transformer group in the chain.
    first: BaseTransformer[T_SourceProduct_arg, T_SourceProduct_ret]

    #: The second transformer or transformer group in the chain.
    second: BaseTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]

    def __init__(
        self,
        first: BaseTransformer[T_SourceProduct_arg, T_SourceProduct_ret],
        second: BaseTransformer[T_SourceProduct_ret, T_TransformedProduct_ret],
    ) -> None:
        """
        :param first: the first transformer or transformer group in the chain
        :param second: the second transformer or transformer group in the chain
        """
        super().__init__()
        self.first = first
        self.second = second

    @property
    def _source(self) -> Source[T_SourceProduct_ret]:
        """[see superclass]"""
        return self.first

    @property
    def _processor(self) -> Processor[T_SourceProduct_ret, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.second

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return self.first.n_concurrent_conduits * self.second.n_concurrent_conduits

    def iter_concurrent_conduits(
        self,
    ) -> Iterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""
        for first in self.first.iter_concurrent_conduits():
            if isinstance(first, Passthrough):
                yield from cast(
                    Iterator[
                        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]
                    ],
                    self.second.iter_concurrent_conduits(),
                )
            else:
                for second in self.second.iter_concurrent_conduits():
                    if isinstance(second, Passthrough):
                        yield cast(
                            SerialTransformer[
                                T_SourceProduct_arg, T_TransformedProduct_ret
                            ],
                            first,
                        )
                    else:
                        yield first >> second

    def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""

        async def _aiter(
            first: (
                SerialTransformer[T_SourceProduct_arg, T_SourceProduct_ret]
                | Passthrough
            )
        ) -> AsyncIterator[
            SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]
            | Passthrough
        ]:
            if isinstance(first, Passthrough):
                async for second in self.second.aiter_concurrent_conduits():
                    yield cast(
                        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]
                        | Passthrough,
                        second,
                    )
            else:
                async for second in self.second.aiter_concurrent_conduits():
                    if isinstance(second, Passthrough):
                        yield cast(
                            SerialTransformer[
                                T_SourceProduct_arg, T_TransformedProduct_ret
                            ],
                            first,
                        )
                    else:
                        yield first >> second

        # noinspection PyTypeChecker
        return async_flatten(
            _aiter(first) async for first in self.first.aiter_concurrent_conduits()
        )


@inheritdoc(match="[see superclass]")
class _BaseBufferedProducer(
    SerialProducer[T_Output_ret], Generic[T_Output_ret], metaclass=ABCMeta
):
    """
    A producer that materializes the products of another producer
    to allow multiple iterations over the same products.
    """

    source: SerialProducer[T_Output_ret]
    _products: list[T_Output_ret] | None

    def __init__(self, source: SerialProducer[T_Output_ret]) -> None:
        """
        :param source: the producer from which to buffer the products
        """
        self.source = source

    @property
    def product_type(self) -> type[T_Output_ret]:
        """[see superclass]"""
        return self.source.product_type

    def get_final_conduits(self) -> Iterator[SerialConduit[T_Output_ret]]:
        """[see superclass]"""
        return self.source.get_final_conduits()

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """[see superclass]"""
        return self.source.get_connections(ingoing=ingoing)


@inheritdoc(match="[see superclass]")
class _BufferedProducer(
    AtomicConduit[T_Output_ret],
    _BaseBufferedProducer[T_Output_ret],
    Generic[T_Output_ret],
):
    """
    A producer that materializes the products of another producer
    to allow multiple iterations over the same products.
    """

    source: SerialProducer[T_Output_ret]
    _products: list[T_Output_ret] | None = None

    def produce(self) -> Iterator[T_Output_ret]:
        """[see superclass]"""
        if self._products is None:
            self._products = list(self.source.produce())
        return iter(self._products)


class _AsyncBufferedProducer(
    AtomicConduit[T_Output_ret],
    _BaseBufferedProducer[T_Output_ret],
    Generic[T_Output_ret],
):
    """
    A producer that creates multiple synchronized iterators over the same products,
    allowing multiple asynchronous iterations over the same products where each
    iteration blocks until all iterators have processed the current item, ensuring
    that the original producer is only iterated once.
    """

    products: AsyncIterator[T_Output_ret]
    k: int

    def __init__(
        self,
        *,
        source: SerialProducer[T_Output_ret],
        products: AsyncIterator[T_Output_ret],
        k: int,
    ) -> None:
        super().__init__(source=source)
        self.products = products
        self.k = k

    @classmethod
    def create(
        cls, source: SerialProducer[T_Output_ret], *, n: int
    ) -> Iterator[_AsyncBufferedProducer[T_Output_ret]]:
        """
        Create multiple synchronized asynchronous producers over the products of the
        given source producer.

        :param source: the source producer
        :param n: the number of synchronized producers to create
        :return: the synchronized producers
        """
        return (
            _AsyncBufferedProducer(source=source, products=products, k=k)
            for k, products in enumerate(_async_iter_parallel(source.aproduce(), n))
        )

    def produce(self) -> Iterator[T_Output_ret]:
        raise NotImplementedError(
            "Not implemented; use `aiter` to iterate asynchronously."
        )

    def aproduce(self) -> AsyncIterator[T_Output_ret]:
        return self.products


#
# Auxiliary constants, functions and classes
#

T = TypeVar("T")

_producer_tasks: set[asyncio.Task[Any]] = set()


def _async_iter_parallel(
    iterable: AsyncIterable[T], n: int
) -> Iterator[AsyncIterator[T]]:

    _END = "END"
    barrier: asyncio.Barrier = asyncio.Barrier(n + 1)
    queue: asyncio.Queue[T | Literal["END"]] = asyncio.Queue()

    async def _shared_iterator() -> AsyncIterator[T]:
        while True:
            # Wait for the item to be available for this iterator
            item = await queue.get()
            if item is _END:
                # The producer has finished
                break
            yield cast(T, item)
            # Wait for all iterators and the producer to reach this point
            try:
                await barrier.wait()
            except asyncio.BrokenBarrierError:  # pragma: no covers
                # The barrier may be broken if the producer has finished
                # and there are no more items to process.
                break

    async def _producer() -> None:
        async for item in iterable:
            for _ in range(n):
                await queue.put(item)
            # Wait for all consumers to process the item
            await barrier.wait()
        # Notify all consumers that the producer has finished
        for _ in range(n):
            await queue.put(cast(Literal["END"], _END))

    # Start the producer task, and store a reference to it to prevent it from being
    # garbage collected before it finishes
    task = asyncio.create_task(_producer())
    _producer_tasks.add(task)
    task.add_done_callback(_producer_tasks.remove)

    return (_shared_iterator() for _ in range(n))
