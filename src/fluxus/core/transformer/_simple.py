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
Implementation of unions.
"""

from __future__ import annotations

import asyncio
import functools
import itertools
import logging
import operator
from abc import ABCMeta
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Collection, Iterator
from typing import Any, Generic, Literal, TypeVar, cast, final

from pytools.api import as_tuple, inheritdoc
from pytools.expression import Expression
from pytools.typing import get_common_generic_base, get_common_generic_subclass

from ... import Passthrough
from .. import AtomicConduit, SerialConduit
from ..producer import SerialProducer
from ._transformer_base import BaseTransformer, ConcurrentTransformer

log = logging.getLogger(__name__)

__all__ = [
    "SimpleConcurrentTransformer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T = TypeVar("T")
T_Output_ret = TypeVar("T_Output_ret", covariant=True)
T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)


#
# Classes
#


@final
@inheritdoc(match="[see superclass]")
class SimpleConcurrentTransformer(
    ConcurrentTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
):
    """
    A collection of one or more transformers, operating in parallel.
    """

    #: The transformers in this group.
    transformers: tuple[
        BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough,
        ...,
    ]

    def __init__(
        self,
        *transformers: (
            BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
        ),
    ) -> None:
        """
        :param transformers: the transformers in this group
        """
        self.transformers = transformers = as_tuple(
            itertools.chain(*map(_flatten_concurrent_transformers, transformers)),
            element_type=cast(
                tuple[
                    type[
                        BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]
                        | Passthrough
                    ],
                    ...,
                ],
                (BaseTransformer, Passthrough),
            ),
        )

        input_types = {
            transformer.input_type
            for transformer in transformers
            if not isinstance(transformer, Passthrough)
        }
        try:
            self._input_type = get_common_generic_subclass(input_types)
        except TypeError as e:
            raise TypeError(
                "Transformers have incompatible input types: "
                + ", ".join(sorted(input_type.__name__ for input_type in input_types))
            ) from e

        product_types = {
            transformer.product_type
            for transformer in transformers
            if not isinstance(transformer, Passthrough)
        }
        try:
            self._product_type = get_common_generic_base(product_types)
        except TypeError as e:
            raise TypeError(
                "Transformers have incompatible product types: "
                + ", ".join(
                    sorted(product_type.__name__ for product_type in product_types)
                )
            ) from e

    @property
    def input_type(self) -> type[T_SourceProduct_arg]:
        """[see superclass]"""
        return self._input_type

    @property
    def product_type(self) -> type[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self._product_type

    @property
    def is_chained(self) -> bool:
        """[see superclass]"""
        return any(transformer.is_chained for transformer in self.transformers)

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return sum(
            transformer.n_concurrent_conduits for transformer in self.transformers
        )

    def is_valid_source(self, source: SerialConduit[T_SourceProduct_arg]) -> bool:
        """[see superclass]"""
        return all(
            transformer.is_valid_source(source=source)
            for transformer in self.transformers
            if not isinstance(transformer, Passthrough)
        )

    def get_final_conduits(self) -> Iterator[SerialConduit[T_TransformedProduct_ret]]:
        """[see superclass]"""
        for transformer in self.transformers:
            yield from transformer.get_final_conduits()

    @property
    def _has_passthrough(self) -> bool:
        """[see superclass]"""
        return any(transformer._has_passthrough for transformer in self.transformers)

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """[see superclass]"""
        for transformer in self.transformers:
            if not isinstance(transformer, Passthrough):
                yield from transformer.get_connections(ingoing=ingoing)

    def get_isolated_conduits(
        self,
    ) -> Iterator[SerialConduit[T_TransformedProduct_ret]]:
        """[see superclass]"""
        for transformer in self.transformers:
            yield from transformer.get_isolated_conduits()

    def iter_concurrent_producers(
        self, *, source: SerialProducer[T_SourceProduct_arg]
    ) -> Iterator[SerialProducer[T_TransformedProduct_ret]]:
        """[see superclass]"""

        n_transformers = len(self.transformers)

        buffered_source = _BufferedSource(source=source, n=n_transformers)

        for k, transformer in enumerate(self.transformers):
            buffered_producer = _BufferedProducer(source=buffered_source, k=k)
            if isinstance(transformer, Passthrough):
                yield cast(SerialProducer[T_TransformedProduct_ret], buffered_producer)
            else:
                yield from transformer.iter_concurrent_producers(
                    source=buffered_producer
                )

    def to_expression(self, *, compact: bool = False) -> Expression:
        """[see superclass]"""
        return functools.reduce(
            operator.and_,
            (
                transformer.to_expression(compact=compact)
                for transformer in self.transformers
            ),
        )


#
# Auxiliary constants, functions and classes
#


def _flatten_concurrent_transformers(
    transformer: (
        BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    )
) -> Iterator[
    BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
]:
    """
    Iterate over the given transformer or its sub-transformers, if they are contained in
    a (possibly nested) simple concurrent transformer.

    :param transformer: the transformer to flatten
    :return: an iterator over the given transformer or its sub-transformers
    """
    if isinstance(transformer, SimpleConcurrentTransformer):
        for transformer in transformer.transformers:
            yield from _flatten_concurrent_transformers(transformer)
    else:
        yield transformer


@inheritdoc(match="[see superclass]")
class _BaseBufferedProducer(
    SerialProducer[T_Output_ret], Generic[T_Output_ret], metaclass=ABCMeta
):
    """
    A producer that materializes the products of another producer
    to allow multiple iterations over the same products.
    """

    source: SerialProducer[T_Output_ret]

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


class _BufferedSource(Generic[T_Output_ret]):
    """
    A source shared by multiple buffered producers.
    """

    source: SerialProducer[T_Output_ret]
    n: int

    _products: list[deque[T_Output_ret]] | None = None
    _products_async: list[AsyncIterator[T_Output_ret]] | None = None

    def __init__(self, source: SerialProducer[T_Output_ret], n: int) -> None:
        """
        :param source: the source producer
        :param n: the number of buffered producers
        """
        self.source = source
        self.n = n

    def get_products(self, k: int) -> Iterator[T_Output_ret]:
        """
        Get the products of the source producer.

        :return: the products
        """
        if self._products is None:
            products = list(self.source.produce())
            self._products = [deque(products) for _ in range(self.n)]

        my_deque = self._products[k]
        while my_deque:
            yield my_deque.popleft()

    def get_products_async(self, k: int) -> AsyncIterator[T_Output_ret]:
        """
        Get the products of the source producer asynchronously.

        :param k: the index of the buffered producer
        :return: the k-th async iterator over the products
        """

        if self._products_async is None:
            self._products_async = list(
                _async_iter_parallel(self.source.aproduce(), self.n)
            )
        return self._products_async[k]


@inheritdoc(match="[see superclass]")
class _BufferedProducer(
    AtomicConduit[T_Output_ret],
    SerialProducer[T_Output_ret],
    Generic[T_Output_ret],
):
    """
    A producer that creates multiple iterators over the same products, allowing multiple
    synchronous or asynchronous iterations, ensuring that the original producer is only
    iterated once.
    """

    source: _BufferedSource[T_Output_ret]
    k: int

    def __init__(self, source: _BufferedSource[T_Output_ret], k: int) -> None:
        """
        :param source: the source shared by multiple buffered producers
        :param k: the index of this buffered producer
        """
        self.source = source
        self.k = k

    @property
    def product_type(self) -> type[T_Output_ret]:
        """[see superclass]"""
        return self.source.source.product_type

    def produce(self) -> Iterator[T_Output_ret]:
        """[see superclass]"""
        return self.source.get_products(self.k)

    def aproduce(self) -> AsyncIterator[T_Output_ret]:
        """[see superclass]"""
        return self.source.get_products_async(self.k)

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """[see superclass]"""
        return self.source.source.get_connections(ingoing=ingoing)


#: Tasks for the producer that need to be awaited before the producer is garbage
#: collected
_producer_tasks: set[asyncio.Task[Any]] = set()

#: Sentinel to indicate the end of processing
_END: Literal["END"] = cast(Literal["END"], "END")


def _async_iter_parallel(
    iterable: AsyncIterable[T], n: int
) -> Iterator[AsyncIterator[T]]:
    # Create a given number of asynchronous iterators that share the same items
    # from the given source iterable.

    async def _shared_iterator(
        queue: asyncio.Queue[T | Literal["END"]],
    ) -> AsyncIterator[T]:
        while True:
            # Wait for the item to be available for this iterator
            item = await queue.get()
            if item is _END:
                # The producer has finished
                break
            yield cast(T, item)

    async def _producer() -> None:
        # Iterate over the items in the source iterable
        async for item in iterable:
            # Add the item to all queues
            for queue in queues:
                await queue.put(item)
        # Notify all consumers that the producer has finished
        for queue in queues:
            await queue.put(_END)

    # Create a queue for each consumer
    queues: list[asyncio.Queue[T | Literal["END"]]] = [
        asyncio.Queue() for _ in range(n)
    ]

    # Start the producer task, and store a reference to it to prevent it from being
    # garbage collected before it finishes
    task = asyncio.create_task(_producer())
    _producer_tasks.add(task)
    task.add_done_callback(_producer_tasks.remove)

    return (_shared_iterator(queue) for queue in queues)
