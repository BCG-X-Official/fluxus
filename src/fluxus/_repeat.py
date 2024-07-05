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
Implementation of ``Repeat``.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABCMeta, abstractmethod
from collections import deque
from collections.abc import AsyncIterator, Collection, Iterator
from typing import Any, Generic, Literal, TypeVar, cast, final

from pytools.api import appenddoc, inheritdoc
from pytools.expression import Expression

from ._passthrough import Passthrough
from .core import SerialConduit
from .core.control import FlowControl
from .core.transformer import BaseTransformer, ConcurrentTransformer, SerialTransformer

log = logging.getLogger(__name__)

__all__ = [
    "AsyncRepeat",
    "Repeat",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_TransformedProduct = TypeVar("T_TransformedProduct")
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)


#
# Constants
#

_END: Literal["END"] = "END"


#
# Classes
#


@inheritdoc(match="[see superclass]")
class _RepeatingTransformer(
    BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
    metaclass=ABCMeta,
):
    """
    A transformer that repeats the transformation of its source product if its repeat
    controller determines that it should be repeated.
    """

    #: The repeat controller
    repeat: Repeat[T_SourceProduct_arg, T_TransformedProduct_ret]

    def __init__(
        self,
        repeat: Repeat[T_SourceProduct_arg, T_TransformedProduct_ret],
    ) -> None:
        """
        :param repeat: the repeat controller
        """
        self.repeat = repeat

    @property
    @abstractmethod
    def transformer(
        self,
    ) -> BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]:
        """
        The transformer that is repeated.
        """

    def get_final_conduits(self) -> Iterator[SerialConduit[T_TransformedProduct_ret]]:
        """[see superclass]"""
        yield self.repeat

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[
        tuple[SerialConduit[Any], SerialConduit[Any]]
        | tuple[SerialConduit[Any], SerialConduit[Any], str],
    ]:
        """[see superclass]"""
        from .viz import FlowGraph

        yield from self.transformer.get_connections(ingoing=ingoing)

        # loop back
        for initial_conduit in {
            next(serial.chained_conduits)
            for serial in self.transformer.iter_concurrent_conduits()
        }:
            yield initial_conduit, self.repeat, FlowGraph.STYLE_BACK

        yield from self.repeat.get_connections(
            ingoing=list(self.transformer.get_final_conduits())
        )

    def to_expression(self, *, compact: bool = False) -> Expression:
        """[see superclass]"""
        return self.transformer.to_expression(
            compact=compact
        ) @ self.repeat.to_expression(compact=compact)


@inheritdoc(match="[see superclass]")
class _RepeatingSerialTransformer(
    _RepeatingTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
):
    """
    A serial transformer that repeats the transformation of its source product if its
    repeat controller determines that it should be repeated.
    """

    #: The transformer that is repeated
    _transformer: SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]

    @appenddoc(to=_RepeatingTransformer.__init__, prepend=True)
    def __init__(
        self,
        *,
        transformer: SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
        repeat: Repeat[T_SourceProduct_arg, T_TransformedProduct_ret],
    ) -> None:
        """
        :param transformer: the transformer that is repeated
        """
        super().__init__(repeat)
        self._transformer = transformer
        self.repeat = repeat

    @property
    def transformer(
        self,
    ) -> SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]:
        return self._transformer

    @property
    def is_chained(self) -> bool:
        """[see superclass]"""
        return self.transformer.is_chained

    def transform(
        self, source_product: T_SourceProduct_arg
    ) -> Iterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        # Use a deque to store the transformed products in first-in-first-out order
        # and initialize it with the transformed products of the source product
        products: deque[T_TransformedProduct_ret] = deque(
            self._transformer.transform(source_product)
        )
        # Repeat until all products have been processed
        while products:
            # Get the next transformed product
            product: T_TransformedProduct_ret = products.popleft()
            # Check whether the transformation should be repeated; if so, get the new
            # source product to use as the starting point for the next transformation
            new_source: T_SourceProduct_arg | None = self.repeat.test(product)
            if new_source is None:
                # If the repeat controller returns None, don't repeat the transformation
                # and yield the transformed product
                yield product
            else:
                # If the repeat controller returns a new source product, transform it
                # and add the new transformed products to the deque
                products.extend(self._transformer.transform(new_source))

    async def atransform(
        self, source_product: T_SourceProduct_arg
    ) -> AsyncIterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        # Use an asynchronous queue to line up the transformed products
        queue: asyncio.Queue[T_TransformedProduct_ret | Literal["END"]] = (
            asyncio.Queue()
        )
        repeat_tasks: set[asyncio.Task[None]] = set()
        # We count the number of pending repetitions to know how many _END signals we
        # need to receive before we can stop the loop.
        # We create the counter as a singleton list to allow it to be modified in the
        # inner function.
        pending_repetitions = [0]

        def _create_transform_task(product_: T_SourceProduct_arg) -> None:
            # Increase the count of pending repetitions
            pending_repetitions[0] += 1
            # Transform the product and put the transformed products in the queue
            repeat_task = asyncio.create_task(_process(product_))
            # Persist the task to prevent it from being garbage collected
            repeat_tasks.add(repeat_task)
            # Set a callback to delete the task when it is done
            repeat_task.add_done_callback(repeat_tasks.remove)

        async def _process(source_product_: T_SourceProduct_arg) -> None:
            # Get the transformed products of the source product
            products: AsyncIterator[T_TransformedProduct_ret] = (
                self._transformer.atransform(source_product_)
            )
            # Repeat until all products have been processed
            async for product_ in products:
                # Check whether the transformation should be repeated; if so, get the
                # new source product to use as the starting point for the next
                # transformation
                new_source: T_SourceProduct_arg | None = await self.repeat.atest(
                    product_
                )
                if new_source is None:
                    # If the repeat controller returns None, don't repeat the
                    # transformation and put the transformed product in the queue
                    await queue.put(product_)
                else:
                    # If the repeat controller returns a new source product, transform
                    # it and put the new transformed products in the queue
                    _create_transform_task(new_source)
            await queue.put(_END)

        _create_transform_task(source_product)

        # Consume the transformed products from the queue.
        # We stop when all pending repetitions have been processed, which is indicated
        # by receiving an _END signal for each pending repetition and the count of
        # pending repetitions reaching 0.
        while pending_repetitions[0] > 0:
            product = await queue.get()
            if product is _END:
                # If we receive an _END signal, decrease the count of pending
                # repetitions
                pending_repetitions[0] -= 1
            else:
                # If we receive a transformed product, yield it
                yield cast(T_TransformedProduct_ret, product)


@inheritdoc(match="[see superclass]")
class _RepeatingConcurrentTransformer(
    _RepeatingTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    ConcurrentTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
):
    """
    A concurrent transformer that repeats the transformation of its source product if
    its repeat controller determines that it should be repeated.
    """

    #: The transformer that is repeated
    _transformer: ConcurrentTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]

    @appenddoc(to=_RepeatingTransformer.__init__, prepend=True)
    def __init__(
        self,
        transformer: ConcurrentTransformer[
            T_SourceProduct_arg, T_TransformedProduct_ret
        ],
        repeat: Repeat[T_SourceProduct_arg, T_TransformedProduct_ret],
    ) -> None:
        """
        :param transformer: the transformer that is repeated
        """
        super().__init__(repeat)
        self._transformer = transformer

    @property
    def transformer(
        self,
    ) -> ConcurrentTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self._transformer

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return self.transformer.n_concurrent_conduits

    def iter_concurrent_conduits(
        self,
    ) -> Iterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""
        for conduit in self.transformer.iter_concurrent_conduits():
            if isinstance(conduit, Passthrough):
                yield conduit
            else:
                product_type = conduit.product_type
                yield _RepeatingSerialTransformer[
                    product_type, product_type  # type: ignore[valid-type]
                ](transformer=conduit, repeat=self.repeat)

    async def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""
        async for conduit in self.transformer.aiter_concurrent_conduits():
            if isinstance(conduit, Passthrough):
                yield conduit
            else:
                product_type = conduit.product_type
                yield _RepeatingSerialTransformer[
                    product_type, product_type  # type: ignore[valid-type]
                ](transformer=conduit, repeat=self.repeat)


class Repeat(
    FlowControl[T_SourceProduct_arg, T_TransformedProduct],
    Generic[T_SourceProduct_arg, T_TransformedProduct],
):
    """
    A flow control that determines whether a transformation should be repeated.
    """

    def __rmatmul__(
        self, other: BaseTransformer[T_SourceProduct_arg, T_TransformedProduct]
    ) -> _RepeatingTransformer[T_SourceProduct_arg, T_TransformedProduct]:
        if isinstance(other, SerialTransformer):
            product_type = other.product_type
            # noinspection PyTypeChecker
            return _RepeatingSerialTransformer[
                product_type, product_type  # type: ignore[valid-type]
            ](transformer=other, repeat=self)
        elif isinstance(other, ConcurrentTransformer):
            product_type = other.product_type
            # noinspection PyTypeChecker
            return _RepeatingConcurrentTransformer[
                product_type, product_type  # type: ignore[valid-type]
            ](transformer=other, repeat=self)
        else:
            return NotImplemented

    @abstractmethod
    def test(self, product: T_TransformedProduct) -> T_SourceProduct_arg | None:
        """
        Determine whether the transformation that produced the given product should be
        repeated.

        If the transformation should be repeated, return the new source product to use
        as the starting point for the next transformation. If the transformation should
        not be repeated, return ``None``.

        :param product: the transformed product
        :return: a source product to use as the starting point for the next
            transformation, or ``None`` if the transformation should not be repeated
        """

    async def atest(self, product: T_TransformedProduct) -> T_SourceProduct_arg | None:
        """
        Asynchronous version of :meth:`test`.

        :param product: the transformed product
        :return: a source product to use as the starting point for the next
            transformation, or ``None`` if the transformation should not be repeated
        """
        return self.test(product)


@inheritdoc(match="[see superclass]")
class AsyncRepeat(
    Repeat[T_SourceProduct_arg, T_TransformedProduct],
    Generic[T_SourceProduct_arg, T_TransformedProduct],
):
    """
    An asynchronous flow control that determines whether a transformation should be
    repeated.

    Synchronous use is supported but discouraged, as it creates a new event loop and
    blocks the current thread until the repeat condition has been evaluated. If
    synchronous use is required, the preferred method is to subclass :class:`Repeat` and
    override the :meth:`test` method.
    """

    def __rmatmul__(
        self, other: BaseTransformer[T_SourceProduct_arg, T_TransformedProduct]
    ) -> _RepeatingTransformer[T_SourceProduct_arg, T_TransformedProduct]:
        if isinstance(other, SerialTransformer):
            # noinspection PyTypeChecker
            return _RepeatingSerialTransformer(transformer=other, repeat=self)
        elif isinstance(other, ConcurrentTransformer):
            # noinspection PyTypeChecker
            return _RepeatingConcurrentTransformer(transformer=other, repeat=self)
        else:
            return NotImplemented

    @final
    def test(self, product: T_TransformedProduct) -> T_SourceProduct_arg | None:
        """[see superclass]"""
        return asyncio.run(self.atest(product))

    @abstractmethod
    async def atest(self, product: T_TransformedProduct) -> T_SourceProduct_arg | None:
        """
        Asynchronous version of :meth:`test`.

        :param product: the transformed product
        :return: a source product to use as the starting point for the next
            transformation, or ``None`` if the transformation should not be repeated
        """
