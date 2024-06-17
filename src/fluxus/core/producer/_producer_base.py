"""
Implementation of conduit base classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Generic, TypeVar, cast, final

from pytools.api import inheritdoc
from pytools.asyncio import async_flatten
from pytools.typing import get_common_generic_base

from ..._consumer import Consumer
from ..._flow import Flow
from .. import ConcurrentConduit, SerialSource, Source

log = logging.getLogger(__name__)

__all__ = [
    "BaseProducer",
    "ConcurrentProducer",
    "SerialProducer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_Product_ret = TypeVar("T_Product_ret", covariant=True)
T_Output_ret = TypeVar("T_Output_ret", covariant=True)

#
# Classes
#


@inheritdoc(match="[see superclass]")
class BaseProducer(Source[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta):
    """
    A source that generates products from scratch – this is either a
    :class:`.Producer` or a :class:`.ConcurrentProducer`.
    """

    @abstractmethod
    def produce(self) -> Iterator[T_Product_ret]:
        """
        Generate new products.

        :return: the new products
        """

    @abstractmethod
    def aproduce(self) -> AsyncIterator[T_Product_ret]:
        """
        Generate new products asynchronously.

        :return: the new products
        """

    @abstractmethod
    def iter_concurrent_conduits(self) -> Iterator[SerialProducer[T_Product_ret]]:
        """[see superclass]"""

    @abstractmethod
    def aiter_concurrent_conduits(self) -> AsyncIterator[SerialProducer[T_Product_ret]]:
        """[see superclass]"""

    @final
    def __iter__(self) -> Iterator[T_Product_ret]:
        return self.produce()

    @final
    def __aiter__(self) -> AsyncIterator[T_Product_ret]:
        return self.aproduce()

    def __and__(
        self, other: BaseProducer[T_Product_ret]
    ) -> ConcurrentProducer[T_Product_ret]:

        if isinstance(other, BaseProducer):
            from . import SimpleConcurrentProducer

            # We determine the type hint at runtime, and use a type cast to
            # indicate the type for static type checks
            return cast(
                ConcurrentProducer[T_Product_ret],
                SimpleConcurrentProducer[  # type: ignore[misc]
                    get_common_generic_base((self.product_type, other.product_type))
                ](self, other),
            )
        else:
            return NotImplemented

    def __rshift__(
        self,
        other: Consumer[T_Product_ret, T_Output_ret],
    ) -> Flow[T_Output_ret]:
        if isinstance(other, Consumer):
            # We import locally to avoid circular imports
            from ._chained_ import _ProducerGroupFlow

            return _ProducerGroupFlow(producer=self, consumer=other)
        else:
            return NotImplemented


@inheritdoc(match="[see superclass]")
class SerialProducer(
    BaseProducer[T_Product_ret],
    SerialSource[T_Product_ret],
    Generic[T_Product_ret],
    metaclass=ABCMeta,
):
    """
    Generates objects of a specific type that may be retrieved locally or remotely, or
    created dynamically.

    It can run synchronously or asynchronously.
    """

    def iter_concurrent_conduits(self) -> Iterator[SerialProducer[T_Product_ret]]:
        """[see superclass]"""
        yield self

    async def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[SerialProducer[T_Product_ret]]:
        """[see superclass]"""
        yield self

    async def aproduce(self) -> AsyncIterator[T_Product_ret]:
        """
        Generate new products asynchronously.

        By default, defers to the synchronous variant, :meth:`.iter`.

        :return: the new products
        """
        for product in self.produce():
            yield product

    def __rshift__(
        self,
        other: Consumer[T_Product_ret, T_Output_ret],
    ) -> Flow[T_Output_ret]:
        if isinstance(other, Consumer):
            # We import locally to avoid circular imports
            from ._chained_ import _ProducerFlow

            return _ProducerFlow(producer=self, consumer=other)
        else:
            return NotImplemented


class ConcurrentProducer(
    ConcurrentConduit[T_Product_ret],
    BaseProducer[T_Product_ret],
    Generic[T_Product_ret],
    metaclass=ABCMeta,
):
    """
    A collection of one or more producers.
    """

    def produce(self) -> Iterator[T_Product_ret]:
        """
        Generate new products from all producers in this group.

        :return: an iterator of the new products
        """
        for producer in self.iter_concurrent_conduits():
            yield from producer

    def aproduce(self) -> AsyncIterator[T_Product_ret]:
        """
        Generate new products from all producers in this group asynchronously.

        :return: an async iterator of the new products
        """
        # create tasks for each producer - these need to be coroutines that materialize
        # the producers

        # noinspection PyTypeChecker
        return async_flatten(
            producer.aproduce() async for producer in self.aiter_concurrent_conduits()
        )
