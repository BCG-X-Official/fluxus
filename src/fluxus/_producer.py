"""
Implementation of producers.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Collection, Iterator
from typing import Any, Generic, TypeVar, final

from pytools.api import inheritdoc
from pytools.asyncio import arun, iter_async_to_sync

from .core import AtomicConduit, SerialConduit
from .core.producer import SerialProducer

log = logging.getLogger(__name__)

__all__ = [
    "AsyncProducer",
    "Producer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_Product_ret = TypeVar("T_Product_ret", covariant=True)


#
# Classes
#


@inheritdoc(match="[see superclass]")
class Producer(
    AtomicConduit[T_Product_ret],
    SerialProducer[T_Product_ret],
    Generic[T_Product_ret],
    metaclass=ABCMeta,
):
    """
    Generates objects of a specific type that may be retrieved locally or remotely, or
    created dynamically.

    It can run synchronously or asynchronously.
    """

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """[see superclass]"""
        if ingoing:
            raise ValueError(
                f"Producers cannot have ingoing connections, but got: {ingoing}"
            )
        yield from ()


@inheritdoc(match="[see superclass]")
class AsyncProducer(Producer[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta):
    """
    A producer designed for asynchronous I/O.

    Synchronous iteration is supported but discouraged, as it creates a new event loop
    and blocks the current thread until the iteration is complete. It is preferable to
    use asynchronous iteration instead.
    """

    @final
    def produce(self) -> Iterator[T_Product_ret]:
        """
        Generate new products, optionally using an existing producer as input.

        This method is implemented for compatibility with synchronous code, but
        preferably, :meth:`.aproduce` should be used instead and called from within an
        event loop.

        When called from outside an event loop, this method will create an event loop
        using :meth:`arun`, collect the products from :meth:`aproduce` and block the
        current thread until the iteration is complete. The products will then be
        returned as a list.

        :return: the new products
        :raises RuntimeError: if called from within an event loop
        """

        return arun(iter_async_to_sync(self.aproduce()))

    @abstractmethod
    def aproduce(self) -> AsyncIterator[T_Product_ret]:
        """[see superclass]"""
