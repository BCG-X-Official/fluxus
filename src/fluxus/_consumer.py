"""
Implementation of conduit base classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterable, Iterable
from typing import Generic, TypeVar, final

from pytools.asyncio import arun, iter_sync_to_async

from .base import AtomicConduit, SerialProcessor

log = logging.getLogger(__name__)

__all__ = [
    "AsyncConsumer",
    "Consumer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_Output_ret = TypeVar("T_Output_ret", covariant=True)


#
# Classes
#


class Consumer(
    AtomicConduit[T_Output_ret],
    SerialProcessor[T_SourceProduct_arg, T_Output_ret],
    Generic[T_SourceProduct_arg, T_Output_ret],
    metaclass=ABCMeta,
):
    """
    Consumes products from a producer or group of producers, and returns a single
    object.
    """

    @final
    def process(self, input: Iterable[T_SourceProduct_arg]) -> T_Output_ret:
        """
        Consume the given products.

        :param input: the products to consume
        :return: the resulting object
        """
        from .simple import SimpleProducer

        return (
            SimpleProducer[self.input_type](input) >> self  # type: ignore[name-defined]
        ).run()

    @final
    async def aprocess(self, input: AsyncIterable[T_SourceProduct_arg]) -> T_Output_ret:
        """
        Consume the given products asynchronously.

        :param input: the products to consume
        :return: the resulting object
        """
        from .simple import SimpleAsyncProducer

        return await (
            SimpleAsyncProducer[self.input_type](input)  # type: ignore[name-defined]
            >> self
        ).arun()

    @abstractmethod
    def consume(
        self, products: Iterable[tuple[int, T_SourceProduct_arg]]
    ) -> T_Output_ret:
        """
        Consume products from a producer.

        :param products: an iterable of tuples (`i`, `product`) where `i` is the index
            of the concurrent producer, and `product` is a product from that producer
        :return: the resulting object
        """

    async def aconsume(
        self, products: AsyncIterable[tuple[int, T_SourceProduct_arg]]
    ) -> T_Output_ret:
        """
        Consume products from an asynchronous producer.

        By default, defers to the synchronous variant, :meth:`.consume`.

        :param products: an iterable of tuples (`i`, `product`) where `i` is the index
            of the concurrent producer, and `product` is a product from that producer
        :return: the resulting object
        """
        return self.consume([products async for products in products])


class AsyncConsumer(
    Consumer[T_SourceProduct_arg, T_Output_ret],
    Generic[T_SourceProduct_arg, T_Output_ret],
    metaclass=ABCMeta,
):
    """
    A consumer designed for asynchronous I/O.

    Synchronous iteration is supported but discouraged, as it creates a new event loop
    and blocks the current thread until the iteration is complete. It is preferable to
    use asynchronous iteration instead.
    """

    @final
    def consume(
        self, products: Iterable[tuple[int, T_SourceProduct_arg]]
    ) -> T_Output_ret:
        """
        Consume products from a producer.

        This method is implemented for compatibility with synchronous code, but
        preferably, :meth:`aconsume` should be used instead and called from within
        an event loop.

        When called from outside an event loop, this method will create an event loop
        using :meth:`arun`, collect the source products from :meth:`aconsume`
        and block the current thread until the iteration is complete. The resulting
        object will then be returned.

        :param products: an iterable of tuples (`i`, `product`) where `i` is the index
            of the concurrent producer, and `product` is a product from that producer
        :return: the resulting object
        """
        return arun(self.aconsume(iter_sync_to_async(products)))

    @abstractmethod
    async def aconsume(
        self,
        products: AsyncIterable[tuple[int, T_SourceProduct_arg]],
    ) -> T_Output_ret:
        """
        Consume products from an asynchronous producer.

        :param products: an iterable of tuples (`i`, `product`) where `i` is the index
            of the concurrent producer, and `product` is a product from that producer
        :return: the resulting object
        """
