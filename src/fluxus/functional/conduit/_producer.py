"""
Implementation of ``DictProducer``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
)
from typing import Any

from pytools.api import inheritdoc
from pytools.asyncio import iter_sync_to_async

from ... import AsyncProducer
from ..product import DictProduct
from ._conduit import DictConduit

log = logging.getLogger(__name__)

__all__ = [
    "DictProducer",
]


#
# Classes
#


@inheritdoc(match="[see superclass]")
class DictProducer(DictConduit, AsyncProducer[DictProduct]):
    """
    A producer of dictionary products.
    """

    #: The dictionaries produced by this producer.
    producer: Callable[
        [],
        AsyncIterable[Mapping[str, Any]]
        | Iterable[Mapping[str, Any]]
        | Mapping[str, Any]
        | Awaitable[Mapping[str, Any]],
    ]

    def __init__(
        self,
        producer: Callable[
            [],
            AsyncIterable[Mapping[str, Any]]
            | Iterable[Mapping[str, Any]]
            | Mapping[str, Any]
            | Awaitable[Mapping[str, Any]],
        ],
        *,
        name: str,
    ) -> None:
        """
        :param producer: a function that produces dictionaries
        :param name: the name of this producer
        """
        super().__init__(name=name)
        self.producer = producer

    async def aproduce(self) -> AsyncIterator[DictProduct]:
        """[see superclass]"""
        products = self.producer()

        # Measure the start time of the step. We are interested in CPU time, not wall
        # time, so we use time.perf_counter() instead of time.time().
        start_time = time.perf_counter()

        if isinstance(products, Mapping):
            yield DictProduct(
                name=self.name,
                product_attributes=products,
                start_time=start_time,
                end_time=start_time,
            )

        elif isinstance(products, Awaitable):
            attributes = await products
            yield DictProduct(
                name=self.name,
                product_attributes=attributes,
                start_time=start_time,
                end_time=time.perf_counter(),
            )

        else:
            if isinstance(products, Iterable):
                products = iter_sync_to_async(products)  # type: ignore[arg-type]
            elif not isinstance(products, AsyncIterable):
                raise TypeError(
                    f"Expected producer function to return a Mapping, an Iterable, or "
                    f"an AsyncIterable, but got: {products!r}"
                )
            async for product in products:
                # Measure the end time of the step.
                end_time = time.perf_counter()

                yield DictProduct(
                    name=self.name,
                    product_attributes=product,
                    start_time=start_time,
                    end_time=end_time,
                )

                # Set the start time of the next iteration to the end time of this
                # iteration.
                start_time = end_time
