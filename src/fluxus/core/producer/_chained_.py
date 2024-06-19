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
Implementation of composition classes.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from typing import Generic, TypeVar, final

from pytools.api import inheritdoc
from pytools.asyncio import aenumerate, async_flatten

from ..._consumer import Consumer
from ..._flow import Flow
from .._chained_base_ import _ChainedConduit, _SerialChainedConduit
from ._producer_base import BaseProducer, SerialProducer

log = logging.getLogger(__name__)


#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_Output_ret = TypeVar("T_Output_ret", covariant=True)
T_SourceProduct_ret = TypeVar("T_SourceProduct_ret", covariant=True)
T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)


#
# Classes
#


@final
@inheritdoc(match="[see superclass]")
class _ProducerFlow(
    Flow[T_Output_ret],
    _SerialChainedConduit[T_SourceProduct_ret, T_Output_ret],
    Generic[T_SourceProduct_ret, T_Output_ret],
):
    """
    A flow, composed of a producer and a consumer, with the output of the producer
    serving as input to the consumer.
    """

    #: The producer that this consumer is bound to.
    _producer: SerialProducer[T_SourceProduct_ret]

    #: The consumer that terminates the flow.
    _consumer: Consumer[T_SourceProduct_ret, T_Output_ret]

    def __init__(
        self,
        *,
        producer: SerialProducer[T_SourceProduct_ret],
        consumer: Consumer[T_SourceProduct_ret, T_Output_ret],
    ) -> None:
        """
        :param producer: the source to use as input
        :param consumer: the consumer to apply to the source
        """
        super().__init__()
        if not consumer.is_valid_source(producer.final_conduit):
            raise TypeError(
                f"{type(consumer).__name__} consumer is not compatible with "
                f"{type(producer.final_conduit).__name__} source. "
                f"Source product type {producer.product_type!r} is not a "
                f"subtype of consumer input type {consumer.input_type!r}."
            )
        self._producer = producer
        self._consumer = consumer

    @property
    def final_conduit(self) -> Consumer[T_SourceProduct_ret, T_Output_ret]:
        """
        The consumer that terminates the flow.
        """
        return self._consumer

    @property
    def _source(self) -> SerialProducer[T_SourceProduct_ret]:
        """
        The source producer.
        """
        return self._producer

    @property
    def _processor(self) -> Consumer[T_SourceProduct_ret, T_Output_ret]:
        """
        The final processor of this flow.
        """
        return self._consumer

    @final
    def run(self) -> T_Output_ret:
        """[see superclass]"""
        # noinspection PyProtectedMember
        return _consume(producer=self._producer, consumer=self._consumer)

    @final
    async def arun(self) -> T_Output_ret:
        """[see superclass]"""

        # noinspection PyProtectedMember
        return await _aconsume(producer=self._producer, consumer=self._consumer)


@final
@inheritdoc(match="[see superclass]")
class _ProducerGroupFlow(
    Flow[T_Output_ret],
    _ChainedConduit[T_SourceProduct_ret, T_Output_ret],
    Generic[T_SourceProduct_ret, T_Output_ret],
):
    """
    A flow, composed of a group of producers and a consumer, with the output of the
    producers serving as input to the consumer.
    """

    #: The producer that this consumer is bound to.
    _producer: BaseProducer[T_SourceProduct_ret]

    #: The consumer that terminates the flow.
    _consumer: Consumer[T_SourceProduct_ret, T_Output_ret]

    def __init__(
        self,
        *,
        producer: BaseProducer[T_SourceProduct_ret],
        consumer: Consumer[T_SourceProduct_ret, T_Output_ret],
    ) -> None:
        """
        :param producer: the source to use as input
        :param consumer: the consumer to apply to the source
        """
        super().__init__()
        invalid_producers = {
            type(producer.final_conduit).__name__
            for producer in producer.iter_concurrent_conduits()
            if not consumer.is_valid_source(producer.final_conduit)
        }
        if invalid_producers:
            raise TypeError(
                "Producers are not compatible with consumer "
                f"{type(consumer).__name__}: " + ", ".join(invalid_producers)
            )
        self._producer = producer
        self._consumer = consumer

    @property
    def is_concurrent(self) -> bool:
        """
        ``True``, since this is a group of concurrent flows.
        """
        return True

    @property
    def final_conduit(self) -> Consumer[T_SourceProduct_ret, T_Output_ret]:
        """[see superclass]"""
        return self._consumer

    @property
    def _source(self) -> BaseProducer[T_SourceProduct_ret]:
        """[see superclass]"""
        return self._producer

    @property
    def _processor(self) -> Consumer[T_SourceProduct_ret, T_Output_ret]:
        """[see superclass]"""
        return self._consumer

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return self._producer.n_concurrent_conduits

    def iter_concurrent_conduits(
        self,
    ) -> Iterator[_ProducerFlow[T_SourceProduct_ret, T_Output_ret]]:
        """[see superclass]"""
        for producer in self._producer.iter_concurrent_conduits():
            yield _ProducerFlow(producer=producer, consumer=self._consumer)

    async def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[_ProducerFlow[T_SourceProduct_ret, T_Output_ret]]:
        """[see superclass]"""
        async for producer in self._producer.aiter_concurrent_conduits():
            yield _ProducerFlow(producer=producer, consumer=self._consumer)

    @final
    def run(self) -> T_Output_ret:
        """[see superclass]"""
        # noinspection PyProtectedMember
        return _consume(producer=self._producer, consumer=self._consumer)

    @final
    async def arun(self) -> T_Output_ret:
        """[see superclass]"""

        # noinspection PyProtectedMember
        return await _aconsume(producer=self._producer, consumer=self._consumer)


#
# Auxiliary constants, functions and classes
#


def _consume(
    *,
    producer: BaseProducer[T_SourceProduct_ret],
    consumer: Consumer[T_SourceProduct_ret, T_Output_ret],
) -> T_Output_ret:
    """
    Consume products from a serial producer.

    :param producer: the producer to consume from
    :param consumer: the consumer to apply to the source
    :return: the resulting object
    """
    return consumer.consume(
        (index, product)
        for index, producer in enumerate(producer.iter_concurrent_conduits())
        for product in producer
    )


async def _aconsume(
    *,
    producer: BaseProducer[T_SourceProduct_ret],
    consumer: Consumer[T_SourceProduct_ret, T_Output_ret],
) -> T_Output_ret:
    """
    Consume products from a serial producer asynchronously.

    :param producer: the producer to consume from
    :param consumer: the consumer to apply to the source
    :return: the resulting object
    """

    # This inner function rewrites asynchronous iterators of products, replacing
    # each product with an annotated product that includes the index of
    # the producer that generated it.

    async def _annotate(
        producer_index: int, producer_: SerialProducer[T_SourceProduct_arg]
    ) -> AsyncIterator[tuple[int, T_SourceProduct_arg]]:
        async for product in producer_:
            yield (producer_index, product)

    # noinspection PyTypeChecker
    return await consumer.aconsume(
        async_flatten(
            _annotate(producer_index, producer)
            async for producer_index, producer in (
                aenumerate(producer.aiter_concurrent_conduits())
            )
        )
    )
