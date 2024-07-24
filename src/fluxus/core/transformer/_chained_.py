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
from pytools.asyncio import async_flatten

from .._base import Processor, Source
from .._chained_base_ import _ChainedConduit, _SerialChainedConduit
from .._conduit import SerialConduit
from ..producer import BaseProducer, ConcurrentProducer, SerialProducer
from ._transformer_base import BaseTransformer, ConcurrentTransformer, SerialTransformer

log = logging.getLogger(__name__)


#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

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
    def source(self) -> SerialProducer[T_SourceProduct_ret]:
        """[see superclass]"""
        return self._producer

    @property
    def processor(
        self,
    ) -> SerialTransformer[T_SourceProduct_ret, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer

    def produce(self) -> Iterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer.process(input=self._producer)

    def aproduce(self) -> AsyncIterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        # noinspection PyTypeChecker
        return self.transformer.aprocess(input=self._producer)


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
    def source(self) -> SerialTransformer[T_SourceProduct_arg, T_SourceProduct_ret]:
        """[see superclass]"""
        return self.first

    @property
    def processor(
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
    @final
    def product_type(self) -> type[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer.product_type

    @property
    def source(self) -> BaseProducer[T_SourceProduct_ret]:
        """[see superclass]"""
        return self._producer

    @property
    def processor(self) -> Processor[T_SourceProduct_ret, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.transformer

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return (
            self._producer.n_concurrent_conduits
            * self.transformer.n_concurrent_conduits
        )

    def iter_concurrent_producers(
        self,
    ) -> Iterator[SerialProducer[T_TransformedProduct_ret]]:
        """[see superclass]"""

        for source in self._producer.iter_concurrent_producers():
            yield from self.transformer.iter_concurrent_producers(source=source)


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

    #: The transformer to apply to the producer
    transformer: BaseTransformer[T_SourceProduct_ret, T_Product_ret]

    def __init__(
        self,
        *,
        source: SerialProducer[T_SourceProduct_ret],
        transformer: BaseTransformer[T_SourceProduct_ret, T_Product_ret],
    ) -> None:
        """
        :param source: the producer to use as input
        :param transformer: the transformer to apply to the producer
        """
        super().__init__()
        self._producer = source
        self.transformer = transformer

    @property
    @final
    def product_type(self) -> type[T_Product_ret]:
        """[see superclass]"""
        return self.transformer.product_type

    @property
    def source(self) -> SerialProducer[T_SourceProduct_ret]:
        """[see superclass]"""
        return self._producer

    @property
    def processor(self) -> BaseTransformer[T_SourceProduct_ret, T_Product_ret]:
        """[see superclass]"""
        return self.transformer

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return self.transformer.n_concurrent_conduits

    def iter_concurrent_producers(self) -> Iterator[SerialProducer[T_Product_ret]]:
        """[see superclass]"""

        yield from self.transformer.iter_concurrent_producers(source=self._producer)


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
    def input_type(self) -> type[T_SourceProduct_arg]:
        """[see superclass]"""
        return self.first.input_type

    @property
    @final
    def product_type(self) -> type[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.second.product_type

    @property
    def source(self) -> Source[T_SourceProduct_ret]:
        """[see superclass]"""
        return self.first

    @property
    def processor(self) -> Processor[T_SourceProduct_ret, T_TransformedProduct_ret]:
        """[see superclass]"""
        return self.second

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return self.first.n_concurrent_conduits * self.second.n_concurrent_conduits

    @final
    def is_valid_source(self, source: SerialConduit[T_SourceProduct_arg]) -> bool:
        """[see superclass]"""
        return self.first.is_valid_source(source=source)

    def iter_concurrent_producers(
        self, *, source: SerialProducer[T_SourceProduct_arg]
    ) -> Iterator[SerialProducer[T_TransformedProduct_ret]]:
        """[see superclass]"""
        for producer in self.first.iter_concurrent_producers(source=source):
            yield from self.second.iter_concurrent_producers(source=producer)
