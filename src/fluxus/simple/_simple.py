"""
Implementation of unions.
"""

from __future__ import annotations

import functools
import itertools
import logging
import operator
from collections.abc import AsyncIterable, AsyncIterator, Collection, Iterable, Iterator
from typing import Any, Generic, TypeVar, cast

from pytools.api import inheritdoc, to_tuple
from pytools.asyncio import async_flatten, iter_sync_to_async
from pytools.expression import Expression
from pytools.typing import isinstance_generic

from .._passthrough import Passthrough
from .._producer import AsyncProducer, Producer
from ..base import SerialConduit
from ..base.producer import BaseProducer, ConcurrentProducer, SerialProducer
from ..base.transformer import BaseTransformer, ConcurrentTransformer, SerialTransformer

log = logging.getLogger(__name__)

__all__ = [
    "SimpleAsyncProducer",
    "SimpleProducer",
    "SimpleConcurrentProducer",
    "SimpleConcurrentTransformer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_Product_ret = TypeVar("T_Product_ret", covariant=True)
T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_SourceProduct_ret = TypeVar("T_SourceProduct_ret", covariant=True)
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)


#
# Constants
#

# The passthrough singleton instance.
_PASSTHROUGH = Passthrough()

#
# Classes
#


@inheritdoc(match="[see superclass]")
class SimpleProducer(Producer[T_Product_ret], Generic[T_Product_ret]):
    """
    A simple producer that iterates over a given list of products.
    """

    #: The products of this producer.
    products: Iterable[T_Product_ret]

    def __init__(self, products: Iterable[T_Product_ret]) -> None:
        """
        :param products: the products to iterate over
        """
        if not isinstance(products, Iterable):
            raise TypeError(
                f"Products must be an iterable, not {type(products).__name__}"
            )
        self.products = products = to_tuple(products)

        product_type = self.product_type
        mismatched_products = [
            product
            for product in products
            if not isinstance_generic(product, product_type)
        ]
        if mismatched_products:
            raise TypeError(
                f"Arg products contains products that are not of expected type"
                f"{product_type}: " + ", ".join(map(repr, mismatched_products))
            )

    def iter(self) -> Iterator[T_Product_ret]:
        """[see superclass]"""
        return iter(self.products)


@inheritdoc(match="[see superclass]")
class SimpleAsyncProducer(AsyncProducer[T_Product_ret], Generic[T_Product_ret]):
    """
    A simple asynchronous producer that iterates over a given list of products.
    """

    #: The products of this producer.
    products: AsyncIterable[T_Product_ret]

    def __init__(self, products: AsyncIterable[T_Product_ret]) -> None:
        """
        :param products: the products to iterate over; must be an async iterable
            and will not be materialized by this producer
        """
        self.products = products

    def aiter(self) -> AsyncIterator[T_Product_ret]:
        """[see superclass]"""
        return aiter(self.products)


@inheritdoc(match="[see superclass]")
class SimpleConcurrentProducer(
    ConcurrentProducer[T_SourceProduct_ret], Generic[T_SourceProduct_ret]
):
    """
    A simple group that manages a collection of producers.
    """

    #: The response sources this producer provides.
    producers: tuple[BaseProducer[T_SourceProduct_ret], ...]

    def __init__(
        self,
        *producers: BaseProducer[T_SourceProduct_ret],
    ) -> None:
        """
        :param producers: the response producer(s) this producer uses to generate
            response groups
        """
        super().__init__()

        self.producers = to_tuple(
            itertools.chain(*map(_flatten_concurrent_producers, producers)),
            element_type=cast(
                tuple[
                    type[BaseProducer[T_SourceProduct_ret]],
                    ...,
                ],
                BaseProducer,
            ),
            arg_name="producers",
        )

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return sum(producer.n_concurrent_conduits for producer in self.producers)

    def get_final_conduits(self) -> Iterator[SerialConduit[T_SourceProduct_ret]]:
        """[see superclass]"""
        for producer in self.producers:
            yield from producer.get_final_conduits()

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """[see superclass]"""
        assert not ingoing, "Producer groups cannot have ingoing conduits"
        for producer in self.producers:
            yield from producer.get_connections(ingoing=ingoing)

    def iter_concurrent_conduits(
        self,
    ) -> Iterator[SerialProducer[T_SourceProduct_ret]]:
        """[see superclass]"""
        for prod in self.producers:
            yield from prod.iter_concurrent_conduits()

    def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[SerialProducer[T_SourceProduct_ret]]:
        """[see superclass]"""

        # noinspection PyTypeChecker
        return async_flatten(
            prod.aiter_concurrent_conduits()
            async for prod in iter_sync_to_async(self.producers)
        )

    def to_expression(self, *, compact: bool = False) -> Expression:
        """[see superclass]"""
        return functools.reduce(
            operator.and_,
            (producer.to_expression(compact=compact) for producer in self.producers),
        )


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
        self.transformers = to_tuple(
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

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return sum(
            transformer.n_concurrent_conduits for transformer in self.transformers
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
            if transformer is not _PASSTHROUGH:
                yield from transformer.get_connections(ingoing=ingoing)

    def iter_concurrent_conduits(
        self,
    ) -> Iterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""
        for transformer in self.transformers:
            yield from transformer.iter_concurrent_conduits()

    def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""
        # noinspection PyTypeChecker
        return async_flatten(
            transformer.aiter_concurrent_conduits()
            async for transformer in iter_sync_to_async(self.transformers)
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
# Auxiliary functions
#


def _flatten_concurrent_producers(
    producer: BaseProducer[T_SourceProduct_ret],
) -> Iterator[BaseProducer[T_SourceProduct_ret]]:
    """
    Iterate over the given producer or its sub-producers, if they are contained in a
    (possibly nested) simple concurrent producer.

    :param producer: the producer to flatten
    :return: an iterator over the given producer or its sub-producers
    """
    if isinstance(producer, SimpleConcurrentProducer):
        for producer in producer.producers:
            yield from _flatten_concurrent_producers(producer)
    else:
        yield producer


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
