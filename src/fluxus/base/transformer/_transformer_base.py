"""
Implementation of conduit base classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import Any, Generic, Self, TypeVar, final, overload

from pytools.api import inheritdoc
from pytools.asyncio import async_flatten
from pytools.typing import (
    get_common_generic_base,
    get_common_generic_subclass,
    issubclass_generic,
)

from ..._passthrough import Passthrough
from .. import ConcurrentConduit, Processor, SerialProcessor, SerialSource, Source
from ..producer import BaseProducer, SerialProducer

log = logging.getLogger(__name__)

__all__ = [
    "BaseTransformer",
    "ConcurrentTransformer",
    "SerialTransformer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_Product_ret = TypeVar("T_Product_ret", covariant=True)
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)


#
# Classes
#


@inheritdoc(match="[see superclass]")
class BaseTransformer(
    Processor[T_SourceProduct_arg, T_TransformedProduct_ret],
    Source[T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
    metaclass=ABCMeta,
):
    """
    A conduit that transforms products from a source – this is either a
    :class:`.SerialTransformer` or a :class:`.TransformerGroup`.
    """

    @abstractmethod
    def iter_concurrent_conduits(
        self,
    ) -> Iterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""

    @abstractmethod
    def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
    ]:
        """[see superclass]"""

    @final
    def process(
        self, input: Iterable[T_SourceProduct_arg]
    ) -> list[T_TransformedProduct_ret]:
        """
        Transform the given products.

        :param input: the products to transform
        :return: the transformed products
        """
        from ...simple import SimpleProducer

        return list(
            SimpleProducer[self.input_type](input) >> self  # type: ignore[name-defined]
        )

    @final
    async def aprocess(
        self, input: AsyncIterable[T_SourceProduct_arg]
    ) -> list[T_TransformedProduct_ret]:
        """
        Transform the given products asynchronously.

        :param input: the products to transform
        :return: the transformed products
        """
        from ...simple import SimpleAsyncProducer

        return [
            product
            async for product in (
                SimpleAsyncProducer[self.input_type](  # type: ignore[name-defined]
                    input
                )
                >> self
            )
        ]

    def __and__(
        self,
        other: (
            BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret] | Passthrough
        ),
    ) -> BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]:
        input_type: type[T_SourceProduct_arg]
        product_type: type[T_TransformedProduct_ret]

        if isinstance(other, Passthrough):
            for transformer in self.iter_concurrent_conduits():
                _validate_concurrent_passthrough(transformer)
            input_type = self.input_type
            product_type = self.product_type
        elif not isinstance(other, BaseTransformer):
            return NotImplemented
        else:
            input_type = get_common_generic_subclass(
                (self.input_type, other.input_type)
            )
            product_type = get_common_generic_base(
                (self.product_type, other.product_type)
            )
        from . import SimpleConcurrentTransformer

        return SimpleConcurrentTransformer[
            input_type, product_type  # type: ignore[valid-type]
        ](self, other)

    def __rand__(
        self, other: Passthrough
    ) -> BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]:
        if isinstance(other, Passthrough):
            for transformer in self.iter_concurrent_conduits():
                _validate_concurrent_passthrough(transformer)

            from . import SimpleConcurrentTransformer

            return SimpleConcurrentTransformer[
                self.input_type, self.product_type  # type: ignore[name-defined]
            ](other, self)
        else:
            return NotImplemented

    @overload
    def __rshift__(
        self,
        other: SerialTransformer[T_TransformedProduct_ret, T_Product_ret],
    ) -> (
        BaseTransformer[T_SourceProduct_arg, T_Product_ret]
        | SerialTransformer[T_SourceProduct_arg, T_Product_ret]
    ):
        pass  # pragma: no cover

    @overload
    def __rshift__(
        self,
        other: BaseTransformer[T_TransformedProduct_ret, T_Product_ret],
    ) -> BaseTransformer[T_SourceProduct_arg, T_Product_ret]:
        pass  # pragma: no cover

    def __rshift__(
        self,
        other: (
            BaseTransformer[T_TransformedProduct_ret, T_Product_ret]
            | SerialTransformer[T_TransformedProduct_ret, T_Product_ret]
        ),
    ) -> (
        BaseTransformer[T_SourceProduct_arg, T_Product_ret]
        | SerialTransformer[T_SourceProduct_arg, T_Product_ret]
    ):
        if isinstance(other, BaseTransformer):
            from ._chained_ import _ChainedConcurrentTransformer

            return _ChainedConcurrentTransformer(self, other)
        else:
            return NotImplemented

    @overload
    def __rrshift__(
        self, other: SerialProducer[T_SourceProduct_arg]
    ) -> (
        SerialProducer[T_TransformedProduct_ret]
        | BaseProducer[T_TransformedProduct_ret]
    ):
        pass

    @overload
    def __rrshift__(
        self, other: BaseProducer[T_SourceProduct_arg]
    ) -> BaseProducer[T_TransformedProduct_ret]:
        pass

    def __rrshift__(
        self,
        other: BaseProducer[T_SourceProduct_arg],
    ) -> BaseProducer[T_TransformedProduct_ret] | Self:
        if isinstance(other, SerialProducer):
            from ._chained_ import _ChainedConcurrentTransformedProducer

            # noinspection PyTypeChecker
            return _ChainedConcurrentTransformedProducer(
                source=other, transformer_group=self
            )
        elif isinstance(other, BaseProducer):
            from ._chained_ import _ChainedConcurrentProducer

            return _ChainedConcurrentProducer(source=other, transformer=self)
        else:
            return NotImplemented


@inheritdoc(match="[see superclass]")
class SerialTransformer(
    SerialProcessor[T_SourceProduct_arg, T_TransformedProduct_ret],
    SerialSource[T_TransformedProduct_ret],
    BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
):
    """
    A transformer that generates new products from the products of a producer.
    """

    @final
    def iter_concurrent_conduits(
        self,
    ) -> Iterator[SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]]:
        """[see superclass]"""
        yield self

    @final
    async def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[
        SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]
    ]:
        """[see superclass]"""
        yield self

    @abstractmethod
    def transform(
        self, source_product: T_SourceProduct_arg
    ) -> Iterator[T_TransformedProduct_ret]:
        """
        Generate a new product from an existing product.

        :param source_product: an existing product to use as input
        :return: the new product
        """

    async def atransform(
        self, source_product: T_SourceProduct_arg
    ) -> AsyncIterator[T_TransformedProduct_ret]:
        """
        Generate a new product asynchronously, using an existing product as input.

        By default, defers to the synchronous variant, :meth:`transform`.

        :param source_product: the existing product to use as input
        :return: the new product
        """
        for tx in self.transform(source_product):
            yield tx

    def iter(
        self, source: Iterable[T_SourceProduct_arg]
    ) -> Iterator[T_TransformedProduct_ret]:
        """
        Generate new products, using an existing producer as input.

        :param source: an existing producer to use as input (optional)
        :return: the new products
        """
        for product in source:
            yield from self.transform(product)

    def aiter(
        self, source: AsyncIterable[T_SourceProduct_arg]
    ) -> AsyncIterator[T_TransformedProduct_ret]:
        """
        Generate new products asynchronously, using an existing producer as input.

        :param source: an existing producer to use as input (optional)
        :return: the new products
        """
        # noinspection PyTypeChecker
        return async_flatten(self.atransform(product) async for product in source)

    @overload
    def __rshift__(
        self,
        other: SerialTransformer[T_TransformedProduct_ret, T_Product_ret],
    ) -> SerialTransformer[T_SourceProduct_arg, T_Product_ret]:
        pass  # pragma: no cover

    @overload
    def __rshift__(
        self,
        other: BaseTransformer[T_TransformedProduct_ret, T_Product_ret],
    ) -> BaseTransformer[T_SourceProduct_arg, T_Product_ret]:
        pass  # pragma: no cover

    def __rshift__(
        self,
        other: (
            BaseTransformer[T_TransformedProduct_ret, T_Product_ret]
            | SerialTransformer[T_TransformedProduct_ret, T_Product_ret]
        ),
    ) -> (
        BaseTransformer[T_SourceProduct_arg, T_Product_ret]
        | SerialTransformer[T_SourceProduct_arg, T_Product_ret]
    ):
        # Create a combined transformer where the output of this transformer is used as
        # the input of the other transformer
        if isinstance(other, SerialTransformer):
            # We import locally to avoid circular imports
            from ._chained_ import _ChainedTransformer

            return _ChainedTransformer(self, other)

        return super().__rshift__(other)

    @overload
    def __rrshift__(
        self, other: SerialProducer[T_SourceProduct_arg]
    ) -> SerialProducer[T_TransformedProduct_ret]:
        pass  # pragma: no cover

    @overload
    def __rrshift__(
        self, other: BaseProducer[T_SourceProduct_arg]
    ) -> BaseProducer[T_TransformedProduct_ret]:
        pass  # pragma: no cover

    def __rrshift__(
        self, other: BaseProducer[T_SourceProduct_arg]
    ) -> BaseProducer[T_TransformedProduct_ret]:
        if isinstance(other, SerialProducer):
            # We import locally to avoid circular imports
            from ._chained_ import _ChainedProducer

            return _ChainedProducer(producer=other, transformer=self)
        else:
            return super().__rrshift__(other)


#
# Auxiliary functions
#


def _validate_concurrent_passthrough(
    conduit: SerialTransformer[Any, Any] | Passthrough
) -> None:
    """
    Validate that the given conduit is valid as a concurrent conduit with a passthrough.

    To be valid, its input type must be a subtype of its product type.

    :param conduit: the conduit to validate
    """

    if not (
        isinstance(conduit, Passthrough)
        or (issubclass_generic(conduit.input_type, conduit.product_type))
    ):
        raise TypeError(
            "Conduit is not a valid concurrent conduit with a passthrough because its "
            f"input type {conduit.input_type} is not a subtype of its product type "
            f"{conduit.product_type}:\n{conduit}"
        )


@inheritdoc(match="[see superclass]")
class ConcurrentTransformer(
    BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    ConcurrentConduit[T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
    metaclass=ABCMeta,
):
    """
    A collection of one or more transformers, operating in parallel.
    """

    @property
    def input_type(self) -> type[T_SourceProduct_arg]:
        """[see superclass]"""
        return get_common_generic_subclass(
            transformer.input_type
            for transformer in self.iter_concurrent_conduits()
            if not isinstance(transformer, Passthrough)
        )
