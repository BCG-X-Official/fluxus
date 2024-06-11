"""
Implementation of conduit base classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Any, Generic, TypeVar, final

from pytools.asyncio import arun, iter_async_to_sync
from pytools.typing import issubclass_generic

from ._passthrough import Passthrough
from .base import AtomicConduit
from .base.transformer import SerialTransformer

log = logging.getLogger(__name__)

__all__ = [
    "AsyncTransformer",
    "Transformer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_Product_arg = TypeVar("T_Product_arg", contravariant=True)
T_Product_ret = TypeVar("T_Product_ret", covariant=True)
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)


#
# Classes
#


class Transformer(
    AtomicConduit[T_TransformedProduct_ret],
    SerialTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
    metaclass=ABCMeta,
):
    """
    An atomic transformer that generates new products from the products of a producer.
    """


class AsyncTransformer(
    Transformer[T_SourceProduct_arg, T_Product_ret],
    Generic[T_SourceProduct_arg, T_Product_ret],
    metaclass=ABCMeta,
):
    """
    A transformer designed for asynchronous I/O.

    Synchronous iteration is supported but discouraged, as it creates a new event loop
    and blocks the current thread until the iteration is complete. It is preferable to
    use asynchronous iteration instead.
    """

    @final
    def transform(self, source_product: T_SourceProduct_arg) -> Iterator[T_Product_ret]:
        """
        Generate a new product, using an existing product as input.

        This method is implemented for compatibility with synchronous code, but
        preferably, :meth:`.atransform` should be used instead and called from
        within an event loop.

        When called from outside an event loop, this method will create an event loop
        using :meth:`arun`, transform the product using :meth:`atransform` and
        block the current thread until the iteration is complete. The new product will
        then be returned.

        :param source_product: the existing product to use as input
        :return: the new product
        """
        return arun(iter_async_to_sync(self.atransform(source_product)))

    @abstractmethod
    def atransform(
        self, source_product: T_SourceProduct_arg
    ) -> AsyncIterator[T_Product_ret]:
        """
        Generate a new product asynchronously, using an existing product as input.

        :param source_product: the existing product to use as input
        :return: the new product
        """


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
        or issubclass_generic(conduit.input_type, conduit.product_type)
    ):
        raise TypeError(
            "Conduit is not a valid concurrent conduit with a passthrough because its "
            f"input type {conduit.input_type} is not a subtype of its product type "
            f"{conduit.product_type}:\n{conduit}"
        )
