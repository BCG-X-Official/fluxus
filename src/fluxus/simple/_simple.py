"""
Implementation of unions.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import Generic, TypeVar

from pytools.api import inheritdoc, to_tuple
from pytools.typing import isinstance_generic

from .._passthrough import Passthrough
from .._producer import AsyncProducer, Producer

log = logging.getLogger(__name__)

__all__ = [
    "SimpleAsyncProducer",
    "SimpleProducer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_Product_ret = TypeVar("T_Product_ret", covariant=True)


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
