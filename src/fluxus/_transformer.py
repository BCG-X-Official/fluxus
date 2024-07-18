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
Implementation of transformers.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Generic, TypeVar, final

from pytools.asyncio import arun, iter_async_to_sync

from .core import AtomicConduit
from .core.transformer import SerialTransformer

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
