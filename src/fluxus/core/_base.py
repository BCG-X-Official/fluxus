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
Implementation of `source` and `processor` base classes.

- Producers and transformers are `sources` because they generate outputs.
- Transformers and consumers are `processors` because they consume inputs.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterable, Collection, Iterable, Iterator
from typing import Any, Generic, Self, TypeVar, cast

from pytools.api import inheritdoc
from pytools.typing import get_common_generic_base, issubclass_generic

from ._conduit import Conduit, SerialConduit

log = logging.getLogger(__name__)

__all__ = [
    "SerialSource",
    "SerialProcessor",
    "Source",
    "Processor",
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
T_Output_ret = TypeVar("T_Output_ret", covariant=True)


class Source(Conduit[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta):
    """
    A conduit that produces or transforms products.

    This is a base class for :class:`.Producer` and :class:`.Transformer`.
    """

    @property
    def product_type(self) -> type[T_Product_ret]:
        """
        The type of the products produced by this conduit.
        """
        from .. import Passthrough

        return get_common_generic_base(
            cast(SerialSource[T_Product_ret], source).product_type
            for source in self.iter_concurrent_conduits()
            if not isinstance(source, Passthrough)
        )


class Processor(
    Conduit[T_Output_ret],
    Generic[T_SourceProduct_arg, T_Output_ret],
    metaclass=ABCMeta,
):
    """
    A transformer or consumer that attaches to a producer or transformer to process its
    products.

    This is a base class for :class:`.Transformer` and :class:`.Consumer`.
    """

    @property
    def input_type(self) -> type[T_SourceProduct_arg]:
        """
        The type of the input processed by this conduit.
        """
        return cast(type[T_SourceProduct_arg], self._get_type_arguments(Processor)[0])

    @abstractmethod
    def process(
        self, input: Iterable[T_SourceProduct_arg]
    ) -> list[T_Output_ret] | T_Output_ret:
        """
        Generate new products from the given input.

        :param input: the input products
        :return: the generated output or outputs
        """

    @abstractmethod
    async def aprocess(
        self, input: AsyncIterable[T_SourceProduct_arg]
    ) -> list[T_Output_ret] | T_Output_ret:
        """
        Generate new products asynchronously from the given input.

        :param input: the input products
        :return: the generated output or outputs
        """

    def is_valid_source(
        self,
        source: SerialConduit[T_SourceProduct_arg],
    ) -> bool:
        """
        Check if the given producer or transformer is a valid source for this conduit.

        The precursor is the final conduit of the source producer or group.
        Returning ``False`` will cause a :class:`TypeError` to be raised.

        :param source: the source conduit to check
        :return: ``True`` if the given conduit is valid source for this conduit,
            ``False`` otherwise
        """
        from .. import Passthrough

        if not isinstance(source, SerialSource):
            return False

        ingoing_product_type = source.product_type
        return all(
            issubclass_generic(
                ingoing_product_type,
                cast(Self, processor).input_type,
            )
            for processor in self.iter_concurrent_conduits()
            if not isinstance(processor, Passthrough)
        )


class SerialSource(
    SerialConduit[T_Product_ret],
    Source[T_Product_ret],
    Generic[T_Product_ret],
    metaclass=ABCMeta,
):
    """
    A conduit that produces or transforms products.
    """

    @property
    def product_type(self) -> type[T_Product_ret]:
        """
        The type of the product produced by this conduit.
        """
        return cast(type[T_Product_ret], self._get_type_arguments(SerialSource)[0])


@inheritdoc(match="[see superclass]")
class SerialProcessor(
    Processor[T_SourceProduct_arg, T_Product_ret],
    SerialConduit[T_Product_ret],
    Generic[T_SourceProduct_arg, T_Product_ret],
    metaclass=ABCMeta,
):
    """
    A processor that processes products sequentially.
    """

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """[see superclass]"""
        for conduit in ingoing:
            yield conduit, self
