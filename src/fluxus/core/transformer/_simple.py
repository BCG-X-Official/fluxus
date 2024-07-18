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
Implementation of unions.
"""

from __future__ import annotations

import functools
import itertools
import logging
import operator
from collections.abc import AsyncIterator, Collection, Iterator
from typing import Any, Generic, TypeVar, cast, final

from pytools.api import as_tuple, inheritdoc
from pytools.expression import Expression
from pytools.typing import get_common_generic_base, get_common_generic_subclass

from ... import Passthrough
from .. import SerialConduit
from ..producer import SerialProducer
from ._transformer_base import BaseTransformer, ConcurrentTransformer

log = logging.getLogger(__name__)

__all__ = [
    "SimpleConcurrentTransformer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)


#
# Classes
#


@final
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
        self.transformers = transformers = as_tuple(
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

        input_types = {
            transformer.input_type
            for transformer in transformers
            if not isinstance(transformer, Passthrough)
        }
        try:
            self._input_type = get_common_generic_subclass(input_types)
        except TypeError as e:
            raise TypeError(
                "Transformers have incompatible input types: "
                + ", ".join(sorted(input_type.__name__ for input_type in input_types))
            ) from e

        product_types = {
            transformer.product_type
            for transformer in transformers
            if not isinstance(transformer, Passthrough)
        }
        try:
            self._product_type = get_common_generic_base(product_types)
        except TypeError as e:
            raise TypeError(
                "Transformers have incompatible product types: "
                + ", ".join(
                    sorted(product_type.__name__ for product_type in product_types)
                )
            ) from e

    @property
    def input_type(self) -> type[T_SourceProduct_arg]:
        """[see superclass]"""
        return self._input_type

    @property
    def product_type(self) -> type[T_TransformedProduct_ret]:
        """[see superclass]"""
        return self._product_type

    @property
    def is_chained(self) -> bool:
        """[see superclass]"""
        return any(transformer.is_chained for transformer in self.transformers)

    @property
    def n_concurrent_conduits(self) -> int:
        """[see superclass]"""
        return sum(
            transformer.n_concurrent_conduits for transformer in self.transformers
        )

    def is_valid_source(self, source: SerialConduit[T_SourceProduct_arg]) -> bool:
        """[see superclass]"""
        return all(
            transformer.is_valid_source(source=source)
            for transformer in self.transformers
            if not isinstance(transformer, Passthrough)
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
            if not isinstance(transformer, Passthrough):
                yield from transformer.get_connections(ingoing=ingoing)

    def get_isolated_conduits(
        self,
    ) -> Iterator[SerialConduit[T_TransformedProduct_ret]]:
        """[see superclass]"""
        for transformer in self.transformers:
            yield from transformer.get_isolated_conduits()

    def iter_concurrent_producers(
        self, *, source: SerialProducer[T_SourceProduct_arg]
    ) -> Iterator[SerialProducer[T_TransformedProduct_ret]]:
        """[see superclass]"""
        from ..transformer._chained_ import _BufferedProducer

        source_product_type = source.product_type
        buffered_source = _BufferedProducer[
            source_product_type  # type: ignore[valid-type]
        ](source)
        for transformer in self.transformers:
            if isinstance(transformer, Passthrough):
                yield buffered_source
            else:
                yield from transformer.iter_concurrent_producers(source=buffered_source)

    async def aiter_concurrent_producers(
        self, *, source: SerialProducer[T_SourceProduct_arg]
    ) -> AsyncIterator[SerialProducer[T_TransformedProduct_ret]]:
        """[see superclass]"""
        n_transformers = len(self.transformers)
        from ..transformer._chained_ import _AsyncBufferedProducer

        for buffered_source, transformer in zip(
            _AsyncBufferedProducer.create(source, n=n_transformers), self.transformers
        ):
            if isinstance(transformer, Passthrough):
                yield cast(SerialProducer[T_TransformedProduct_ret], buffered_source)
            else:
                async for producer in transformer.aiter_concurrent_producers(
                    source=buffered_source
                ):
                    yield producer

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
