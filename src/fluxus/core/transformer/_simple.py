"""
Implementation of unions.
"""

from __future__ import annotations

import functools
import itertools
import logging
import operator
from collections.abc import AsyncIterator, Collection, Iterator
from typing import Any, Generic, TypeVar, cast

from pytools.api import as_tuple, inheritdoc
from pytools.asyncio import async_flatten, iter_sync_to_async
from pytools.expression import Expression

from ... import Passthrough
from .. import SerialConduit
from ._transformer_base import BaseTransformer, ConcurrentTransformer, SerialTransformer

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
# Constants
#

# The passthrough singleton instance.
_PASSTHROUGH = Passthrough()

#
# Classes
#


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
        self.transformers = as_tuple(
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
