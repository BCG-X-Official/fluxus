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
from typing import Any, Generic, TypeVar, cast

from pytools.api import as_tuple, inheritdoc
from pytools.asyncio import async_flatten, iter_sync_to_async
from pytools.expression import Expression

from ... import Passthrough
from .. import SerialConduit
from ._producer_base import BaseProducer, ConcurrentProducer, SerialProducer

log = logging.getLogger(__name__)

__all__ = [
    "SimpleConcurrentProducer",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_SourceProduct_ret = TypeVar("T_SourceProduct_ret", covariant=True)


#
# Constants
#

# The passthrough singleton instance.
_PASSTHROUGH = Passthrough()

#
# Classes
#


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

        self.producers = as_tuple(
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
