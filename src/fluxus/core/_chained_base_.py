"""
Implementation of composition classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Collection, Iterator
from typing import Any, Generic, TypeVar, cast, final

from pytools.api import inheritdoc
from pytools.expression import Expression

from . import Conduit, Processor, SerialConduit, SerialSource, Source

log = logging.getLogger(__name__)


#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_Output_ret = TypeVar("T_Output_ret", covariant=True)
T_SourceProduct_ret = TypeVar("T_SourceProduct_ret", covariant=True)


#
# Classes
#


@inheritdoc(match="[see superclass]")
class _ChainedConduit(
    Conduit[T_Output_ret], Generic[T_SourceProduct_ret, T_Output_ret], metaclass=ABCMeta
):
    """
    A conduit that is the result of sequentially chaining two conduits, one acting
    as the source and the other processing the output of the source.
    """

    @property
    @final
    def is_chained(self) -> bool:
        """
        ``True``, since this is a composition of chained conduits.
        """
        return True

    @property
    @abstractmethod
    def _source(self) -> Source[T_SourceProduct_ret]:
        """
        The source producer of this conduit.
        """

    @property
    @abstractmethod
    def _processor(self) -> Processor[T_SourceProduct_ret, T_Output_ret]:
        """
        The second conduit in this chained conduit, processing the output of the
        :attr:`._source` conduit.
        """

    def get_final_conduits(self) -> Iterator[SerialConduit[T_Output_ret]]:
        """[see superclass]"""
        if self._processor._has_passthrough:
            yield from cast(
                Iterator[SerialConduit[T_Output_ret]], self._source.get_final_conduits()
            )
        yield from self._processor.get_final_conduits()

    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """
        Get all conduit-to-conduit connections in the flow leading up to this conduit.

        :return: an iterable of connections
        """
        source = self._source
        processor = self._processor

        # We first yield all connections from within the source
        yield from source.get_connections(ingoing=ingoing)

        # We get the list of all ingoing conduits of the processor
        processor_ingoing = list(source.get_final_conduits())

        # If the source includes a pass-through, we add the original ingoing conduits
        if source._has_passthrough:
            processor_ingoing.extend(ingoing)

        # Then we get all connections of the processor, including ingoing connections
        yield from processor.get_connections(ingoing=processor_ingoing)

    def to_expression(self, *, compact: bool = False) -> Expression:
        """[see superclass]"""
        return self._source.to_expression(
            compact=compact
        ) >> self._processor.to_expression(compact=compact)


class _SerialChainedConduit(
    _ChainedConduit[T_SourceProduct_ret, T_Output_ret],
    SerialConduit[T_Output_ret],
    Generic[T_SourceProduct_ret, T_Output_ret],
):
    """
    A chained conduit that is a sequential composition of two conduits, one acting as
    the source and the other as the final conduit.
    """

    @property
    @abstractmethod
    def _source(self) -> SerialSource[T_SourceProduct_ret]:
        """[see superclass]"""

    @property
    def chained_conduits(self) -> Iterator[SerialConduit[Any]]:
        """
        The chained conduits in the flow leading up to this conduit.
        """
        yield from self._source.chained_conduits
        yield self.final_conduit
