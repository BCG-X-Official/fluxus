"""
Implementation of conduit and subconduit base classes
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterator
from typing import Any, Never, final

from pytools.api import inheritdoc
from pytools.meta import SingletonABCMeta

# Special import from private submodule to avoid circular imports
# noinspection PyProtectedMember
from .base._conduit import SerialConduit

log = logging.getLogger(__name__)

__all__ = [
    "Passthrough",
]


#
# Classes
#


@final
@inheritdoc(match="[see superclass]")
class Passthrough(SerialConduit[Any], metaclass=SingletonABCMeta):
    """
    Can be used in place of a transformer when the flow should pass through the input
    without modification.

    May only be grouped concurrently with other transformers whose input type is a
    subtype of the output type.

    Chaining a pass-through object with a transformer yields the original transformer.
    """

    @property
    def is_chained(self) -> bool:
        """
        ``False``, since a passthrough is not a composition of multiple conduits.
        """
        return False

    @property
    def final_conduit(self) -> Never:
        """[see superclass]"""
        raise NotImplementedError("Final conduit is not defined for passthroughs")

    def get_final_conduits(self) -> Iterator[Never]:
        """
        :return: An empty iterator; passthroughs do not define a final conduit.
        """
        # Passthrough conduits are transparent, so we return an empty iterator
        yield from ()

    @property
    def _has_passthrough(self) -> bool:
        """[see superclass]"""
        return True

    def get_connections(self, *, ingoing: Collection[SerialConduit[Any]]) -> Never:
        """
        Fails with a :class:`NotImplementedError` since passthroughs are transparent in
        flows and therefore connections are not defined.

        :param ingoing: The ingoing conduits (ignored)
        :return: Nothing; passthroughs do not define connections
        :raises NotImplementedError: Passthroughs do not define connections
        """
        raise NotImplementedError("Connections are not defined for passthroughs")
