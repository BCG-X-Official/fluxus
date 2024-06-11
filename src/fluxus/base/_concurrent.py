"""
Implementation of ``MyClass``.
"""

from __future__ import annotations

import logging
from abc import ABCMeta
from typing import Any, Generic, Self, TypeVar, final

from ._conduit import Conduit

log = logging.getLogger(__name__)


__all__ = [
    "ConcurrentConduit",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_Product_ret = TypeVar("T_Product_ret", covariant=True)

#
# Classes
#


class ConcurrentConduit(
    Conduit[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta
):
    """
    A conduit made up of multiple concurrent conduits.

    This includes producer groups (see :class:`.ProducerGroup`) and transformer groups
    (see :class:`.TransformerGroup`).
    """

    @property
    @final
    def is_concurrent(self) -> bool:
        """
        ``True``, since this is a group of concurrent conduits.
        """
        return True

    @property
    def is_chained(self) -> bool:
        """[see superclass]"""
        return any(conduit.is_chained for conduit in self.iter_concurrent_conduits())

    @property
    def final_conduit(self) -> Self:
        """
        ``self``, since this is a group of concurrent conduits and has no final
        conduit on a more granular level.
        """
        return self


# assign Any to suppress "unused import" warning
_ = Any
