"""
Implementation of ``DictConduit``.
"""

from __future__ import annotations

import logging
from abc import ABCMeta
from typing import final

from pytools.api import inheritdoc

from ...core import Conduit
from ..product import DictProduct

log = logging.getLogger(__name__)

__all__ = [
    "DictConduit",
]


@inheritdoc(match="[see superclass]")
class DictConduit(Conduit[DictProduct], metaclass=ABCMeta):
    """
    A conduit of dictionary products.

    Base class of all dictionary conduits in the functional flow API.
    """

    #: The name of this conduit.
    _name: str

    def __init__(self, *, name: str) -> None:
        """
        :param name: the name of this conduit
        """
        if not isinstance(name, str) or not name.isidentifier():
            raise ValueError(
                f"Step name can only include valid python "
                f"identifiers (a-z, A-Z, 0-9, _): {name!r}"
            )
        self._name = name

    @property
    @final
    def name(self) -> str:
        """[see superclass]"""
        return self._name
