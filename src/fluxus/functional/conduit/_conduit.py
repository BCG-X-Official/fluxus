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
