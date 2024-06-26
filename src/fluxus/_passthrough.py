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
Implementation of conduit and subconduit base classes
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterator
from typing import Any, final

from typing_extensions import Never

from pytools.api import inheritdoc
from pytools.meta import SingletonABCMeta

# Special import from private submodule to avoid circular imports
# noinspection PyProtectedMember
from .core._conduit import SerialConduit

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
        Returns an empty iterator since passthroughs do not define a final conduit.

        :return: an empty iterator
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

        :param ingoing: the ingoing conduits (ignored)
        :return: nothing; passthroughs do not define connections
        :raises NotImplementedError: passthroughs do not define connections
        """
        raise NotImplementedError("Connections are not defined for passthroughs")
