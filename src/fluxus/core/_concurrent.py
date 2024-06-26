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
Implementation of ``ConcurrentConduit``.
"""

from __future__ import annotations

import logging
from abc import ABCMeta
from typing import Any, Generic, TypeVar, final

from typing_extensions import Self

from pytools.api import inheritdoc

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


@inheritdoc(match="[see superclass]")
class ConcurrentConduit(
    Conduit[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta
):
    """
    A conduit made up of multiple concurrent conduits.

    This includes concurrent producers (see :class:`.ConcurrentProducer`) and concurrent
    transformers (see :class:`.ConcurrentTransformer`).
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
