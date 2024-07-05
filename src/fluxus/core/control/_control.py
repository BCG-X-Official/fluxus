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
Implementation of ``Repeat``.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar

from .. import AtomicConduit, SerialProcessor, SerialSource
from ..transformer import BaseTransformer

log = logging.getLogger(__name__)

__all__ = [
    "FlowControl",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_TransformedProduct_ret = TypeVar("T_TransformedProduct_ret", covariant=True)


#
# Classes
#


class FlowControl(
    SerialProcessor[T_SourceProduct_arg, T_TransformedProduct_ret],
    SerialSource[T_TransformedProduct_ret],
    AtomicConduit[T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
    metaclass=ABCMeta,
):
    """
    A `flow control` is a mechanism that dynamically alters the execution of the flow.

    These controls modify the behaviour of the flow based on certain conditions or
    requirements. Flow controls change how steps within a data flow are executed.
    For example, they can introduce conditional logic, repetition, or merging of
    products from parallel flows.
    """

    @abstractmethod
    def __rmatmul__(
        self, other: BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]
    ) -> BaseTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]:
        pass
