"""
Implementation of conduit base classes.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from typing import Any, Generic, TypeVar

from ._consumer import Consumer
from .base import Conduit

log = logging.getLogger(__name__)

__all__ = [
    "Flow",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#

T_Output_ret = TypeVar("T_Output_ret", covariant=True)


#
# Classes
#


class Flow(Conduit[T_Output_ret], Generic[T_Output_ret], metaclass=ABCMeta):
    """
    A flow is a sequence of producers, transformers, and consumers that can be
    executed to produce a result.
    """

    @property
    @abstractmethod
    def final_conduit(self) -> Consumer[Any, T_Output_ret]:
        """
        The final conduit in the flow; this is the consumer that terminates the flow.
        """

    @abstractmethod
    def run(self) -> T_Output_ret:
        """
        Run the flow.

        :return: the result of the flow
        """

    @abstractmethod
    async def arun(self) -> T_Output_ret:
        """
        Run the flow asynchronously.

        :return: the result of the flow
        """
