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
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any

from pytools.api import subsdoc
from pytools.expression.atomic import Id
from pytools.expression.composite import BinaryOperation, DictLiteral
from pytools.expression.operator import BinaryOperator

from ... import AsyncRepeat
from ..product import DictProduct
from ._conduit import FunctionalConduit

log = logging.getLogger(__name__)

__all__ = [
    "DictRepeat",
]


#
# Classes
#


@subsdoc(
    pattern=r"Base class for conduits that apply a function to a dictionary",
    replacement=(
        "A flow controller that repeats an associated subflow until a condition is met"
    ),
)
@subsdoc(
    pattern=r"#INTRO#",
    replacement=FunctionalConduit.__doc__ or "",
)
class DictRepeat(AsyncRepeat[DictProduct, DictProduct], FunctionalConduit):
    """
    #INTRO#

    As an example, the following function could be used as a repeat condition:

    .. code-block:: python

        def while_a_below_3(a: int) -> dict[str, Any] | None:
            if a < 3:
                # Repeat with a
                return dict(a=a)
            else:
                # Stop repeating
                return None

    The repeat step could be defined as follows:

    .. code-block:: python

        loop = repeat(
            "loop",
            step("add_one", add_one),
            test=while_a_below_3
        )

    The repeat step will repeat step ``add_one`` until the condition
    ``while_a_below_3`` returns ``None``.

    Example usage:

    .. code-block:: python

        result = run(loop, input=dict(a=0))

    The result will be

    .. code-block:: python

        RunResult(
            [
                {
                    'input': {'a': 0},
                    'add_one': {'a': 1},
                    'loop': {'a': 1},
                    'add_one#1': {'a': 2},
                    'loop#1': {'a': 2},
                    'add_one#2': {'a': 3}
                }
            ]
        )
    """

    _function: Callable[..., Mapping[str, Any] | Awaitable[Mapping[str, Any]] | None]

    @subsdoc(
        # Find the line that defines the _name parameter and insert a new string in the
        # next line
        pattern=r"(:param _name:.*)\n",
        replacement=(
            r"\1\n"
            r":param _function: the function called to test the repeat condition, "
            r"which returns a new dictionary to repeat with, or ``None`` if no repeat "
            r"should occur\n"
        ),
        using=FunctionalConduit.__init__,
    )
    def __init__(
        self,
        _name: str,
        _function: Callable[
            ..., Mapping[str, Any] | Awaitable[Mapping[str, Any]] | None
        ],
        /,
        **kwargs: Any,
    ) -> None:
        """[see above]"""
        # Set the function field first, because the superclass constructor will call
        # _validate_function() which requires the function to be set.
        super().__init__(_name, **kwargs)
        self._function = _function
        self._function_arguments = self._validate_function(
            step=_name, function=_function, kwargs=kwargs, returns_iterable=False
        )

    @property
    def function(
        self,
    ) -> Callable[..., Mapping[str, Any] | Awaitable[Mapping[str, Any]] | None]:
        """
        The function called to test the repeat condition.
        """
        return self._function

    async def atest(self, source_product: DictProduct) -> DictProduct | None:
        """
        Apply the function of this step to the dictionary managed by the source product
        to determine if the repeat condition is met, and if so, return the product to
        repeat with.

        :param source_product: the source product containing the dictionary to be
            passed to the function of this step
        :return: an async iterator of the resulting product or products
        """

        input_args = self._get_input_args(source_product)

        # Measure the start time of the step. We are interested in CPU time, not wall
        # time, so we use time.perf_counter() instead of time.time().
        start_time = time.perf_counter()

        # Call the function of this step with the input arguments. This may return the
        # actual result, an iterable of results, or an async iterable of results.
        test_result = self._function(**input_args)

        if isinstance(test_result, Awaitable):
            test_result = await test_result

        if test_result is None:
            return None

        if not isinstance(test_result, Mapping):
            raise TypeError(
                f"Test function {self._function.__name__}() of repeat {self.name!r} "
                f"must return None or a dictionary, but got {type(test_result)}"
            )

        # Measure the end time of the step.
        end_time = time.perf_counter()

        log.debug(
            f"Tested repeat condition {self.name!r} in {end_time - start_time:g} "
            f"seconds:\n"
            + str(
                BinaryOperation(
                    BinaryOperator.ASSIGN,
                    Id(self._function)(**input_args),
                    DictLiteral(**test_result),
                )
            )
        )

        return DictProduct(
            name=self.name,
            product_attributes=test_result,
            precursor=source_product,
            start_time=start_time,
            end_time=end_time,
        )


#
# Auxiliary functions
#


async def _awaitable_to_async_iter(
    x: Awaitable[Mapping[str, Any]]
) -> AsyncIterator[Mapping[str, Any]]:
    """
    Convert an awaitable to an async iterator.

    :param x: the awaitable to convert
    :return: an async iterator that yields the result of the awaitable
    """
    yield await x
