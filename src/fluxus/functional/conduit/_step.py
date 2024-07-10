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
Implementation of ``Step``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
)
from typing import Any, cast

from pytools.api import subsdoc
from pytools.asyncio import iter_sync_to_async
from pytools.expression.atomic import Id
from pytools.expression.composite import BinaryOperation, DictLiteral
from pytools.expression.operator import BinaryOperator

from ... import AsyncTransformer
from ..product import DictProduct
from ._conduit import FunctionalConduit

log = logging.getLogger(__name__)

__all__ = [
    "Step",
]

#
# Constants
#


#
# Step classes
#


@subsdoc(
    pattern=r"Base class for conduits that apply",
    replacement="A step in a flow that applies",
)
@subsdoc(
    pattern=r"#INTRO#",
    replacement=FunctionalConduit.__doc__ or "",
)
class Step(FunctionalConduit, AsyncTransformer[DictProduct, DictProduct]):
    """
    #INTRO#

    As an example, the following function could be used as a step:

    .. code-block:: python

        def add_one(x):
            return dict(x=x + 1)

    The function ``add_one`` takes a single argument ``x`` and returns a dictionary
    with the key ``x`` and the value ``x + 1``. The step could be defined as follows:

    .. code-block:: python

        step = Step("add_one", add_one)

    The step could then be combined with other steps using the :func:`.chain` and
    :func:`.parallel` functions, or the ``>>`` and ``&`` operators (see the function
    documentation for more information).

    The step, or a larger flow, can then be run with a given input, using function
    :func:`.run`. For example:

    .. code-block:: python

        result = run(step, input=dict(x=1))
    """

    #: The function that this step applies to the source product.
    _function: Callable[
        ...,
        Mapping[str, Any]
        | Iterable[Mapping[str, Any]]
        | AsyncIterable[Mapping[str, Any]]
        | Awaitable[Mapping[str, Any]]
        | Awaitable[Iterable[Mapping[str, Any]]]
        | Awaitable[AsyncIterable[Mapping[str, Any]]],
    ]

    @subsdoc(
        # Find the line that defines the _name parameter and insert a new string in the
        # next line
        pattern=r"(:param _name:.*)\n",
        replacement=(
            r"\1\n"
            r":param _function: the function that the step applies to the source "
            r"product, and returns a single dictionary, or a synchronous or "
            r"asynchronous iterable of dictionaries\n"
        ),
        using=FunctionalConduit.__init__,
    )
    def __init__(
        self,
        _name: str,
        _function: Callable[
            ...,
            Mapping[str, Any]
            | Iterable[Mapping[str, Any]]
            | AsyncIterable[Mapping[str, Any]]
            | Awaitable[Mapping[str, Any]]
            | Awaitable[Iterable[Mapping[str, Any]]]
            | Awaitable[AsyncIterable[Mapping[str, Any]]],
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
            step=_name, function=_function, kwargs=kwargs, returns_iterable=True
        )

    @property
    def function(self) -> Callable[
        ...,
        Mapping[str, Any]
        | Iterable[Mapping[str, Any]]
        | AsyncIterable[Mapping[str, Any]]
        | Awaitable[Mapping[str, Any]]
        | Awaitable[Iterable[Mapping[str, Any]]]
        | Awaitable[AsyncIterable[Mapping[str, Any]]],
    ]:
        """
        The function that this step applies to the source product.
        """
        return self._function

    async def atransform(
        self, source_product: DictProduct
    ) -> AsyncIterator[DictProduct]:
        """
        Apply the function of this step to the dictionary managed by the source product.

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
        attribute_iterable = self._function(**input_args)

        if isinstance(attribute_iterable, Awaitable):
            attribute_iterable = await attribute_iterable

        if isinstance(attribute_iterable, Mapping):
            attribute_iterable = iter_sync_to_async([attribute_iterable])
        elif isinstance(attribute_iterable, Iterable):
            attribute_iterable = iter_sync_to_async(
                cast(Iterable[Mapping[str, Any]], attribute_iterable)
            )
        elif not isinstance(attribute_iterable, AsyncIterable):
            raise TypeError(
                f"Function {self._function.__name__}() of step {self.name!r} must "
                f"return a dictionary, an iterable of dictionaries, or an async "
                f"iterable of dictionaries, but got {type(attribute_iterable)}"
            )

        async for attributes in attribute_iterable:
            # Measure the end time of the step.
            end_time = time.perf_counter()

            if not isinstance(attributes, Mapping):
                raise TypeError(
                    f"Expected function {self._function.__name__}() of step "
                    f"{self.name!r} to return one or more instances of Mapping or "
                    f"dict, but got: {attributes!r}"
                )

            log.debug(
                "Completed step %r in %g seconds:\n%s",
                self.name,
                end_time - start_time,
                BinaryOperation(
                    BinaryOperator.ASSIGN,
                    Id(self._function)(**input_args),
                    DictLiteral(**attributes),
                ),
            )

            yield DictProduct(
                name=self.name,
                product_attributes=attributes,
                precursor=source_product,
                start_time=start_time,
                end_time=end_time,
            )

            # Set the start time of the next iteration to the end time of this
            # iteration.
            start_time = end_time


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
