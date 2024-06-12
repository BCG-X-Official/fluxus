"""
Implementation of ``MyClass``.
"""

from __future__ import annotations

import inspect
import logging
import time
import typing
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
)
from typing import Any, cast

from pytools.api import inheritdoc
from pytools.asyncio import iter_sync_to_async
from pytools.expression.atomic import Id
from pytools.expression.composite import BinaryOperation, DictLiteral
from pytools.expression.operator import BinaryOperator
from pytools.typing import issubclass_generic

from ... import AsyncTransformer
from ..product import DictProduct
from ._conduit import DictConduit

log = logging.getLogger(__name__)

__all__ = [
    "Step",
]

#
# Constants
#

# A sentinel value that indicates that a required argument was not provided.
_NOT_PROVIDED = object()

# A sentinel value that indicates that an optional argument was not provided.
_NOT_PROVIDED_OPTIONAL = object()

_ARG_REQUIRED = True
_ARG_OPTIONAL = False

#
# Step classes
#


@inheritdoc(match="[see superclass]")
class Step(DictConduit, AsyncTransformer[DictProduct, DictProduct]):
    """
    A step in a flow that applies a function to a dictionary.

    The ingoing dictionary is an attribute-value mapping. For each attribute that
    matches an argument name of the function, the attribute value is passed to the
    function. The function's attributes are therefore not allowed to be positional-only.

    Dictionary attributes that are not matched to an argument name of the function are
    ignored.

    If the function allows arbitrary keyword arguments using `**` notation, all
    attributes of the source product are passed to the function.

    The `Step` object may define additional fixed keyword arguments that are passed to
    the function on each call.

    The function must return either

    - a single attribute-value mapping or dictionary
    - an iterable of such dictionaries
    - an async iterable of such dictionaries

    The name of the step and the names of the keyword arguments must be valid Python
    identifiers.

    As an example, the following function could be used as a step:

    .. code-block:: python

        def add_one(x):
            return dict(x=x + 1)

    The function `add_one` takes a single argument `x` and returns a dictionary with the
    key `x` and the value `x + 1`. The step could be defined as follows:

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
        | Awaitable[Mapping[str, Any]],
    ]

    #: Additional keyword arguments to pass to the function.
    kwargs: dict[str, Any]

    #: The names of the arguments that must or can be determined from the source
    # product.
    # Maps argument names to whether they are required (_ARG_REQUIRED) or optional
    # (_ARG_OPTIONAL).
    # None if arbitrary keyword arguments are allowed.
    _function_arguments: Mapping[str, bool] | None

    def __init__(
        self,
        _name: str,
        _function: Callable[
            ...,
            Mapping[str, Any]
            | Iterable[Mapping[str, Any]]
            | AsyncIterable[Mapping[str, Any]]
            | Awaitable[Mapping[str, Any]],
        ],
        /,
        **kwargs: Any,
    ) -> None:
        """
        :param _name: the name of the step
        :param _function: the function that the step applies to the source product, and
            returns a single dictionary, or a synchronous or asynchronous iterable of
            dictionaries
        :param kwargs: additional keyword arguments to pass to the function
        :raises TypeError: if the signature of the function is invalid
        :raises ValueError: if the name of the step or the names of the keyword
            arguments are not valid identifiers
        """
        super().__init__(name=_name)

        invalid_kwargs = [
            key for key in kwargs if not (isinstance(key, str) and key.isidentifier())
        ]
        if invalid_kwargs:
            raise ValueError(
                "Names of keyword arguments must be valid identifiers, but got: "
                + ", ".join(map(repr, invalid_kwargs))
            )

        function_arguments = _validate_function(
            step=_name, function=_function, kwargs=kwargs
        )
        self._name = _name
        self._function = _function
        self.kwargs = kwargs
        self._function_arguments = function_arguments

    def get_repr_attributes(self) -> Mapping[str, Any]:
        """[see superclass]"""
        return {"name": self.name, **self.kwargs}

    @property
    def function(self) -> Callable[
        ...,
        Mapping[str, Any]
        | Iterable[Mapping[str, Any]]
        | AsyncIterable[Mapping[str, Any]]
        | Awaitable[Mapping[str, Any]],
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

        kwargs = self.kwargs

        # Get the source's product's attributes that need to be passed to the function
        # of this step.
        source_product_attributes = source_product.attributes

        # Warn if the fixed keyword arguments of the step shadow attributes of the
        # source.
        shadowed_attributes = source_product_attributes.keys() & kwargs.keys()
        if shadowed_attributes:
            logging.warning(
                f"Fixed keyword arguments of step {self.name!r} shadow attributes of "
                f"the source product: "
                + ", ".join(
                    f"{attr}={kwargs[attr]} shadows {attr}="
                    f"{source_product_attributes[attr]}"
                    for attr in sorted(shadowed_attributes)
                )
            )

        # Input arguments are the union of the source product attributes and the fixed
        # keyword arguments of the step; fixed keyword arguments take precedence over
        # source product attributes.
        input_args = {
            **await self._get_source_product_args(source_product_attributes),
            **kwargs,
        }

        # Measure the start time of the step. We are interested in CPU time, not wall
        # time, so we use time.perf_counter() instead of time.time().
        start_time = time.perf_counter()

        # Call the function of this step with the input arguments. This may return the
        # actual result, an iterable of results, or an async iterable of results.
        attribute_iterable = self._function(**input_args)

        if isinstance(attribute_iterable, Mapping):
            attribute_iterable = iter_sync_to_async([attribute_iterable])
        elif isinstance(attribute_iterable, Awaitable):
            attribute_iterable = _awaitable_to_async_iter(attribute_iterable)
        elif isinstance(attribute_iterable, Iterable):
            attribute_iterable = iter_sync_to_async(
                cast(Iterable[Mapping[str, Any]], attribute_iterable)
            )
        elif not isinstance(attribute_iterable, AsyncIterable):
            raise TypeError(
                f"Function {self._function.__name__}() of step {self.name!r} must "
                f"return a dictionary, an iterable of dictionaries, or an async "
                f"iterable of dictionaries, not {type(attribute_iterable)}"
            )

        async for attributes in attribute_iterable:
            # Measure the end time of the step.
            end_time = time.perf_counter()

            if not isinstance(attributes, Mapping):
                raise TypeError(
                    f"Expected function {self._function.__name__}() of step "
                    f"{self.name!r} to return a Mapping or dict, but got: "
                    f"{attributes!r}"
                )

            log.debug(
                f"Completed step {self.name!r} in {end_time - start_time:g} "
                f"seconds:\n"
                + str(
                    BinaryOperation(
                        BinaryOperator.ASSIGN,
                        Id(self._function)(**input_args),
                        DictLiteral(**attributes),
                    )
                )
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

    async def _get_source_product_args(
        self, source_product_attributes: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        # Get the arguments to pass to the function of this step from the source
        # product.

        function_argument_names: Mapping[str, bool] | None = self._function_arguments
        if function_argument_names is None:
            # The function accepts arbitrary keyword arguments, so we pass all
            # attributes of the source product to the function.
            source_product_args = source_product_attributes
        else:
            # We match the attributes of the source product to the free arguments of
            # the function.
            source_product_args = {
                name: source_product_attributes.get(
                    name,
                    (
                        _NOT_PROVIDED_OPTIONAL
                        if optional is _ARG_OPTIONAL
                        else _NOT_PROVIDED
                    ),
                )
                for name, optional in function_argument_names.items()
            }

            # Check if all free arguments could be matched
            unmatched_arguments = [
                name
                for name, value in source_product_args.items()
                if value is _NOT_PROVIDED
            ]
            if unmatched_arguments:
                raise ValueError(
                    f"Step {self.name!r} is missing input attributes: "
                    + ", ".join(unmatched_arguments)
                )

            # remove optional arguments that were not provided
            source_product_args = {
                name: value
                for name, value in source_product_args.items()
                if value is not _NOT_PROVIDED_OPTIONAL
            }
        return source_product_args


#
# Auxiliary functions
#


def _validate_function(
    *, step: str, function: Callable[..., Any], kwargs: Mapping[str, Any]
) -> Mapping[str, bool] | None:
    # Validate the N of the step function, if defined, and determine
    # whether the return type is an iterator or an async iterator.
    #
    # Returns the names of arguments that must or can be determined from the
    # source product, or None if arbitrary keyword arguments are allowed.

    # Get the function signature
    signature = inspect.signature(function)

    # The free arguments that need to be (or can be) determined from the source product.
    # Maps argument names to whether they are required (_ARG_REQUIRED) or optional
    # (_ARG_OPTIONAL).
    function_arguments: Mapping[str, bool] | None

    # Validate the parameters of the step function
    parameters: list[inspect.Parameter] = list(signature.parameters.values())
    if any(parameter.kind == parameter.POSITIONAL_ONLY for parameter in parameters):
        raise TypeError("Step function cannot have positional-only parameters.")

    if any(parameter.kind == parameter.VAR_KEYWORD for parameter in parameters):
        # We allow arbitrary keyword arguments, so we don't need to check for missing
        # named arguments.
        function_arguments = None
    else:
        # If the function does not accept arbitrary keyword arguments, we need to
        # ensure that there are named arguments for all fixed keyword arguments.
        missing_named_arguments = kwargs.keys() - (
            parameter.name for parameter in parameters
        )
        if missing_named_arguments:
            raise TypeError(
                f"Function {function.__name__} of step {step!r} is missing named "
                "arguments for fixed keyword arguments: "
                + ", ".join(missing_named_arguments)
            )

        function_arguments = {
            parameter.name: (
                _ARG_REQUIRED if parameter.default == parameter.empty else _ARG_OPTIONAL
            )
            for parameter in parameters
            # We exclude *args from the free arguments …
            if parameter.kind != parameter.VAR_POSITIONAL
            # … as well as the names of fixed keyword arguments
            and parameter.name not in kwargs
        }

    # Get the return type of the step. This is either a Mapping, an iterator, or an
    # async iterator. If the return type is not specified, we will still test the actual
    # return type once the function is called.
    return_annotation = signature.return_annotation
    if return_annotation != signature.empty:
        if isinstance(return_annotation, str):
            # The return type is a forward reference, so we need to resolve it to a
            # type object.
            return_annotation = typing.get_type_hints(function).get("return", None)
            if return_annotation is None:  # pragma: no cover
                # This should never happen
                raise TypeError(
                    f"Return type of function {function.__name__} of step {step!r} "
                    f"is a forward reference that cannot be resolved: "
                    f"{return_annotation!r}"
                )
        if not any(
            issubclass_generic(return_annotation, tp)
            for tp in (
                Mapping[str, Any],
                Iterable[Mapping[str, Any]],
                AsyncIterable[Mapping[str, Any]],
                Awaitable[Mapping[str, Any]],
            )
        ):
            raise TypeError(
                f"Return type of function {function.__name__} of step {step!r} must be "
                "one of Mapping[str, Any], Iterable[Mapping[str, Any]], or "
                f"AsyncIterable[Mapping[str, Any]], but got: {return_annotation}"
            )

    return function_arguments


async def _awaitable_to_async_iter(
    x: Awaitable[Mapping[str, Any]]
) -> AsyncIterator[Mapping[str, Any]]:
    """
    Convert an awaitable to an async iterator.

    :param x: the awaitable to convert
    :return: an async iterator that yields the result of the awaitable
    """
    yield await x
