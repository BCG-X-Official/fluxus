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

import inspect
import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Mapping
from types import UnionType
from typing import Any, final, get_type_hints

from pytools.api import inheritdoc
from pytools.typing import issubclass_generic

from ...core import AtomicConduit
from ..product import DictProduct

log = logging.getLogger(__name__)

__all__ = [
    "DictConduit",
    "FunctionalConduit",
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
# Classes
#


@inheritdoc(match="[see superclass]")
class DictConduit(AtomicConduit[DictProduct], metaclass=ABCMeta):
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


@inheritdoc(match="[see superclass]")
class FunctionalConduit(DictConduit, metaclass=ABCMeta):
    """
    Base class for conduits that apply a function to a dictionary.

    The ingoing dictionary is an attribute-value mapping. For each attribute that
    matches an argument name of the function, the attribute value is passed to the
    function. The function's attributes are therefore not allowed to be positional-only.

    Dictionary attributes that are not matched to an argument name of the function are
    ignored.

    If the function allows arbitrary keyword arguments using ``**`` notation, all
    attributes of the source product are passed to the function.

    The conduit may define additional fixed keyword arguments that are passed to
    the function on each call.

    The function must return either

    - a single attribute-value mapping or dictionary
    - an iterable of such dictionaries
    - an async iterable of such dictionaries

    The name of the conduit and the names of the keyword arguments must be valid Python
    identifiers.

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

    #: Additional keyword arguments to pass to the function.
    kwargs: dict[str, Any]

    #: The names of the arguments that must or can be determined from the source
    # product.
    # Maps argument names to whether they are required (_ARG_REQUIRED) or optional
    # (_ARG_OPTIONAL).
    # None if arbitrary keyword arguments are allowed.
    _function_arguments: Mapping[str, bool] | None

    def __init__(self, _name: str, /, **kwargs: Any) -> None:
        """
        :param _name: the name of the step
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

        self._name = _name
        self.kwargs = kwargs
        self._function_arguments = None

    def get_repr_attributes(self) -> Mapping[str, Any]:
        """[see superclass]"""
        return {"name": self.name, **self.kwargs}

    @property
    @abstractmethod
    def function(self) -> Callable[..., Any]:
        """
        The function that this conduit applies to the source product.
        """

    def _get_input_args(self, source_product: DictProduct) -> Mapping[str, Any]:
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
        return {
            **self._get_source_product_args(source_product_attributes),
            **kwargs,
        }

    def _get_source_product_args(
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

    @staticmethod
    def _validate_function(
        *,
        step: str,
        function: Callable[..., Any],
        kwargs: Mapping[str, Any],
        returns_iterable: bool,
    ) -> Mapping[str, bool] | None:
        # Validate the N of the step function, if defined, and determine
        # whether the return type is an iterator or an async iterator.
        #
        # Returns the names of arguments that must or can be determined from the
        # source product, or None if arbitrary keyword arguments are allowed.

        # Get the function signature
        signature = inspect.signature(function)

        # The free arguments that need to be (or can be) determined from the source
        # product.
        # Maps argument names to whether they are required (_ARG_REQUIRED) or optional
        # (_ARG_OPTIONAL).
        function_arguments: Mapping[str, bool] | None

        # Validate the parameters of the step function
        parameters: list[inspect.Parameter] = list(signature.parameters.values())
        if any(parameter.kind == parameter.POSITIONAL_ONLY for parameter in parameters):
            raise TypeError("Step function cannot have positional-only parameters.")

        if any(parameter.kind == parameter.VAR_KEYWORD for parameter in parameters):
            # We allow arbitrary keyword arguments, so we don't need to check for
            # missing named arguments.
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
                    _ARG_REQUIRED
                    if parameter.default == parameter.empty
                    else _ARG_OPTIONAL
                )
                for parameter in parameters
                # We exclude *args from the free arguments …
                if parameter.kind != parameter.VAR_POSITIONAL
                # … as well as the names of fixed keyword arguments
                and parameter.name not in kwargs
            }

        # Get the return type of the step. This is either a Mapping, an iterator, or an
        # async iterator. If the return type is not specified, we will still test the
        # actual return type once the function is called.
        return_annotation = signature.return_annotation
        if return_annotation != signature.empty:
            if isinstance(return_annotation, str):
                # The return type is a forward reference, so we need to resolve it to a
                # type object.
                return_annotation = get_type_hints(function).get("return", None)
                if return_annotation is None:  # pragma: no cover
                    # This should never happen
                    raise TypeError(
                        f"Return type of function {function.__name__} of step {step!r} "
                        f"is a forward reference that cannot be resolved: "
                        f"{return_annotation!r}"
                    )
            acceptable_return_types: tuple[type | UnionType, ...]
            if returns_iterable:
                acceptable_return_types = (
                    Mapping[str, Any],
                    Awaitable[Mapping[str, Any]],
                    Iterable[Mapping[str, Any]],
                    AsyncIterable[Mapping[str, Any]],
                )
                types_str = (
                    "one of Mapping[str, Any], Iterable[Mapping[str, Any]] or "
                    "AsyncIterable[Mapping[str, Any]]"
                )
            else:
                acceptable_return_types = (
                    Mapping[str, Any] | None,
                    Awaitable[Mapping[str, Any] | None],
                )
                types_str = "Mapping[str, Any] | None"

            if not any(
                issubclass_generic(return_annotation, tp)
                for tp in acceptable_return_types
            ):
                raise TypeError(
                    f"Return type of function {function.__name__} of step {step!r} "
                    f"must be {types_str}, but got: {return_annotation}"
                )

        return function_arguments
