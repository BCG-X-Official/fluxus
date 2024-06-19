"""
Implementation of public functions of the functional API for the flow module.
"""

from __future__ import annotations

import functools
import itertools
import logging
import operator
from collections.abc import (
    AsyncIterable,
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Iterator,
    Mapping,
)
from types import FunctionType
from typing import Any, TypeVar, cast, overload

from pytools.asyncio import arun

from .. import Passthrough
from ..core import Conduit
from ..core.producer import BaseProducer, ConcurrentProducer, SimpleConcurrentProducer
from ..core.transformer import (
    BaseTransformer,
    ConcurrentTransformer,
    SerialTransformer,
    SimpleConcurrentTransformer,
)
from ._result import RunResult
from .conduit import DictConsumer, DictProducer, Step
from .product import DictProduct

log = logging.getLogger(__name__)

__all__ = [
    "chain",
    "parallel",
    "passthrough",
    "run",
    "step",
]

#
# Type variables
#

T_Input = TypeVar("T_Input")
T_Product = TypeVar("T_Product")
T_Conduit = TypeVar("T_Conduit", bound=Conduit[Any])

#
# 'step' function, defining an individual step in the flow
#


@overload
def step(
    _name: str,
    _function: Callable[
        [],
        Mapping[str, Any]
        | Iterable[dict[str, Any]]
        | AsyncIterable[dict[str, Any]]
        | Awaitable[Mapping[str, Any]],
    ],
    /,
    **kwargs: Any,
) -> DictProducer:
    """[see below]"""


@overload
def step(  # type: ignore[misc]
    _name: str,
    # Technically, the function signature of _function is a superset of the previous
    # one, but in Python there is no way of specifying that we need at least one
    # arbitrary keyword argument.
    # It still works as intended, because the previous overload is more specific.
    # Still, for mypy we have to ignore the error; see the 'ignore' comment above.
    _function: Callable[
        ...,
        Mapping[str, Any]
        | Iterable[dict[str, Any]]
        | AsyncIterable[dict[str, Any]]
        | Awaitable[Mapping[str, Any]],
    ],
    /,
    **kwargs: Any,
) -> Step:
    """[see below]"""


@overload
def step(
    _name: str,
    _data: (
        Mapping[str, Any] | Iterable[Mapping[str, Any]] | AsyncIterable[dict[str, Any]]
    ),
    /,
) -> DictProducer:
    """[see below]"""


def step(
    _name: str,
    _function_or_data: (
        Callable[
            ...,
            Mapping[str, Any]
            | Iterable[dict[str, Any]]
            | AsyncIterable[dict[str, Any]]
            | Awaitable[Mapping[str, Any]],
        ]
        | Iterable[Mapping[str, Any]]
        | AsyncIterable[dict[str, Any]]
        | Mapping[str, Any]
    ),
    /,
    **kwargs: Any,
) -> DictProducer | Step:
    # noinspection GrazieInspection
    """
    Create a step in a flow.

    Takes two positional-only arguments, followed by an arbitrary number of fixed
    keyword arguments.

    The first positional argument is the name of the step, and the second positional
    argument is either a function, or a mapping or iterable of mappings.

    If the second argument is a mapping or an iterable of mappings, then the step is
    a `producer` step that produces the given data.

    If the second argument is a function, then that function must return a single
    mapping, or a synchronous or asynchronous iterable of mappings. If the function
    takes no arguments, then the step is a `producer` step that produces the data
    returned by the function. If the function takes one or more keyword arguments, then
    the step is a `transformer` step that applies the function to the input data and
    produces one or more dictionaries based on the input data.

    All arguments to the function must be keyword arguments. The values passed for these
    arguments are determined by the dictionary items produced by the preceding steps in
    the flow that match the names of the arguments, plus any fixed keyword arguments
    associated with any of the preceding steps or the current step. If multiple
    preceding steps use the same names for fixed keyword arguments or for dictionary
    items produced by their functions, then the value of the latest step in the flow
    takes precedence for that attribute.

    Code examples:

    .. code-block:: python

        # Create a producer step that produces a single dictionary
        producer = step(
            "input",
            dict(a=1, b=2, c=3),
        )

        # Create a producer step that produces multiple dictionaries
        producer = step(
            "input",
            [
                dict(a=1, b=2, c=3),
                dict(a=4, b=5, c=6),
            ],
        )

        # Create a producer step that dynamically produces dictionaries
        producer = step(
            "input",
            lambda: (
                dict(x=i) for i in range(3)
            )
        )

        # Create a transformer step that applies a function to the input
        transform_step = step(
            "transform_data",
            lambda number, increment: dict(incremented_number=number + increment),
            increment=1
        )

        # Create a transformer step that produces multiple dictionaries for each input
        transform_step = step(
            "transform_data",
            lambda number, increment: (
                dict(incremented_number=number + i) for i in range(max_increment)
            ),
            max_increment=1
        )


    :param _name: the name of the step
    :param _function_or_data: the function that the step applies to the source product,
        and returns a single mapping, or a synchronous or asynchronous iterable of
        mappings
    :param kwargs: additional keyword arguments to pass to the function
    :return: the step object
    :raises TypeError: if the signature of the function is invalid, or if additional
        keyword arguments are provided while the function does not accept any
    """

    # Count the number of arguments that the function accepts

    if isinstance(_function_or_data, Mapping):
        return DictProducer(lambda: _function_or_data, name=_name)
    elif isinstance(_function_or_data, Iterable):
        return DictProducer(
            lambda: iter(cast(Iterable[Mapping[str, Any]], _function_or_data)),
            name=_name,
        )
    elif isinstance(_function_or_data, AsyncIterable):
        return DictProducer(
            lambda: aiter(cast(AsyncIterable[Mapping[str, Any]], _function_or_data)),
            name=_name,
        )
    elif (
        not isinstance(_function_or_data, FunctionType)
        or _function_or_data.__code__.co_varnames
    ):
        return Step(_name, _function_or_data, **kwargs)
    else:
        return DictProducer(_function_or_data, name=_name)


#
# 'passthrough' function, defining a passthrough step in the flow
#


def passthrough() -> Passthrough:
    # noinspection GrazieInspection
    """
    Create a `passthrough` step in a flow.

    A `passthrough` is a special step that passes the input through without
    modification. It is invisible in the flow, and does not affect the data in any way.

    Use a passthrough as part of a :func:`parallel` composition of steps to ensure that,
    in addition to applying all parallel steps concurrently, the input is also passed
    without modification to the subsequent steps.

    Chaining a passthrough with another step is not permitted; it would have no effect.

    Examples:

    .. code-block:: python

        # Create a parallel composition of steps
        parallel_steps = chain(
            # Create a producer step that produces a single dictionary
            step(
                "input",
                dict(a=1, b=2, c=3),
            ),
            # Create a group of parallel steps, including a passthrough
            parallel(
                # Create a transformer step that increments the value of 'a'
                step(
                    "increment_a",
                    lambda a: dict(a=a + 1),
                ),
                # Create a passthrough step that passes the input through
                passthrough(),
            ),
            # Create a transformer step that doubles the value of 'a'
            step(
                "double_a",
                lambda a: dict(a=a * 2),
            ),
        )

        # Run the parallel composition of steps
        result = run(parallel_steps)

    The results will include the original input dictionary, as well as the dictionary
    produced by the 'increment_a' step:

    .. code-block:: python

        [
            # First output, including the original input and the result of the
            # 'increment_a' and 'double_a' steps
            {
                "input": dict(a=1, b=2, c=3),
                "increment_a": dict(a=2),
                "double_a": dict(a=4),
            },
            # Second output, including the original input, bypassing the 'increment_a'
            # step, including the result of the 'double_a' step
            {
                "input": dict(a=1, b=2, c=3),
                "double_a": dict(a=2),
            },
        ]

    :return: the passthrough step
    """
    return Passthrough()


#
# 'chain' function, defining a sequence of steps in the flow
#


@overload
def chain(
    start: BaseProducer[T_Product],
    /,
    *steps: BaseTransformer[T_Product, T_Product],
) -> BaseProducer[T_Product]:
    """[see below]"""


@overload
def chain(
    start: BaseTransformer[T_Product, T_Product],
    /,
    *steps: BaseTransformer[T_Product, T_Product],
) -> BaseTransformer[T_Product, T_Product]:
    """[see below]"""


def chain(
    start: BaseProducer[T_Product] | BaseTransformer[T_Product, T_Product],
    /,
    *steps: BaseTransformer[T_Product, T_Product],
) -> BaseProducer[T_Product] | BaseTransformer[T_Product, T_Product]:
    """
    Combine multiple transformers with an optional leading producer to be executed
    sequentially.

    Combining a producer with one or more transformers results in a chained producer.
    Combining one or more transformers results in a chained transformer.

    Examples:

    .. code-block:: python

        chained_steps = chain(
            # Start with an initial producer that generates the initial data
            step("initial_producer", lambda: dict(value=1)),
            # Apply a transformer that increments the value
            step("increment_transformer", lambda data: dict(value=data["value"] + 1)),
            # Apply a transformer that doubles the value
            step("double_transformer", lambda data: dict(value=data["value"] * 2)),
            # Apply a transformer that squares the value
            step("square_transformer", lambda data: dict(value=data["value"] ** 2))
        )

        chained_steps = chain(
            [
                # Create three chained steps that increment the value by 1, 2, and 3
                step("increment_transformer", lambda data,
                i=i: dict(value=data["value"] + i)) for i in range(1, 4)
            ],
            [
                # Create three chained steps that multiply the value by 1, 2, and 3
                step("multiply_transformer", lambda data,
                i=i: dict(value=data["value"] * i)) for i in range(1, 4)
            ],
            # Include a passthrough step that passes the input through unchanged
            passthrough()
        )

    :param start: the initial producer or transformer in the chain
    :param steps: additional transformers in the chain
    :return: the chained producer or transformer
    """
    return functools.reduce(operator.rshift, (start, *steps))


#
# 'parallel' function, defining a concurrent execution of steps in the flow
#

SimpleIterable = Collection[T_Conduit] | Iterator[T_Conduit]


@overload
def parallel(
    first: BaseProducer[T_Product] | SimpleIterable[BaseProducer[T_Product]],
    second: BaseProducer[T_Product] | SimpleIterable[BaseProducer[T_Product]],
    /,
    *more: BaseProducer[T_Product] | SimpleIterable[BaseProducer[T_Product]],
) -> ConcurrentProducer[T_Product]:
    """[see below]"""


@overload
def parallel(
    inputs: SimpleIterable[BaseProducer[T_Product]],
    /,
) -> ConcurrentProducer[T_Product] | BaseProducer[T_Product]:
    """[see below]"""


@overload
def parallel(
    first: (
        BaseTransformer[T_Input, T_Product]
        | SimpleIterable[BaseTransformer[T_Input, T_Product] | Passthrough]
        | Passthrough
    ),
    second: (
        BaseTransformer[T_Input, T_Product]
        | SimpleIterable[BaseTransformer[T_Input, T_Product] | Passthrough]
        | Passthrough
    ),
    /,
    *more: (
        BaseTransformer[T_Input, T_Product]
        | SimpleIterable[BaseTransformer[T_Input, T_Product] | Passthrough]
        | Passthrough
    ),
) -> ConcurrentTransformer[T_Input, T_Product]:
    """[see below]"""


@overload
def parallel(
    steps: SimpleIterable[BaseTransformer[T_Input, T_Product] | Passthrough],
    /,
) -> ConcurrentTransformer[T_Input, T_Product] | BaseTransformer[T_Input, T_Product]:
    """[see below]"""


def parallel(
    first: (
        BaseProducer[T_Product]
        | SimpleIterable[BaseProducer[T_Product]]
        | BaseTransformer[T_Input, T_Product]
        | SimpleIterable[BaseTransformer[T_Input, T_Product] | Passthrough]
        | Passthrough
    ),
    /,
    *more: BaseProducer[T_Product]
    | SimpleIterable[BaseProducer[T_Product]]
    | BaseTransformer[T_Input, T_Product]
    | SimpleIterable[BaseTransformer[T_Input, T_Product] | Passthrough]
    | Passthrough,
) -> (
    ConcurrentProducer[T_Product]
    | ConcurrentTransformer[T_Input, T_Product]
    | BaseProducer[T_Product]
    | BaseTransformer[T_Input, T_Product]
):
    # noinspection GrazieInspection
    """
    Combine two or more sub-flows, to be executed concurrently.

    All sub-flows must have compatible input and output types.

    If all arguments are producers, then the result is a `concurrent producer`.

    If all arguments are transformers, with or without an additional passthrough step,
    then the result is a `concurrent transformer`. At most one passthrough step is
    allowed in a group of parallel steps, since additional passthroughs would result in
    duplicate output.

    Mixing producers and transformers is not allowed.

    Arguments can be provided as individual steps or sub-flows, or as iterables of steps
    or sub-flows. If an iterable is provided, its elements are unpacked and combined
    with the other arguments.

    Examples:

    .. code-block:: python

        concurrent_steps = parallel(
            # Create two concurrent steps that increment 'a' and 'b' by 1
            step("increment_a", lambda a: dict(a=a + 1)),
            step("increment_b", lambda b: dict(b=b + 1)),
        )

        concurrent_steps = parallel(
            [
                # Create three concurrent steps that increment 'a' by 1, 2, and 3
                step("increment_a", lambda a: dict(a=a + i)) for i in range(1, 4)
            ],
            [
                # Create three concurrent steps that increment 'b' by 1, 2, and 3
                step("increment_b", lambda b: dict(b=b + i)) for i in range(1, 4)
            ],
            # Include a passthrough step that passes the input through unchanged
            passthrough(),
        )

    :param first: the first step to combine
    :param more: additional steps to combine
    :return: the combined steps for concurrent execution
    :raises TypeError: if no steps are provided
    """

    # Unpack iterable arguments
    steps = cast(
        list[
            BaseProducer[T_Product] | BaseTransformer[T_Input, T_Product] | Passthrough
        ],
        list(
            itertools.chain.from_iterable(
                (
                    [step_]
                    # Check if the first argument is an iterable of steps.
                    # Producers are iterable, so we need to exclude them here.
                    if isinstance(step_, (BaseProducer, BaseTransformer, Passthrough))
                    or not isinstance(step_, Iterable)
                    else step_
                )
                for step_ in (first, *more)
            )
        ),
    )

    # Check for invalid step types
    invalid_steps = [
        step_
        for step_ in steps
        if not isinstance(step_, (BaseProducer, BaseTransformer, Passthrough))
    ]
    if invalid_steps:
        raise TypeError(
            "parallel() can only be used with producer, transformer, or passthrough "
            "steps, but got: " + ", ".join(str(step_) for step_ in invalid_steps)
        )

    n_passthrough = sum(1 for step_ in steps if isinstance(step_, Passthrough))
    if n_passthrough > 1:
        raise TypeError(
            f"parallel() may include at most one passthrough step, but got "
            f"{n_passthrough}"
        )

    elif not steps:
        raise TypeError("parallel() must be called with at least one step")

    elif len(steps) == 1:
        single_step = steps[0]
        # We have a single step, so don't need to combine anything
        if isinstance(single_step, Passthrough):
            # We don't allow a single Passthrough step, because that might lead to
            # chaining a Passthrough with another step, which is not allowed.
            raise TypeError("parallel() cannot be used with a single passthrough step")
        else:
            return single_step

    elif isinstance(steps[0], BaseProducer):
        # We have multiple producers. Combine them into a single concurrent producer.
        return SimpleConcurrentProducer(*cast(list[BaseProducer[T_Product]], steps))

    else:
        # We have multiple transformers. Combine them into a single concurrent
        # transformer.
        return SimpleConcurrentTransformer(
            *cast(list[BaseTransformer[T_Input, T_Product] | Passthrough], steps)
        )


#
# 'run' function, running the steps in the flow
#


@overload
def run(steps: BaseProducer[DictProduct], *, timestamps: bool = False) -> RunResult:
    """[see below]"""


# noinspection PyShadowingNames
@overload
def run(
    steps: SerialTransformer[DictProduct, DictProduct],
    *,
    input: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    timestamps: bool = False,
) -> RunResult:
    """[see below]"""


@overload
def run(
    steps: ConcurrentTransformer[DictProduct, DictProduct],
    *,
    input: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    timestamps: bool = False,
) -> RunResult:
    """[see below]"""


@overload
def run(
    steps: BaseTransformer[DictProduct, DictProduct],
    *,
    input: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    timestamps: bool = False,
) -> RunResult:
    """[see below]"""


# noinspection PyShadowingNames, PyTestUnpassedFixture
def run(
    steps: BaseProducer[DictProduct] | BaseTransformer[DictProduct, DictProduct],
    *,
    input: Iterable[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    timestamps: bool = False,
) -> RunResult:
    """
    Run the given steps.

    If the given steps do not include a leading `producer`` step (i.e. a step that takes
    no parameters), then the input can be provided as an additional argument.
    If the flow requires an input but none is provided, then the flow will be run with
    an empty dictionary as input.

    See :class:`.RunResult` for details on the output format.

    Examples:

    .. code-block:: python

        # Example 1: A sequence of steps
        chained_steps = chain(
            # Start with an initial input producer
            step("initial_producer", lambda: dict(value=1)),
            # Increment the value
            step("increment_transformer", lambda value: dict(value=value + 1)),
            # Double the value
            step("double_transformer", lambda value: dict(value=value * 2)),
            # Square the value
            step("square_transformer", lambda value: dict(value=value ** 2))
        )

        # Run the steps
        result = run(chained_steps)

    generates the following output:

    .. code-block:: python

        RunResult(
            [
                {
                    'initial_producer': {'value': 1},
                    'increment_transformer': {'value': 2},
                    'double_transformer': {'value': 4},
                    'square_transformer': {'value': 16}
                }
            ]
        )

    and

    .. code-block:: python

        # Example 2: Parallel steps
        parallel_steps = parallel(
            # Create two concurrent steps that increment 'a' and 'b' by 1
            step("increment_a", lambda a: dict(a=a + 1)),
            step("increment_b", lambda b: [dict(b=b + 1), dict(b=b + 2)]),
        )

        # Run the steps with input
        result = run(parallel_steps, input={"a": 1, "b": 2})

    generates the following output:

    .. code-block:: python

        RunResult(
            [{'input': {'a': 1, 'b': 2}, 'increment_a': {'a': 2}}],
            [
                {'input': {'a': 1, 'b': 2}, 'increment_b': {'b': 3}},
                {'input': {'a': 1, 'b': 2}, 'increment_b': {'b': 4}}
            ]
        )

    Parallel and serial steps can be combined in a single flow:

    .. code-block:: python

        # Example 3: Running complex parallel steps
        complex_parallel_steps = parallel(
            [
                # Create three concurrent steps that increment 'a' by 1, 2, and 3
                step("increment_a", lambda a, i=i: dict(a=a+i)) for i in range(1, 4)
            ],
            [
                # Create three concurrent steps that increment 'b' by 1, 2, and 3
                step("increment_b", lambda b, i=i: dict(b=b+i)) for i in range(1, 4)
            ],
            # Include a passthrough step that passes the input through unchanged
            passthrough()
        )

        # Run the steps with input
        result = run(complex_parallel_steps, input=dict(a=1, b=2))

    generates the following output:

    .. code-block:: python

        RunResult(
            [{'input': {'a': 1, 'b': 2}, 'increment_a': {'a': 2}}],
            [{'input': {'a': 1, 'b': 2}, 'increment_a': {'a': 3}}],
            [{'input': {'a': 1, 'b': 2}, 'increment_a': {'a': 4}}],
            [{'input': {'a': 1, 'b': 2}, 'increment_b': {'b': 3}}],
            [{'input': {'a': 1, 'b': 2}, 'increment_b': {'b': 4}}],
            [{'input': {'a': 1, 'b': 2}, 'increment_b': {'b': 5}}],
            [{'input': {'a': 1, 'b': 2}}]
        )

    :param steps: the steps to run
    :param input: one or more inputs to the steps (optional)
    :param timestamps: if ``True``, include start and end timestamps for each step in
        the output; if ``False``, do not include timestamps
    :return: the dictionaries produced by the final step or steps
    """

    consumer = DictConsumer(timestamps=timestamps)

    if isinstance(steps, BaseProducer):
        if input is not None:
            raise TypeError(
                "arg input cannot be specified when the given steps include a leading "
                "producer step"
            )
        return arun((steps >> consumer).arun())

    elif isinstance(steps, BaseTransformer):
        return arun(
            (
                DictProducer(lambda: {} if input is None else input, name="input")
                >> steps
                >> consumer
            ).arun()
        )

    else:
        message = (
            f"arg steps must be a step or composition of steps, but got a "
            f"{type(steps).__qualname__}"
        )
        if (
            isinstance(steps, tuple)
            and len(steps) == 1
            and isinstance(steps[0], Conduit)
        ):
            message += "; did you leave a comma before a closing parenthesis?"
        raise TypeError(message)
