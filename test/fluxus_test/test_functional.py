"""
Unit tests for the functional API
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from io import StringIO
from typing import Any

import pandas as pd
import pytest

from fluxus.functional import RunResult, chain, parallel, passthrough, run, step
from fluxus.functional.product import DictProduct
from pytools.asyncio import iter_async_to_sync, iter_sync_to_async

log = logging.getLogger(__name__)


def test_input() -> None:
    """
    Test the 'input' function.
    """

    # Test with a single dictionary
    producer = step(
        "input",
        dict(a=1, b=2, c=3),
    )
    assert list(producer) == [
        dict(a=1, b=2, c=3),
    ]

    # Test with multiple dictionaries
    producer = step(
        "input",
        [
            dict(a=1, b=2, c=3),
            dict(a=4, b=5, c=6),
        ],
    )
    assert list(producer) == [
        dict(a=1, b=2, c=3),
        dict(a=4, b=5, c=6),
    ]


def test_step() -> None:
    """
    Test the 'step' function.
    """

    # Test with a synchronous function
    def _function(a: int, b: int, fixed: int) -> dict[str, int]:
        return dict(ab=a * b + fixed)

    my_step = step(
        "step",
        _function,
        fixed=0,
    )
    assert my_step.function == _function
    assert my_step.get_repr_attributes() == dict(name="step", fixed=0)
    assert (
        repr(my_step) == "Step(_name='step', _function=_function, kwargs={'fixed': 0})"
    )

    assert run(
        my_step,
        input=[dict(a=2, b=3, c=1)],
    ) == RunResult(
        [
            dict(
                input=dict(a=2, b=3, c=1),
                step=dict(ab=6),
            ),
        ]
    )

    # Test with an asynchronous function
    async def _function_async(a: int, b: int, c: int) -> AsyncIterator[dict[str, int]]:
        yield dict(ab=a * b)
        yield dict(ac=a * c)

    assert _sort_nested(
        run(
            step("step", _function_async),
            input=[dict(a=2, b=3, c=1)],
        )
    ) == [
        [
            dict(input=dict(a=2, b=3, c=1), step=dict(ab=6)),
            dict(input=dict(a=2, b=3, c=1), step=dict(ac=2)),
        ]
    ]

    with pytest.raises(
        ValueError, match=r"^Step 'step' is missing input attributes: c$"
    ):
        run(
            step("step", _function_async),
            input=[dict(a=2, b=3)],
        )

    with pytest.raises(
        TypeError,
        match=(
            "^Function _function_async of step 'step' is missing named arguments for "
            "fixed keyword arguments: unexpected$"
        ),
    ):
        step("step", _function_async, unexpected=1)


def test_flow() -> None:
    """
    Test a complete flow, comprising an input and multiple concurrent and chained steps.

    Input is integers a and b, and steps carry out various simple calculations.
    """

    # The input data
    input_data = [
        dict(a=2, b=3),
        dict(a=4, b=5),
    ]

    # The step that produces the input data
    input_step = step(
        "input",
        input_data,
    )

    # The steps that process the input data
    # noinspection PyTypeChecker
    steps = chain(
        parallel(
            step(
                "multiply",
                lambda a, b: dict(ab=a * b),
            ),
            step(
                "add",
                lambda a, b: dict(a_plus_b=a + b),
            ),
            step(
                "subtract",
                lambda a, b: dict(a_minus_b=a - b),
            ),
        ),
        step(
            "insert",
            lambda new: dict(new=new),
            new=42,
        ),
    )

    # Check the result
    result_expected = [
        [
            dict(input=dict(a=2, b=3), add=dict(a_plus_b=5), insert=dict(new=42)),
            dict(input=dict(a=4, b=5), add=dict(a_plus_b=9), insert=dict(new=42)),
        ],
        [
            dict(input=dict(a=2, b=3), multiply=dict(ab=6), insert=dict(new=42)),
            dict(input=dict(a=4, b=5), multiply=dict(ab=20), insert=dict(new=42)),
        ],
        [
            dict(
                input=dict(a=2, b=3), subtract=dict(a_minus_b=-1), insert=dict(new=42)
            ),
            dict(
                input=dict(a=4, b=5), subtract=dict(a_minus_b=-1), insert=dict(new=42)
            ),
        ],
    ]

    # run once with input as part of the chain
    assert _sort_nested(run(chain(input_step, steps))) == result_expected

    # run again with input as a separate argument
    assert _sort_nested(run(steps, input=input_data)) == result_expected

    # construct the same flow, with parallel steps as an iterable
    steps = chain(
        parallel(
            iter(
                [
                    step(
                        "multiply",
                        lambda a, b: dict(ab=a * b),
                    ),
                    step(
                        "add",
                        lambda a, b: dict(a_plus_b=a + b),
                    ),
                    step(
                        "subtract",
                        lambda a, b: dict(a_minus_b=a - b),
                    ),
                ]
            )
        ),
        step(
            "insert",
            lambda new: dict(new=new),
            new=42,
        ),
    )

    # run again with input as a separate argument
    assert _sort_nested(run(steps, input=input_data)) == result_expected


def test_parallel_inputs() -> None:
    """
    Test a complete flow with multiple concurrent inputs.
    """

    # noinspection PyTypeChecker
    steps = (
        parallel(
            step(
                "input1",
                # A synchronous iterable as input
                [dict(a=2, b=3), dict(a=4, b=5)],
            )
            >> (
                step("multiply", lambda a, b: dict(ab=a * b))
                & step("add", lambda a, b: dict(a_plus_b=a + b))
            ),
            step(
                "input2",
                # An asynchronous iterable as input
                iter_sync_to_async([dict(a=3, b=4), dict(a=5, b=6)]),
            )
            >> step("subtract", lambda a, b: dict(a_minus_b=a - b)),
        )
    ) >> step("inc", lambda a, b: dict(inc=a + 1, a=a, b=b, a_orig=a))

    result = run(steps)

    # Check the result
    assert _sort_nested(result) == [
        [
            dict(
                input1=dict(a=2, b=3),
                add=dict(a_plus_b=5),
                inc=dict(inc=3, a=2, b=3, a_orig=2),
            ),
            dict(
                input1=dict(a=4, b=5),
                add=dict(a_plus_b=9),
                inc=dict(inc=5, a=4, b=5, a_orig=4),
            ),
        ],
        [
            dict(
                input1=dict(a=2, b=3),
                multiply=dict(ab=6),
                inc=dict(inc=3, a=2, b=3, a_orig=2),
            ),
            dict(
                input1=dict(a=4, b=5),
                multiply=dict(ab=20),
                inc=dict(inc=5, a=4, b=5, a_orig=4),
            ),
        ],
        [
            dict(
                input2=dict(a=3, b=4),
                subtract=dict(a_minus_b=-1),
                inc=dict(inc=4, a=3, b=4, a_orig=3),
            ),
            dict(
                input2=dict(a=5, b=6),
                subtract=dict(a_minus_b=-1),
                inc=dict(inc=6, a=5, b=6, a_orig=5),
            ),
        ],
    ]

    with pytest.raises(
        TypeError,
        match=r"^parallel\(\) missing 1 required positional argument: 'first'$",
    ):
        parallel()  # type: ignore[call-overload]

    with pytest.raises(
        TypeError,
        match=r"^parallel\(\) must be called with at least one step$",
    ):
        parallel([])

    with pytest.raises(
        TypeError,
        match=r"^parallel\(\) cannot be used with a single passthrough step$",
    ):
        parallel(passthrough())  # type: ignore[call-overload]

    inc = step("increment", lambda a: dict(a=a + 1))
    assert run(parallel([inc], inc), input=[dict(a=2)]) == RunResult(
        [{"input": {"a": 2}, "increment": {"a": 3}}],
        [{"input": {"a": 2}, "increment": {"a": 3}}],
    )

    # Parallel accepts an iterable of a single step, yielding the step itself
    assert parallel([inc]) is inc


def test_passthrough() -> None:

    flow = chain(
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

    result = _sort_nested(run(flow))

    assert _sort_nested(result) == [
        [
            dict(
                input=dict(a=1, b=2, c=3),
                double_a=dict(a=2),
            )
        ],
        [
            dict(
                input=dict(a=1, b=2, c=3),
                increment_a=dict(a=2),
                double_a=dict(a=4),
            )
        ],
    ]

    with pytest.raises(TypeError, match=r"^unsupported operand type\(s\) for >>"):
        # Producer step + passthrough --> warning
        chain(  # type: ignore[call-overload]
            step(
                "input",
                dict(a=1, b=2, c=3),
            ),
            passthrough(),
        )

    with pytest.raises(TypeError, match=r"^unsupported operand type\(s\) for >>"):
        # Transformer >> passthrough → not implemented
        chain(  # type: ignore[call-overload]
            step("increment_a", lambda a: dict(a=a + 1)), passthrough()
        )

    with pytest.raises(TypeError, match=r"^unsupported operand type\(s\) for >>"):
        # Passthrough >> transformer → not implemented
        chain(  # type: ignore[call-overload]
            passthrough(), step("increment_a", lambda a: dict(a=a + 1))
        )

    with pytest.raises(TypeError, match=r"^unsupported operand type\(s\) for >>"):
        # Passthrough >> passthrough → not implemented
        chain(passthrough(), passthrough())  # type: ignore[call-overload]

    with pytest.raises(
        TypeError,
        match=r"^parallel\(\) may include at most one passthrough step, but got 2",
    ):
        # Passthrough & passthrough → not implemented
        parallel(passthrough(), passthrough())


def test_run_exceptions() -> None:
    """
    Test exceptions in the 'run' function.
    """

    # Test with a transformer that requires an input

    def _function(a: int, b: int) -> dict[str, int]:
        return dict(ab=a * b)

    my_step = step(
        "step",
        _function,
    )

    with pytest.raises(
        ValueError,
        match="^Step 'step' is missing input attributes: a, b$",
    ):
        run(my_step)

    # Test with an invalid step argument

    with pytest.raises(
        TypeError,
        match="^arg steps must be a step or composition of steps, but got a tuple",
    ):
        run((1,))  # type: ignore[call-overload]

    # Test with a superfluous input argument

    with pytest.raises(
        TypeError,
        match=(
            r"^arg steps must be a step or composition of steps, but got a tuple; "
            r"did you leave a comma before a closing parenthesis\?$"
        ),
    ):
        run(
            (  # type: ignore[call-overload]
                step("input", dict(a=2, b=3, c=1)) >> my_step,
            )
        )

    with pytest.raises(
        TypeError,
        match=(
            "^arg input cannot be specified when the given steps include a leading "
            "producer step$"
        ),
    ):
        run(
            step("input", dict(a=2, b=3, c=1))  # type: ignore[call-overload]
            >> my_step,
            input=[dict(a=2, b=3, c=1)],
        )

    with pytest.raises(
        TypeError,
        match=r"^Expected step 'input' to produce mappings, but got: 1$",
    ):
        run(step("inc", lambda x: x + 1), input=[1, 2, 3])  # type: ignore[list-item]


def test_run_warnings(caplog: Iterator[pytest.LogCaptureFixture]) -> None:
    run(
        step("add", lambda a, b: dict(c=a + b), a=1, b=2),
        input=[dict(a=2, b=5)],
    )

    assert (
        "Fixed keyword arguments of step 'add' shadow attributes of the source "
        "product: a=1 shadows a=2, b=2 shadows b=5"
    ) in caplog.text  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_producer_step() -> None:
    # Test the step function used to create a producer

    # Test with a synchronous function
    def _function() -> dict[str, int]:
        return dict(a=6)

    my_step = step(
        "step",
        _function,
    )
    assert list(my_step) == [dict(a=6)]

    def _function_iter() -> Iterator[dict[str, int]]:
        yield dict(ab=5)
        yield dict(ac=3)

    my_step = step("step", _function_iter)
    assert list(my_step) == [dict(ab=5), dict(ac=3)]

    # Test with an asynchronous function
    async def _function_async() -> AsyncIterator[dict[str, int]]:
        yield dict(ab=5)
        yield dict(ac=3)

    assert _sort_nested(
        [d.attributes for d in await iter_async_to_sync(step("step", _function_async))]
    ) == [
        dict(ab=5),
        dict(ac=3),
    ]


def test_timestamps() -> None:
    # Create a simple serial flow
    flow = step(
        "input",
        dict(a=1, b=2, c=3),
    ) >> step(
        "multiply",
        lambda a, b: dict(ab=a * b),
    )

    # Run the flow with the timestamps option enabled
    result = run(flow, timestamps=True)

    df = (
        # Create a dataframe from the result
        result.to_frame()
        # The frame has two column levels.
        # Move the top column level to the index axis.
        .stack(level=0)
    )

    # Get the start and end columns
    col_start_time: pd.Series = (  # type: ignore[type-arg]
        df.loc[:, DictProduct.KEY_START_TIME]
    )
    col_end_time: pd.Series = (  # type: ignore[type-arg]
        df.loc[:, DictProduct.KEY_END_TIME]
    )

    # Confirm the dtypes of the start and end time columns are a float64
    assert col_start_time.dtype == pd.Float64Dtype()
    assert col_end_time.dtype == pd.Float64Dtype()

    # Confirm all end times are greater than or equal to the start times
    # noinspection PyUnresolvedReferences
    assert (col_end_time >= col_start_time).all()

    # Draw the timeline as text

    string_buffer = StringIO()
    result.draw_timeline(style="text", out=string_buffer)
    print(string_buffer.getvalue())
    # noinspection RegExpRepeatedSpace,RegExpSimplifiable
    assert (
        re.fullmatch(
            r"=================================== Timeline ============================"
            r"=======\n"
            r"\n"
            r"Output  Path  Step      Start +End *\n"
            r"======  ====  ========  ======*  ====*\n"
            r"     0     0  input     +\d+\.\d+(e-\d+)? +\d+\.\d+(e-\d+)?\n"
            r"     0     0  multiply  +\d+\.\d+(e-\d+)? +\d+\.\d+(e-\d+)?\n"
            r"\n",
            string_buffer.getvalue(),
            re.MULTILINE,
        )
        is not None
    )

    with pytest.raises(
        ValueError, match="^arg out is only supported with arg style='text'$"
    ):
        result.draw_timeline(style="matplot", out=string_buffer)


def test_invalid_identifiers() -> None:
    expected_message = (
        "Step name can only include valid python identifiers "
        "(a-z, A-Z, 0-9, _): 'step:'"
    )
    with pytest.raises(ValueError, match=re.escape(expected_message)):
        step("step:", lambda x: x)

    with pytest.raises(
        ValueError,
        match=(
            r"^Names of keyword arguments must be valid identifiers, but got: 'step:', "
            r"'step;'$"
        ),
    ):
        step("step", lambda x: x, **{"step:": 1, "step_valid": 2, "step;": 3})

    with pytest.raises(
        ValueError,
        match=(
            r"^Attribute names in output of step 'my_step' must be valid Python "
            r"identifiers, but included invalid names: 'x:', 'invalid attribute'$"
        ),
    ):
        run(step("my_step", lambda: {"x:": 3, "invalid attribute": 4}))


def test_implicit_input() -> None:
    assert run(step("multiply", lambda a, b: dict(ab=a * b), a=2, b=3)) == RunResult(
        [
            dict(multiply=dict(ab=6)),
        ]
    )


#
# Auxiliary functions
#


def _sort_nested(
    dicts: RunResult | Mapping[str, Any] | Iterator[Any] | Sequence[Any]
) -> Mapping[str, Any] | list[Any]:
    """
    Sort dictionaries by their string representation.

    :param dicts: the dictionaries to sort
    :return: the sorted dictionaries
    """
    log.debug(f"sort: {type(dicts)} {dicts!r}")
    if isinstance(dicts, RunResult):
        return _sort_nested(dicts.get_outputs_per_path())
    if isinstance(dicts, (Iterator, Sequence)):
        return sorted(map(_sort_nested, dicts), key=repr)
    elif isinstance(dicts, Mapping):
        return dicts
    else:
        raise TypeError(f"Unexpected type of arg dicts: {type(dicts)}")
