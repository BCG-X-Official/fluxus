"""
Implementation of ``RunResult``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import NoneType
from typing import Any, Literal, TextIO, cast

import pandas as pd

from pytools.api import inheritdoc
from pytools.expression import Expression, HasExpressionRepr
from pytools.expression.atomic import Id

from .product import DictProduct

log = logging.getLogger(__name__)

__all__ = [
    "RunResult",
]


@inheritdoc(match="""[see superclass]""")
class RunResult(HasExpressionRepr):
    # noinspection GrazieInspection
    """
    The result of running a flow.

    A run result represents the output of a flow, as a list of dictionaries for each
    distinct path through the flow.

    Each dictionary represents the output of the flow for a single input. The dictionary
    is in the form of a nested dictionary, where the keys of the outer dictionary are
    the names of the steps, and the values are attribute-value mappings produced by
    each of the steps.

    For example:

    .. code-block:: python

        # Create a producer step that produces a single dictionary
        producer = step(
            "input",
            dict(a=1, b=2, c=3),
        )

        # Define an increment function
        def increment(a: int, by: int) -> dict[str, int]:
            return dict(a=a + by)

        # Create transformers step that increment the value of 'a'
        increment_by_1 = step(
            "increment",
            increment,
            by=1,
        )

        increment_by_2 = step(
            "increment",
            increment,
            by=2,
        )

        # Construct and run the flow
        run(producer >> (increment_by_1 & increment_by_2))

    The output will be:

    .. code-block:: python

        RunResult(
            [{'input': {'a': 1, 'b': 2, 'c': 3}, 'increment': {'a': 2}}],
            [{'input': {'a': 1, 'b': 2, 'c': 3}, 'increment': {'a': 3}}]
        )

    To access the results of running the flow, use the :meth:`get_outputs` method.

    To access the results grouped by path, use the :meth:`get_outputs_per_path` method.

    The run result can be converted to a data frame using the :meth:`to_frame` method.
    """

    _step_results: tuple[list[dict[str, dict[str, Any]]], ...]

    def __init__(self, *step_results: list[dict[str, dict[str, Any]]]) -> None:
        """
        :param step_results: the results of running the flow, as a list of nested
            dictionaries for each distinct path through the flow
        """
        for step_result in step_results:
            if not isinstance(step_result, list):
                raise TypeError(
                    f"expected a list of dictionaries but got: {step_result!r}"
                )
            for flow_output in step_result:
                if not isinstance(flow_output, dict):
                    raise TypeError(f"expected a dictionary but got: {flow_output!r}")
                for step_name, step_output in flow_output.items():
                    if not isinstance(step_output, dict):
                        raise TypeError(
                            f"expected a dictionary for step {step_name!r} but got: "
                            f"{step_output!r}"
                        )
        self._step_results = step_results

    def get_outputs(self) -> Iterator[Mapping[str, Mapping[str, Any]]]:
        """
        Get the results of running the flow, for all inputs across all paths.

        :return: an iterator over all results
        """
        for path in self._step_results:
            yield from path

    def get_outputs_per_path(
        self,
    ) -> list[Iterator[Mapping[str, Mapping[str, Any]]]]:
        """
        Get the results of running the flow, as a list of outputs for each path.

        Returns a nested list, where the outer list corresponds to the paths through the
        flow, and the inner list corresponds to the outputs along that path for all
        inputs.

        Note that the inner list may be empty if the path has no outputs, or may contain
        multiple outputs for individual inputs.

        :return: the results of running the flow, as a list of output iterators for each
            path
        """
        return [iter(path) for path in self._step_results]

    def to_frame(
        self, *, path: int | None = None, simplify: bool = False
    ) -> pd.DataFrame:
        """
        Convert this run result to a frame.

        If the path is specified, the output will be a frame for that path only.

        :param path: the index of the path to convert to a frame; if not specified, the
            output will be a frame for all paths
        :param simplify: if ``True``, convert complex types to strings using
            :func:`simplify_complex_types`; if ``False``, leave objects with complex
            types as they are (default: ``False``)
        :return: this run result
        """
        if path is not None:
            if path < 0 or path >= len(self._step_results):
                raise IndexError(f"path index out of bounds: {path}")
            outputs_for_path = self._step_results[path]
            if not outputs_for_path:
                raise ValueError(f"path {path} has no outputs; cannot convert to frame")
            return _dicts_to_frame(outputs_for_path, simplify=simplify or False)
        else:
            if not self._step_results:
                raise TypeError("RunResult has no outputs; cannot convert to frame")
            return _dicts_to_frame(self._step_results, simplify=simplify or False)

    def draw_timeline(
        self,
        *,
        style: Literal["text", "matplot", "matplot_dark"] = "matplot",
        out: TextIO | None = None,
    ) -> None:
        """
        Draw a timeline of the flow execution.

        :param style: the style to use for rendering the timeline
        :param out: the output stream to write the timeline to, if style is ``"text"``
            (defaults to :obj:`sys.stdout`)
        """

        if not any(
            DictProduct.KEY_START_TIME in step_output
            and DictProduct.KEY_END_TIME in step_output
            for path in self._step_results
            for execution_output in path
            for step_output in execution_output.values()
        ):
            raise ValueError(
                "Results do not include timestamps; re-run the flow with using "
                "function 'run' with argument 'timestamps=True'"
            )
        from ..viz import TimelineDrawer, TimelineTextStyle

        style_arg: TimelineTextStyle | str

        if out is not None:
            text_style_name = TimelineTextStyle.get_default_style_name()
            if style == text_style_name:
                style_arg = TimelineTextStyle(out=out)
            else:
                raise ValueError(
                    f"arg out is only supported with arg style={text_style_name!r}"
                )
        else:
            permissible_style_names = TimelineDrawer.get_named_styles()
            if style not in permissible_style_names:
                raise ValueError(
                    f"arg style must be one of: "
                    + ", ".join(map(repr, permissible_style_names))
                )
            style_arg = style

        TimelineDrawer(style=style_arg).draw(self, title="Timeline")

    def to_expression(self) -> Expression:
        """[see superclass]"""
        return Id(type(self))(*self._step_results)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, RunResult) and self._step_results == other._step_results
        )


#
# Auxiliary functions
#


def _dicts_to_frame(
    dicts: Sequence[Any], *, simplify: bool, max_levels: int | None = None
) -> pd.DataFrame:
    """
    Convert a (possibly nested) iterable of dictionaries or mappings to a DataFrame,
    using the dictionary keys as column names.

    Nested lists are converted to multi-level indices, where the first level holds the
    outermost list indices and subsequent index levels represent the nested list
    indices.

    Nested dictionaries are converted to multi-level columns, where the first level
    holds the outermost dictionary keys and subsequent index levels represent the
    nested dictionary keys.

    :param dicts: the dictionaries or mappings to convert, either as a single iterable
        or as a nested iterable
    :param simplify: if ``True``, convert complex types to strings using
        :func:`simplify_complex_types`; if ``False``, leave objects with complex types
        as they are
    :param max_levels: the maximum number of levels in the resulting multi-level index;
        if ``None``, the number of levels is not limited (default: ``None``)
    :return: the data frame
    """

    element_types = {type(d) for d in dicts}
    if len(element_types) > 1:
        raise TypeError("arg dicts must not contain mixed types of elements")
    element_type = element_types.pop()

    if issubclass(element_type, Mapping):
        return (
            pd.concat(
                (
                    _dict_to_series(d, simplify=simplify, max_levels=max_levels)
                    for d in dicts
                ),
                axis=1,
                ignore_index=True,
            )
            .T.convert_dtypes()
            .rename_axis(index="item")
        )
    elif issubclass(element_type, Sequence):
        # We have a sequence of sequences
        sub_frames = [
            # We iterate over the inner sequences and convert them to data
            # frames, which are then concatenated along the column axis.
            # We skip empty sequences.
            _dicts_to_frame(d, simplify=simplify, max_levels=max_levels)
            for d in dicts
            if d  # Skip empty sequences
        ]
        if not sub_frames:
            raise ValueError("arg dicts must not contain only empty sequences")
        return pd.concat(
            cast(
                Iterable[pd.DataFrame],
                sub_frames,
            ),
            axis=0,
            # We add an index level for the outer sequence
            names=["group"],
        )
    else:
        raise TypeError(
            "arg dicts must be a sequence or nested sequence of dictionaries"
        )


def _dict_to_series(
    d: Mapping[str, Any], *, simplify: bool, max_levels: int | None = None
) -> pd.Series[Any]:
    """
    Convert a dictionary to a Series, using the keys as index labels.

    Nested dictionaries are converted to multi-level indices, where the first level
    holds the outermost dictionary keys and subsequent index levels represent the nested
    dictionary keys.

    :param d: the dictionary or mapping to convert
    :param simplify: if ``True``, convert complex types to strings using
        :func:`simplify_complex_types`; if ``False``, leave objects with complex types
        as they are
    :param max_levels: the maximum number of levels in the resulting multi-level index;
        if ``None``, the number of levels is not limited (default: ``None``)
    :return: the series
    """

    if max_levels is not None and max_levels <= 0:
        raise ValueError(
            f"arg max_levels must be a positive integer or None, but got: {max_levels}"
        )

    def _flatten(
        sub_dict: Mapping[str, Any],
        level: int,
        parent_key: tuple[str, ...] | str | None = None,
    ) -> Iterator[tuple[tuple[str, ...] | str, Any]]:
        new_key: tuple[str, ...] | str

        for k, v in sub_dict.items():
            if not parent_key:
                # The first level of the index is a single string
                new_key = k
            elif isinstance(parent_key, tuple):
                # Subsequent levels are tuples of strings
                new_key = (*parent_key, k)
            else:
                # The second level of the index is the first to be a tuple
                new_key = (parent_key, k)
            if isinstance(v, dict) and level != 1:
                yield from _flatten(v, level - 1, new_key)
            else:
                # We are at the last level of the index and will stop flattening
                yield new_key, _simplify_complex_types(v) if simplify else v

    sr: pd.Series[Any] = pd.Series(dict(_flatten(d, max_levels or 0)))
    return sr


#
# Auxiliary functions
#


def _simplify_complex_types(
    value: Any,
) -> bool | int | float | str | bytes | complex | None:
    """
    Convert instances of complex types to strings.

    :param value: the value to convert
    :return: the value, with complex types converted to strings
    """
    if isinstance(value, (bool, int, float, str, bytes, complex, NoneType)):
        return value
    else:
        return str(value)
