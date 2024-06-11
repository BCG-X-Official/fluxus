"""
Native classes, enhanced with mixin class :class:`.HasExpressionRepr`.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sized
from typing import Any

from pytools.expression.atomic import Id
from pytools.expression.base import BracketPair, Invocation

log = logging.getLogger(__name__)

__all__ = [
    "simplify_repr_attributes",
]


#
# Classes
#


def simplify_repr_attributes(attributes: Mapping[str, Any]) -> dict[str, Any]:
    """
    Simplify the values of an attribute-value dictionary for representation.

    Returns a new dictionary with all attribute-value pairs where the value is

    - a number
    - a boolean
    - a string or ``bytes`` object, shortening it to a maximum length of 15
      characters
    - an object implementing :func:`len`, returning its type and length as an
      :class:`.Expression` object in the format ``"ClassName<n>"``

    :param attributes: the attribute-value dictionary to simplify
    :return: the simplified attribute-value dictionary
    """
    return {
        name: value
        for name, value in (
            (name, _simplify_repr_value(value)) for name, value in attributes.items()
        )
        if value is not None
    }


#
# Auxiliary functions
#
def _simplify_repr_value(value: Any) -> Any:
    """
    Simplify a value for representation.

    :param value: the value to simplify
    :return: the simplified value
    """

    def _str_repr(s: str) -> str:
        # remove leading and trailing whitespace, and escape special characters
        return repr(s.strip())[1:-1]

    if isinstance(value, (int, float, complex, bool)):
        return value
    elif isinstance(value, (str, bytes)):
        if len(value) <= 15:
            return value
        else:
            value = str(value.strip())
            return f"{_str_repr(value[:6])}...{_str_repr(value[-6:])}"
    elif isinstance(value, Sized):
        return Invocation(
            Id(type(value)),
            brackets=BracketPair.ANGLED,
            args=(len(value),),
        )
    return None
