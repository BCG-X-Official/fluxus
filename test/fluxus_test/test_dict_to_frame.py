"""
Test conversions from dicts to frames.
"""

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal


def test_dict_to_series() -> None:
    """
    Test conversions from dicts to frames.
    """

    # noinspection PyProtectedMember
    from fluxus.functional._result import _dict_to_series, _dicts_to_frame

    # Test with a single dictionary
    assert_series_equal(
        _dict_to_series(dict(a=1, b=2, c=3), simplify=True),
        pd.Series(
            dict(a=1, b=2, c=3),
        ),
    )

    # Test with a nested dictionary
    assert_series_equal(
        _dict_to_series(dict(a=dict(b=2, c=3)), simplify=True),
        pd.Series({("a", "b"): 2, ("a", "c"): 3}),
    )

    # Test with a nested dictionary and a limit on the number of levels
    assert_series_equal(
        _dict_to_series(dict(a=dict(b=2, c=3)), simplify=True, max_levels=1),
        pd.Series(dict(a="{'b': 2, 'c': 3}")),
    )

    # Test with a nested dictionary and a limit on the number of levels,
    # and complex types
    expected_series = pd.Series(dict(a=dict(b=2, c=3)))
    assert_series_equal(
        _dict_to_series(dict(a=dict(b=2, c=3)), simplify=False, max_levels=1),
        expected_series,
    )

    # Test with a list of dictionaries
    assert_frame_equal(
        _dicts_to_frame([dict(a=1, b=2, c=3)], simplify=True),
        pd.DataFrame(dict(a=[1], b=[2], c=[3]), dtype=pd.Int64Dtype).rename_axis(
            index="item"
        ),
    )

    # Test with a list of dictionaries and a limit on the number of levels
    # Simplify is False, so the dictionary is kept as a dictionary
    assert_frame_equal(
        _dicts_to_frame([dict(a=dict(b=2, c=3))], simplify=False, max_levels=1),
        pd.DataFrame({"a": [{"b": 2, "c": 3}]}).rename_axis(index="item"),
    )

    # Test with a list of dictionaries and a limit on the number of levels
    # Simplify is True, so the dictionary is converted to a string
    assert_frame_equal(
        _dicts_to_frame([dict(a=dict(b=2, c=3))], simplify=True, max_levels=1),
        pd.DataFrame(dict(a=["{'b': 2, 'c': 3}"]), dtype=pd.StringDtype).rename_axis(
            index="item"
        ),
    )

    # Passing a negative value for max_levels should raise a ValueError
    with pytest.raises(
        ValueError,
        match="^arg max_levels must be a positive integer or None, but got: -1$",
    ):
        _dict_to_series(dict(a=dict(b=2, c=3)), simplify=True, max_levels=-1)
