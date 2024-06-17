"""
Unit tests for product lineages.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

from fluxus.lineage import (
    HasLineage,
    LabelingProducer,
    LabelingTransformer,
    LineageOrigin,
)

log = logging.getLogger(__name__)


class Origin(LineageOrigin):
    """
    A simple origin product.
    """

    def __init__(self, type: str = "origin"):
        self.type = type

    @property
    def product_name(self) -> str:
        return Origin.__name__


class Derived(HasLineage["Origin | Derived"]):
    """
    A derived product.
    """

    def __init__(
        self,
        precursor: Origin | Derived,
        type: str = "derived",
        is_derived: bool = True,
    ) -> None:
        self._precursor = precursor
        self.type = type
        self.is_derived = is_derived

    @property
    def precursor(self) -> Origin | Derived:
        return self._precursor

    @property
    def product_name(self) -> str:
        return Derived.__name__


class OriginProducer(LabelingProducer[Origin]):
    """
    A producer of ``Origin`` products.
    """

    def produce(self) -> Iterator[Origin]:
        yield Origin()


class OriginTransformer(LabelingTransformer[Origin, Derived]):
    """
    A transformer that transforms ``Origin`` products into ``Derived`` products.
    """

    def transform(self, source_product: Origin) -> Iterator[Derived]:
        yield Derived(source_product)


def test_lineage_attributes() -> None:
    """
    Test the attributes of a product lineage.
    """

    # Construct a product lineage

    a = Origin().label(my_label="my value")
    b = Derived(a)
    c = Derived(b).label(my_other_label="my other value")
    d = Derived(c)

    # Navigate the lineage backwards

    assert d.precursor == c
    assert c.precursor == b
    assert b.precursor == a
    assert a.precursor is None

    # Check the product attributes
    assert a.product_attributes == dict(type="origin")
    assert b.product_attributes == dict(type="derived", is_derived=True)
    assert c.product_attributes == dict(type="derived", is_derived=True)
    assert d.product_attributes == dict(type="derived", is_derived=True)

    # Check the product labels
    assert a.product_labels == dict(my_label="my value")
    assert b.product_labels == {}
    assert c.product_labels == dict(my_other_label="my other value")
    assert d.product_labels == {}

    # check the lineage properties
    assert d.get_lineage_attributes() == {
        "Origin": dict(my_label="my value", type="origin"),
        "Derived": dict(is_derived=True, type="derived"),
        "Derived#1": dict(
            is_derived=True, my_other_label="my other value", type="derived"
        ),
        "Derived#2": dict(is_derived=True, type="derived"),
    }

    # a ValueError is raised when attempting to override a lineage attribute
    with pytest.raises(
        ValueError, match="Label would override existing attributes: type"
    ):
        d.label(type="overridden")

    with pytest.raises(
        ValueError,
        match=(
            r"Labels would override existing attributes: "
            r"(type, is_derived|is_derived, type)"
        ),
    ):
        d.label(type="overridden", is_derived="overridden")


def test_conduit_labels() -> None:
    """
    Test class ``Labeler`` on a ``Producer``.
    """

    producer = OriginProducer().label(origin_label="origin", origin_list=[1, 2, 3])
    transformer = OriginTransformer().label(tx_label="transformer")
    chain = producer >> transformer

    expected_lineage_attributes = dict(
        Origin=dict(type="origin", origin_label="origin", origin_list=[1, 2, 3]),
        Derived=dict(type="derived", is_derived=True, tx_label="transformer"),
    )

    for product in chain:
        assert product.get_lineage_attributes() == expected_lineage_attributes

    assert next(iter(chain)).get_lineage_attributes() == expected_lineage_attributes

    assert producer.get_repr_attributes() == {
        "#origin_label": "origin",
        "#origin_list": [1, 2, 3],
    }
    assert transformer.get_repr_attributes() == {"#tx_label": "transformer"}
