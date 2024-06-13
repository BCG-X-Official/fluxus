"""
Implementation of the functional API for the flow module.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from typing import Any

from pytools.api import inheritdoc
from pytools.repr import HasDictRepr

from ...lineage import HasLineage

log = logging.getLogger(__name__)

__all__ = [
    "DictProduct",
]


@inheritdoc(match="""[see superclass]""")
class DictProduct(HasLineage["DictProduct | None"], Mapping[str, Any], HasDictRepr):
    """
    A flow product that consists of an attribute-value mapping.

    The product name is the name of the step that created this product.

    The attribute names must be valid Python identifiers.
    """

    #: The product attribute name for the start time of the step that created this
    #: product.
    KEY_START_TIME = "[start]"

    #: The product attribute name for the end time of the step that created this
    #: product.
    KEY_END_TIME = "[end]"

    #: The name of this product.
    name: str

    #: The attributes that were changed or added by the step that created this product.
    _product_attributes: Mapping[str, Any]

    #: The complete set of attributes of this product, including both input and output
    #: attributes; output attributes take precedence over input attributes of the same
    #: name.
    attributes: Mapping[str, Any]

    #: The start CPU time of the step that created this product, in seconds since an
    #: arbitrary starting point.
    start_time: float

    #: The end CPU time of the step that created this product, in seconds since an
    #: arbitrary starting point.
    end_time: float

    #: The precursor of this product.
    _precursor: DictProduct | None

    def __init__(
        self,
        name: str,
        product_attributes: Mapping[str, Any],
        *,
        precursor: DictProduct | None = None,
        start_time: float,
        end_time: float,
    ) -> None:
        """
        :param name: the name of this product
        :param product_attributes: the attributes that were changed or added by the
            step that created this product, comprising both fixed attributes and
            dynamically generated attributes
        :param precursor: the precursor of this product (optional)
        :param start_time: the start CPU time of the step that created this product, in
            seconds since an arbitrary starting point
        :param end_time: the end CPU time of the step that created this product, in
            seconds since an arbitrary starting point
        :raises TypeError: if the product attributes are not a mapping
        :raises ValueError: if the attribute names are not valid Python identifiers
        """
        # Validate that the attributes are a mapping with valid identifiers as keys
        _validate_product_attributes(product_attributes, step_name=name)

        self.name = name
        self.start_time = start_time
        self.end_time = end_time
        self._product_attributes = product_attributes
        self._precursor = precursor

        # We calculate the complete set of attributes by combining the input and output,
        # with product attributes (the output) taking precedence over input attributes
        # of the same name.
        self.attributes = (
            {**precursor.attributes, **product_attributes}
            if precursor
            else product_attributes
        )

    @property
    def product_name(self) -> str:
        return self.name

    @property
    def product_attributes(self) -> Mapping[str, Any]:
        """[see superclass]"""
        return self._product_attributes

    @property
    def precursor(self) -> DictProduct | None:
        """[see superclass]"""
        return self._precursor

    def __getitem__(self, __key: str) -> Any:
        return self._product_attributes[__key]

    def __len__(self) -> int:
        return len(self._product_attributes)

    def __iter__(self) -> Iterator[str]:
        return iter(self._product_attributes)


def _validate_product_attributes(
    attributes: Mapping[str, Any], *, step_name: str
) -> None:
    """
    Validate that the output of a step is a :class:`Mapping`, and that its attribute
    names are valid Python identifiers.

    :param attributes: the step output to validate
    :param step_name: the name of the step producing the mapping
    :raises TypeError: if the step output is not a mapping
    :raises ValueError: if the attribute names are invalid
    """

    if not isinstance(attributes, Mapping):
        raise TypeError(
            f"Expected step {step_name!r} to produce mappings, but got: {attributes!r}"
        )

    invalid_names = [
        name
        for name in attributes
        if not (isinstance(name, str) and name.isidentifier())
    ]
    if invalid_names:
        raise ValueError(
            f"Attribute names in output of step {step_name!r} must be valid Python "
            f"identifiers, but included invalid names: "
            + ", ".join(map(repr, invalid_names))
        )
