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
Implementation of the product lineage interface.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Iterator, Mapping
from typing import Any, Generic, TypeVar, final

from typing_extensions import Self

from pytools.api import get_init_params

log = logging.getLogger(__name__)

__all__ = [
    "HasLineage",
    "LineageOrigin",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_Precursor_ret = TypeVar(
    "T_Precursor_ret", bound="HasLineage[Any] | None", covariant=True
)


#
# Classes
#


class HasLineage(Generic[T_Precursor_ret], metaclass=ABCMeta):
    # noinspection GrazieInspection
    """
    Mixin-class for a product of a flow :class:`.Conduit` that is derived from a
    preceding product.

    To implement this in a user-defined class, inherit from this class and
    implement the :attr:`.precursor` and :attr:`.product_attributes` properties:

    - :attr:`.precursor`
        Returns the preceding product, or ``None`` if there is no preceding product.
        The preceding product must also implement the :class:`HasLineage` interface.
    - :attr:`.product_attributes`
        Returns a dictionary of attributes of this product, excluding the precursor.

    Users can also label products with additional values using the :meth:`.label`
    method.

    As an example, consider a class ``Route`` that is derived from a preceding class
    ``Destination``. ``Destination`` has no precursor, so its :attr:`.precursor`
    property returns ``None``.

    .. code-block:: python

        class Destination(LineageOrigin):
            \"""A destination in a route.\"""

            def __init__(self, name: str) -> None:
                self.name = name

            def product_attributes(self) -> dict[str, Any]:
                \"""
                Attributes of this product.
                \"""
                return dict(name=self.name)


        class Route(HasLineage):
            \"""A route to a given destination.""

            def __init__(
                self,
                start: str,
                destination: Destination,
            ) -> None:
                self.start = start
                self.destination = destination
                # ...

            @property
            def precursor(self) -> Destination:
                \"""
                The preceding product.
                \"""
                return self.destination

            @property
            def product_attributes(self) -> dict[str, Any]:
                \"""
                Attributes of this product.
                \"""
                return dict(start=self.start)

    Now we can create a ``Route`` object representing a route with any number of stops
    to a destination, adding custom labels:

    .. code-block:: python

        par = Destination("Paris")
        lon_par = Route("London", paris).label(distance_km=340, mode="train")
        ber_lon = Route("Berlin", london_paris).label(distance_km=940, mode="plane")

    The lineage of the ``ber_lon`` object is ``[par, lon_par, ber_lon]``.

    Calling ``ber_lon.get_lineage_attributes()`` will return the following dictionary:

    .. code-block:: python

        {
            ("Destination", "name"): "Paris",
            ("Route", "start"): "London",
            ("Route", "distance_km"): 340,
            ("Route", "mode"): "train",
            ("Route#1", "start"): "Berlin",
            ("Route#1", "distance_km"): 940,
            ("Route#1", "mode"): "plane",
        }
    """

    __labels: dict[str, Any] | None = None

    @property
    @abstractmethod
    def precursor(self) -> T_Precursor_ret:
        """
        The preceding product.
        """

    @property
    @abstractmethod
    def product_name(self) -> str:
        """
        A name representing this product; used for grouping attributes in
        :meth:`.get_lineage_attributes`.
        """

    @property
    def product_attributes(self) -> Mapping[str, Any]:
        """
        Attributes of this product, excluding the precursor.
        """
        # The precursor should not be included in the attributes
        precursor = self.precursor

        # We get the init parameters, including the default values
        params = get_init_params(self, ignore_default=False)

        if precursor is None:
            # There is no precursor, so we return all init parameters
            return params

        return {
            name: value
            for name, value in params.items()
            # We skip the init parameter if it is the precursor
            if value is not precursor
        }

    @property
    @final
    def product_labels(self) -> Mapping[str, Any]:
        """
        Labels of this product that were set using the :meth:`.label` method.
        """
        return self.__labels or {}

    def label(self, **labels: Any) -> Self:
        """
        Label this product with the given labels and return ``self``.

        This means that labelling can be easily inserted into the code anywhere
        the product is used, e.g., after object creation, replacing

        .. code-block:: python

                product = Product()

        with

        .. code-block:: python

                product = Product().label(label1=value1).label(label2=value2)

        or, even better

        .. code-block:: python

                product = Product().label(label1=value1, label2=value2)

        If the labels are already stored in a dictionary, they can be passed as
        keyword arguments using the ``**`` operator:

        .. code-block:: python

                labels = {"label1": value1, "label2": value2}
                product = Product().label(**labels)


        :param labels: the labels to add
        :return: ``self``
        """

        # Raise a ValueError if any label would override an existing attribute.
        invalid_labels = labels.keys() & self.product_attributes.keys() & labels.keys()
        if invalid_labels:
            raise ValueError(
                f"Label{'s' if len(invalid_labels) > 1 else ''} would override "
                "existing attributes: " + ", ".join(invalid_labels)
            )

        existing_labels = self.__labels
        if existing_labels is None:
            # We do not have a labels dictionary yet, so we create one.
            self.__labels = labels
        else:
            # We already have a labels dictionary, so we update it.
            existing_labels.update(labels)

        # We return self to allow method chaining.
        return self

    @final
    def get_lineage(self) -> list[HasLineage[Any]]:
        """
        Get the lineage of this product as a list, starting with originating product and
        ending with this product.

        :return: the lineage of this product
        """

        def _iter_lineage(product: HasLineage[Any]) -> Iterator[HasLineage[Any]]:
            precursor = product.precursor
            if precursor is not None:
                yield from _iter_lineage(precursor)
            yield product

        return list(_iter_lineage(self))

    @final
    def get_lineage_attributes(self) -> dict[str, dict[str, Any]]:
        """
        Get the attributes and labels of this product's lineage.

        Creates a nested dictionary where the first level keys are product names and the
        second level keys are attribute/label names. The second level values are
        attribute/label values.

        Product names are unique names for each product in the lineage. This is the
        class name of the product, followed by an ascending integer if the same class
        appears multiple times in the lineage. For example, if the lineage is ``[A(…),
        B(…), A(…), A(…), C(…), B(…)]``, the product names will be ``["A", "B", "A#1",
        "A#2", "C", "B#1"]``.

        If a label has the same name as an attribute, the label's value will override
        the attribute's value in the dictionary. For example, if a product ``A(…)`` has
        an attribute ``"name"`` and a label ``"name"``, the label's value will be used
        for the key ``{"A": {"name": …}}``.

        :return: the attributes and labels of the lineage as a nested dictionary
        """

        # The lineage for which we want to get the attributes.
        lineage: list[HasLineage[Any]] = self.get_lineage()

        # Create a list of unique names for each product in the lineage.
        # This is the class name of the product, followed by an ascending integer
        # if the same class appears multiple times in the lineage.
        product_names: list[str] = _product_names(lineage)

        # Create a nested dictionary with two levels: the product names and the
        # attribute names. Exclude products with no attributes.
        return {
            product_name: attributes
            for product_name, attributes in (
                (
                    product_name,
                    {
                        # Override attributes with labels
                        **product.product_attributes,
                        **product.product_labels,
                    },
                )
                for product_name, product in zip(product_names, lineage)
            )
            if attributes
        }


class LineageOrigin(HasLineage[None], metaclass=ABCMeta):
    """
    A product that has no precursor.

    This is a convenience class for products that are not derived from any other
    product.

    To implement this in a user-defined class, inherit from this class and implement
    the :attr:`.product_attributes` property, as described in :class:`HasLineage`.
    """

    @property
    @final
    def precursor(self) -> None:
        """
        The preceding product.
        """
        return None


#
# Auxiliary functions
#


def _product_names(lineage: list[HasLineage[Any]]) -> list[str]:
    """
    Create a list of unique names for each product in the lineage.

    This is the class name of the product, followed by an ascending integer if the same
    class appears multiple times in the lineage.

    For example, if the lineage is ``[A, B, A, A, C, B]``, the names will be
    ``["A", "B", "A#1", "A#2", "C", "B#1"]``.

    :param lineage: the lineage of products for which to create names
    :return: the list of names
    """

    def _generate_name(item: HasLineage[Any]) -> str:
        name = item.product_name
        count = counts.get(name, 0)
        counts[name] = count + 1
        if count:
            return f"{name}#{count}"
        else:
            return name

    counts: dict[str, int] = {}

    return list(map(_generate_name, lineage))


def _as_dict(mapping: Mapping[str, Any]) -> dict[str, Any]:
    """
    Convert a mapping to a dictionary, unless it is already a dictionary.

    :param mapping: the mapping to convert
    :return: the dictionary
    """
    return mapping if isinstance(mapping, dict) else dict(mapping)
