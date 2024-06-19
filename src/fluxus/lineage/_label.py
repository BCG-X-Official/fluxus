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
Implementation of class ``LabelingProducer``.

The ``LabelingProducer`` and ``LabelingTransformer`` classes are designed to add
labels to the products produced by a producer or transformer. These classes are
subclasses of the ``Producer`` and ``Transformer`` classes, respectively, as well as the
``_Delegator`` class, which delegates attribute access to a wrapped object, unless the
dictionary of the wrapped object includes the attribute.
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Iterator, Mapping
from typing import Any, Generic, TypeVar, cast

from pytools.api import inheritdoc, subsdoc

from .._producer import Producer
from .._transformer import Transformer
from ..core import SerialSource
from ._lineage import HasLineage

log = logging.getLogger(__name__)

__all__ = [
    "LabelingProducer",
    "LabelingTransformer",
]


#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions

T_Labeler_ret = TypeVar("T_Labeler_ret", bound="_Labeler[Any]", covariant=True)
T_LabelingProducer_ret = TypeVar(
    "T_LabelingProducer_ret", bound="LabelingProducer[Any]"
)
T_LabelingTransformer_ret = TypeVar(
    "T_LabelingTransformer_ret", bound="LabelingTransformer[Any, Any]"
)
T_Product_ret = TypeVar("T_Product_ret", bound=HasLineage[Any], covariant=True)
T_Source = TypeVar("T_Source", bound=SerialSource[Any])
T_SourceProduct_arg = TypeVar("T_SourceProduct_arg", contravariant=True)
T_TransformedProduct_ret = TypeVar(
    "T_TransformedProduct_ret", bound=HasLineage[Any], covariant=True
)

#
# Constants
#

# Sentinel value for attribute not found
_NOT_FOUND = object()


#
# Classes
#


class _Labeler(SerialSource[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta):
    """
    A mixin class, adding the ``label`` method to a producer or transformer to allow
    adding labels to the products.
    """

    @abstractmethod
    def label(self: T_Labeler_ret, **labels: Any) -> T_Labeler_ret:
        """
        Label the product of this source.

        :param labels: the labels to add
        :return: a new source instance that will add the given labels to all products
        """

    @subsdoc(
        pattern=r"(\n\n\s*:return:)",
        replacement=(
            r"\n\nIncludes labels added to this conduit, prefixing their names "
            r"with a '#' character.\1"
        ),
        using=SerialSource.get_repr_attributes,
    )
    def get_repr_attributes(self) -> Mapping[str, Any]:
        """[see superclass]"""
        return super().get_repr_attributes()


class LabelingProducer(
    Producer[T_Product_ret],
    _Labeler[T_Product_ret],
    Generic[T_Product_ret],
    metaclass=ABCMeta,
):
    """
    A producer that can label their products.

    Implements the :meth:`.label` method, which defines labels that the producer will
    add to all products.

    Products must implement the :class:`.HasLineage` interface.

    As an example, consider a producer of numbers:

    .. code-block:: python

        class Number(HasLineage):
            # Implementation goes here

        class NumberProducer(Producer[Number], Labeler[A]):
            \"""A producer\"""

            def iter(self) -> Iterator[int]:
                for i in range(3):
                    yield Number(i)

    The producer can be labeled with an arbitrary number of labels:

    .. code-block:: python

        producer = NumberProducer().label(a="A", b="B")

        for product in producer.iter():
            print(product.product_labels)

    The output confirms that the labels have been added to all products:

    .. code-block:: python

        {'a': 'A', 'b': 'B'}
        {'a': 'A', 'b': 'B'}
        {'a': 'A', 'b': 'B'}

    """

    def label(self: T_LabelingProducer_ret, **labels: Any) -> T_LabelingProducer_ret:
        """
        Label the product of this producer.

        :param labels: the labels to add
        :return: a new version of this producer that will add the given labels to all
            products
        """

        return cast(
            # We cast the producer wrapper to the type of the delegate
            T_LabelingProducer_ret,
            _LabeledProducer(self, labels),
        )


class LabelingTransformer(
    Transformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    _Labeler[T_TransformedProduct_ret],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
    metaclass=ABCMeta,
):
    """
    A transformer that can label its products.

    Implements the :meth:`.label` method, which defines labels that the transformer will
    add to all products.

    Products must implement the :class:`.HasLineage` interface.

    As an example, consider a transformer that doubles numbers:

    .. code-block:: python

            class Number(HasLineage):
                # Implementation goes here

            class NumberDoubler(Transformer[Number, Number], Labeler[A]):
                \"""A transformer that doubles numbers\"""

                def transform(self, source_product: Number) -> Iterator[Number]:
                    yield Number(source_product.value * 2)

    The transformer can be labeled with an arbitrary number of labels:

    .. code-block:: python

            transformer = NumberDoubler().label(a="A", b="B")

            for product in transformer.run([Number(1), Number(2)]):
                print(product.product_labels)

    The output confirms that the labels have been added to all products:

    .. code-block:: python

        {'a': 'A', 'b': 'B'}
        {'a': 'A', 'b': 'B'}
    """

    def label(
        self: T_LabelingTransformer_ret, **labels: Any
    ) -> T_LabelingTransformer_ret:
        """
        Label the product of this transformer.

        :param labels: the labels to add
        :return: a new transformer that will add the given labels to all products
        """
        return cast(
            # We cast the transformer wrapper to the type of the delegate
            T_LabelingTransformer_ret,
            _LabeledTransformer(self, labels),
        )


@inheritdoc(match="""[see superclass]""")
class _LabeledSource(_Labeler[Any], Generic[T_Source], metaclass=ABCMeta):
    """
    A producer or transformer with added labels. Delegates attribute access to a wrapped
    ``LabelingProducer`` or ``LabelingTransformer``, except for a small number of
    methods that this wrapper class overrides to add labels to the products.
    """

    _delegate: T_Source
    _labels: dict[str, Any]

    def __init__(self, delegate: T_Source, labels: dict[str, Any]) -> None:
        """
        :param delegate: the producer to delegate to
        :param labels: the labels to add to the products
        """
        self._delegate = delegate
        self._labels = labels

    def __getattribute__(self, name: str) -> Any:
        # We won't delegate access to protected attributes
        __getattribute__original = object.__getattribute__
        if name.startswith("_"):
            return __getattribute__original(self, name)
        # First, check if the attribute is in the local dictionary (excluding
        # subclasses)
        local_attributes = __getattribute__original(self, "__dict__")
        value = local_attributes.get(name, _NOT_FOUND)
        if value is _NOT_FOUND:
            # If the attribute is not in the local dictionary, check if it is in the
            # class dictionary
            mro = __getattribute__original(self, "__class__").mro()
            for cls in mro:
                if name in cls.__dict__:
                    # If it is, use the default behavior to get the attribute
                    return __getattribute__original(self, name)
                if cls is _LabeledSource:
                    # If we reach the _LabeledSource class, we stop searching
                    break
            # If the attribute is not in the relevant class dictionaries, delegate to
            # the wrapped object
            return __getattribute__original(local_attributes.get("_delegate"), name)
        else:
            # Otherwise, return the value
            return value

    def get_repr_attributes(self) -> Mapping[str, Any]:
        """[see superclass]"""
        return dict(self._delegate.get_repr_attributes()) | {
            f"#{name}": value for name, value in self._labels.items()
        }

    def label(self: T_Labeler_ret, **labels: Any) -> T_Labeler_ret:
        """
        Label the product of this source.

        :param labels: the labels to add
        :return: a new source instance that will add the given labels to all products
        """
        # Combine the existing and new labels
        cast(_LabeledSource[T_Source], self).__labels.update(labels)
        # We return self to allow method chaining
        return self


@inheritdoc(match="[see superclass]")
class _LabeledProducer(
    LabelingProducer[T_Product_ret],
    _LabeledSource[Producer[T_Product_ret]],
    Generic[T_Product_ret],
):
    """
    A producer that labels its products.
    """

    @property
    def product_type(self) -> type[T_Product_ret]:
        """[see superclass]"""
        # Delegate to the wrapped object by invoking the property on the
        # _Delegator class
        return self._delegate.product_type

    def produce(self) -> Iterator[T_Product_ret]:
        """[see superclass]"""
        for product in self._delegate.produce():
            yield product.label(**self._labels)

    async def aproduce(self) -> AsyncIterator[T_Product_ret]:
        """[see superclass]"""
        async for product in self._delegate.aproduce():
            yield product.label(**self._labels)


@inheritdoc(match="[see superclass]")
class _LabeledTransformer(
    LabelingTransformer[T_SourceProduct_arg, T_TransformedProduct_ret],
    _LabeledSource[LabelingTransformer[T_SourceProduct_arg, T_TransformedProduct_ret]],
    Generic[T_SourceProduct_arg, T_TransformedProduct_ret],
):
    """
    A transformer that labels its products.
    """

    @property
    def input_type(self) -> type[T_SourceProduct_arg]:
        """[see superclass]"""
        # Delegate to the wrapped object by invoking the property on the
        # _Delegator class
        return self._delegate.input_type

    @property
    def product_type(self) -> type[T_TransformedProduct_ret]:
        """[see superclass]"""
        # Delegate to the wrapped object by invoking the property on the
        # _Delegator class
        return self._delegate.product_type

    def transform(
        self, source_product: T_SourceProduct_arg
    ) -> Iterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        for product in self._delegate.transform(source_product):
            yield product.label(**self._labels)

    async def atransform(
        self, source_product: T_SourceProduct_arg
    ) -> AsyncIterator[T_TransformedProduct_ret]:
        """[see superclass]"""
        async for product in super().atransform(source_product):
            yield product.label(**self._labels)
