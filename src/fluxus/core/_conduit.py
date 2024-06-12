"""
Implementation of conduit and subconduit base classes
"""

from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, Collection, Iterator, Mapping
from typing import Any, Generic, Self, TypeVar, final

from pytools.api import get_init_params, inheritdoc
from pytools.expression import (
    Expression,
    HasExpressionRepr,
    expression_from_init_params,
)
from pytools.expression.atomic import Id
from pytools.typing import get_type_arguments

from ..util import simplify_repr_attributes

log = logging.getLogger(__name__)

__all__ = [
    "AtomicConduit",
    "Conduit",
    "SerialConduit",
]

#
# Type variables
#
# Naming convention used here:
# _ret for covariant type variables used in return positions
# _arg for contravariant type variables used in argument positions
#


T_Product_ret = TypeVar("T_Product_ret", covariant=True)
T_Output_ret = TypeVar("T_Output_ret", covariant=True)

#
# Classes
#


class Conduit(HasExpressionRepr, Generic[T_Output_ret], metaclass=ABCMeta):
    """
    An element of a flow, which can be a producer, a transformer, a consumer,
    or a sequential or concurrent composition of these.
    """

    @property
    def name(self) -> str:
        """
        The name of this conduit.
        """
        return type(self).__name__

    @property
    @abstractmethod
    def is_chained(self) -> bool:
        """
        ``True`` if this conduit contains a composition of conduits chained together
        sequentially, ``False`` otherwise.
        """

    @property
    @abstractmethod
    def is_concurrent(self) -> bool:
        """
        ``True`` if this conduit is a group of concurrent conduits, ``False``
        otherwise.
        """

    @property
    @final
    def is_atomic(self) -> bool:
        """
        ``True`` if this conduit is atomic, ``False`` if it is a chained or concurrent
        composition of other conduits.
        """
        return not (self.is_chained or self.is_concurrent)

    @property
    @abstractmethod
    def final_conduit(self) -> Conduit[T_Output_ret]:
        """
        The final conduit if this conduit is a sequential composition of conduits;
        ``self`` if this conduit is atomic or a group of concurrent conduits.
        """

    @abstractmethod
    def get_final_conduits(self) -> Iterator[SerialConduit[T_Output_ret]]:
        """
        Get an iterator yielding the final atomic conduit or conduits of the (sub)flow
        represented by this conduit.

        If this conduit is atomic, yields the conduit itself.

        If this conduit is a sequential composition of conduits, yields the final
        conduit of that sequence.

        If this conduit is a group of concurrent conduits, yields the final conduits of
        each of the concurrent conduits.

        :return: an iterator yielding the final conduits
        """

    @property
    @abstractmethod
    def n_concurrent_conduits(self) -> int:
        """
        The number of concurrent conduits in this conduit.
        """

    @abstractmethod
    def iter_concurrent_conduits(self) -> Iterator[SerialConduit[T_Output_ret]]:
        """
        Iterate over the concurrent conduits that make up this conduit.

        :return: an iterator over the concurrent conduits
        """

    async def aiter_concurrent_conduits(
        self,
    ) -> AsyncIterator[SerialConduit[T_Output_ret]]:
        """
        Asynchronously iterate over the concurrent conduits that make up this conduit.

        :return: an asynchronous iterator over the concurrent conduits
        """
        for conduit in self.iter_concurrent_conduits():
            yield conduit

    def draw(self, style: str = "graph") -> None:
        """
        Draw the flow.

        :param style: the style to use for drawing the flow, see :class:`.FlowDrawer`
            for available styles (defaults to "graph")
        """
        from ..viz import FlowDrawer

        FlowDrawer(style=style).draw(self, title="Flow")

    @property
    def _has_passthrough(self) -> bool:
        """
        ``True`` if this conduit contains a passthrough, ``False`` otherwise.
        """
        return False

    @abstractmethod
    def get_connections(
        self, *, ingoing: Collection[SerialConduit[Any]]
    ) -> Iterator[tuple[SerialConduit[Any], SerialConduit[Any]]]:
        """
        Get the connections between conduits in this conduit.

        :param ingoing: the ingoing conduits, if any
        :return: an iterator yielding connections between conduits
        """

    def _repr_svg_(self) -> str:  # pragma: no cover
        """
        Get the SVG representation of the flow.

        :return: the SVG representation
        """
        # create a Bytes buffer to write the SVG to
        from io import BytesIO

        svg = BytesIO()

        from ..viz import FlowDrawer, FlowGraphStyle

        FlowDrawer(style=FlowGraphStyle(file=svg, format="svg")).draw(
            self, title="Flow"
        )
        return svg.getvalue().decode("utf-8")

    def _repr_html_(self) -> str:  # pragma: no cover
        """[see superclass]"""

        try:
            import graphviz  # noqa F401
        except ImportError:
            # Graphviz is not available, so we keep the default representation
            return super()._repr_html_()
        else:
            # Graphviz is available, so we can add the SVG representation
            return self._repr_svg_()

    @abstractmethod
    def to_expression(self, *, compact: bool = False) -> Expression:
        """
        Make an expression representing this conduit.

        :param compact: if ``True``, use a compact representation using only the subset
            of conduit attributes from :meth:`.get_repr_attributes`; if ``False``,
            generate the full representation using all attributes
        :return: the expression representing this conduit
        """

    def _get_type_arguments(self, base: type) -> tuple[type, ...]:
        """
        Get the type arguments of this conduit with respect to the given base class.

        :param base: the base class to get the type arguments for
        :return: the type arguments
        :raises TypeError: if the type arguments are ambiguous due to multiple
            inheritance
        """
        args = get_type_arguments(self, base)
        if len(args) > 1:
            raise TypeError(
                f"Ambiguous type arguments for {self.name} with respect to "
                f"base class {base.__name__}: " + ", ".join(map(str, args))
            )
        return args[0]

    def __str__(self) -> str:
        """[see superclass]"""
        return str(self.to_expression(compact=True))


@inheritdoc(match="[see superclass]")
class SerialConduit(Conduit[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta):
    """
    A conduit that is either atomic, or a sequential composition of conduits.
    """

    @property
    @final
    def is_concurrent(self) -> bool:
        """
        ``False``, since this is a serial conduit and therefore is not made up of
        concurrent conduits.
        """
        return False

    @property
    def final_conduit(self) -> SerialConduit[T_Product_ret]:
        """
        The final conduit if this conduit is a sequential composition of conduits;
        ``self`` if this conduit is atomic.
        """
        return next(self.get_final_conduits())

    @abstractmethod
    def get_final_conduits(self) -> Iterator[SerialConduit[T_Product_ret]]:
        """[see superclass]"""

    @property
    def n_concurrent_conduits(self) -> int:
        """
        The number of concurrent conduits in this conduit. Returns `1`, since a serial
        conduit is not made up of concurrent conduits.
        """
        return 1

    def iter_concurrent_conduits(self) -> Iterator[Self]:
        """
        Yields ``self``, since this is a serial conduit and is not made up of concurrent
        conduits.

        :return: an iterator with ``self`` as the only element
        """
        yield self

    async def aiter_concurrent_conduits(self: Self) -> AsyncIterator[Self]:
        """
        Yields ``self``, since this is a serial conduit and is not made up of concurrent
        conduits.

        :return: an asynchronous iterator with ``self`` as the only element
        """
        yield self

    @property
    def chained_conduits(self) -> Iterator[SerialConduit[T_Product_ret]]:
        """
        An iterator yielding the chained conduits that make up this conduit, starting
        with the initial conduit and ending with the final conduit.

        For atomic conduit, yields the conduit itself.
        """
        yield self.final_conduit

    def get_repr_attributes(self) -> Mapping[str, Any]:
        """
        Get attributes of this conduit to be used in representations.

        :return: a dictionary mapping attribute names to their values
        """

        return get_init_params(self, ignore_default=True, ignore_missing=True)

    def to_expression(self, *, compact: bool = False) -> Expression:
        """[see superclass]"""
        if compact:
            return Id(self.name)(**simplify_repr_attributes(self.get_repr_attributes()))
        else:
            return expression_from_init_params(self)


@inheritdoc(match="[see superclass]")
class AtomicConduit(
    SerialConduit[T_Product_ret], Generic[T_Product_ret], metaclass=ABCMeta
):
    """
    An atomic conduit that is not a composition of other conduits.
    """

    @property
    @final
    def is_chained(self) -> bool:
        """
        ``False``, since this is an atomic conduit and is not a composition of multiple
        conduits.
        """
        return False

    @property
    @final
    def final_conduit(self) -> Self:
        """
        ``self``, since this is an atomic conduit and has no final conduit on a more
        granular level.
        """
        return self

    @final
    def get_final_conduits(self) -> Iterator[Self]:
        """[see superclass]"""
        yield self
