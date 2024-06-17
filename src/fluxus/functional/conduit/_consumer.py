"""
Implementation of ``DictConsumer``.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import AsyncIterable
from typing import Any, cast

from pytools.api import inheritdoc
from pytools.expression.repr import ListWithExpressionRepr

from ... import AsyncConsumer
from .._result import RunResult
from ..product import DictProduct
from ._conduit import DictConduit

log = logging.getLogger(__name__)

__all__ = [
    "DictConsumer",
]


#
# Classes
#


@inheritdoc(match="""[see superclass]""")
class DictConsumer(DictConduit, AsyncConsumer[DictProduct, RunResult]):
    """
    A consumer of dictionary products.

    Combines all dictionaries into one or more lists of dictionaries.

    If the ingoing flow is sequential, the output will be a list of dictionaries.

    If the ingoing flow is concurrent, the output will be a list of lists of
    dictionaries, where each list corresponds to one distinct path through the
    flow.
    """

    #: If ``True``, include start and end timestamps for each step in the lineage
    #: attributes; if ``False``, do not include timestamps.
    timestamps: bool

    def __init__(self, *, name: str = "output", timestamps: bool = False) -> None:
        """
        :param name: the name of this consumer
        :param timestamps: if ``True``, include start and end timestamps for each step
            in the lineage attributes; if ``False``, do not include timestamps
        """
        super().__init__(name=name)
        self.timestamps = timestamps

    async def aconsume(
        self, products: AsyncIterable[tuple[int, DictProduct]]
    ) -> RunResult:
        """[see superclass]"""

        lineage_by_group: dict[int, list[dict[str, dict[str, Any]]]] = defaultdict(
            ListWithExpressionRepr
        )

        # Get the timestamps flag
        timestamps = self.timestamps

        # Initialize summary statistics
        run_start = time.perf_counter()
        cumulative_time = 0.0

        async for group, end_product in products:
            # Get the lineage attributes for the end product, which is a dictionary
            # of dictionaries. The outer dictionary maps product names to their
            # attributes, and the inner dictionaries map attribute names to their
            # values.
            attributes = end_product.get_lineage_attributes()

            # Iterate over products and their attribute dicts
            for product, product_attributes in zip(
                cast(list[DictProduct], end_product.get_lineage()), attributes.values()
            ):

                # Get the time stamps from the product
                start_time = product.start_time
                end_time = product.end_time

                # Update summary statistics
                cumulative_time += end_time - start_time

                if timestamps:
                    # Add the time stamps to the attributes
                    product_attributes[DictProduct.KEY_START_TIME] = (
                        start_time - run_start
                    )
                    product_attributes[DictProduct.KEY_END_TIME] = end_time - run_start

            lineage_by_group[group].append(attributes)

        # Log a summary message

        run_duration = time.perf_counter() - run_start
        summary_message = (
            f"Run took {run_duration :g} seconds, with {cumulative_time:g} seconds "
            "of total wait time."
        )
        if run_duration > 0:
            speedup = cumulative_time / run_duration
            summary_message += (
                f" Concurrent execution achieved a speedup factor of {speedup:g} "
                f"over sequential execution."
            )
        log.info(summary_message)

        # Return the lineage by group
        n_groups = max(lineage_by_group.keys()) + 1

        if n_groups == 1:
            # If there is only one group, return the lineage for that group as a list
            return RunResult(lineage_by_group[0])
        else:
            # If there are multiple groups, return the lineage for each group in a
            # separate list
            return RunResult(*(lineage_by_group[group] for group in range(n_groups)))
