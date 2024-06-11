"""
Products implementing the :class:`.HasLineage` interface are able to store and retrieve
the lineage of the intermediary products that were created along the way through the
flow to the final product.

Such products can also be tagged with additional labels to help categorize them in the
final set of products.
"""

from ._label import *
from ._lineage import *
