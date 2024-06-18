"""
Conduits at the core of the functional flow API.

The conduits are:

- :class:`.DictProducer`
    A producer of dictionary products, as the starting point of a
    functional flow.
- :class:`.DictConsumer`
    A consumer of dictionary products, combining all produced
    dictionaries into a list of dictionaries.
- :class:`.Step`
    A step that transforms a dictionary product into one or more
    dictionary products.
"""

from ._consumer import *
from ._producer import *
from ._step import *
