"""
This module is designed to handle data flows in a pipeline-like manner. It provides a
set of classes that represent different components of a data flow, such as producers,
transformers, and consumers. These components can be combined to form complex data
processing pipelines.

Here's a brief overview of the main classes and their roles:

- :class:`.Conduit`
    This is an abstract base class that represents an element of a
    flow. It can be a producer, a transformer, a consumer, or a sequential or concurrent
    composition of these.
- :class:`.Producer`
    This class generates objects of a specific type that may be
    retrieved locally or remotely, or created dynamically. It can be run synchronously
    or asynchronously.
- :class:`.Transformer`
    This class generates new products from the products of a
    producer. It can be run synchronously or asynchronously.
- :class:`.Consumer`
    This class consumes products from a producer or group of
    producers, and returns a single object. It can be run synchronously or
    asynchronously.
- :class:`.Flow`
    This class represents a sequence of producers, transformers, and
    consumers that can be executed to produce a result.

The module also provides classes for concurrent groups of producers
(:class:`.ConcurrentProducer`) and transformers (:class:`.ConcurrentTransformer`), and
for handling asynchronous operations (:class:`.AsyncProducer`,
:class:`.AsyncTransformer`, :class:`.AsyncConsumer`).

The ``>>`` operator is overloaded in these classes to allow for easy chaining of
operations. For example, a producer can be connected to a transformer, which can then
be connected to a consumer, forming a complete data flow.

Groups of concurrent producers or transformers can be created using the ``&`` operator.

The :mod:`.core.flow` package is designed to be flexible and extensible, allowing for
complex data processing pipelines to be built with relative ease.
"""

from ._consumer import *
from ._flow import *
from ._passthrough import *
from ._producer import *
from ._transformer import *
from ._warning import *

__version__ = "1.0rc3"
