.. _faq:

FAQ
===

.. contents::
   :local:
   :depth: 2

About the project
-----------------

What is *fluxus* for?
~~~~~~~~~~~~~~~~~~~~~

*fluxus* is a Python framework designed by `BCG X <https://www.bcg.com/x>`_ to
streamline the development of complex data processing pipelines (called *flows*),
enabling users to quickly and efficiently build, test, and deploy highly concurrent
workflows, making complex operations more manageable.

**FLUXUS** is inspired by the data stream paradigm and is designed to be simple,
expressive, and composable.

Who developed *fluxus*, and why?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*fluxus* was developed by the Responsible AI team at
`BCG X <https://www.bcg.com/beyond-ventures/bcg-x>`_, primarily to provide a scalable
and efficient way to rapidly stand up highly concurrent red teaming workloads for
GenAI testing and evaluation.

Given that other use cases for *fluxus* are likely to emerge, we decided to publish
the flow management portion of the codebase as a a separate open-source project.

What is the origin of the name *fluxus*?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The name *fluxus* is derived from the Latin word for "flow" or "stream." The name
was chosen to reflect the project's focus on data streams and the flow of data
through a pipeline.

How does *fluxus* differ from other pipelining/workflow libraries?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*fluxus* is designed to be simple, expressive, and composable. It is built on top
of the `asyncio <https://docs.python.org/3/library/asyncio.html>`_ library, which
provides a powerful and flexible way to write concurrent code in Python.

*fluxus* is also designed to be highly extensible, allowing users to easily add
new components and customize existing ones. It provides both a functional API for
quickly and intuitively building flows using dictionaries as their primary data
structures, as well as a class-based API for more complex flows that require
custom data types.

Finally, *fluxus* is designed to be lightweight and efficient, managing the complexities
of concurrency and parallelism behind the scenes so that users can focus on building
their pipelines without worrying about the underlying implementation details.

What are examples of use cases for *fluxus*?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*fluxus* is designed to be a general-purpose framework for building data processing
pipelines, so it can be used in a wide variety of applications.  It is particularly
powerful for building highly concurrent workflows that make heavy use of I/O-bound
operations, such as network requests, file I/O, and database queries.

Some examples of use cases for *fluxus* include:

- Real-time data processing
- ETL (Extract, Transform, Load) pipelines
- Machine learning workflows
- Data extraction
- Data aggregation and analysis
- Red teaming and security testing
