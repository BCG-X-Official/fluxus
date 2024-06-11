The following is commented out in RST format:

.. comment .. image:: sphinx/source/_images/ARTKIT_Logo_Light_RGB.png
   :alt: ARTKIT logo
   :width: 200px

FLUXUS
======

**FLUXUS** is a Python framework designed by `BCG X <https://www.bcg.com/x>`_ to
streamline the development of complex data processing pipelines (called *flows*),
enabling users to quickly and efficiently build, test, and deploy data workflows.

Key features include:

- **Modular Design:** Build complex data processing pipelines by composing simple,
  reusable components into flows structured as directed acyclic graphs (DAGs).
- **Efficient Execution:** Leverage asynchronous processing to speed up execution and
  manage costs of pipelines that rely heavily on remote API calls.
- **Customizable:** Develop custom components to connect to any data service and define
  custom pipeline steps to process data in any way you need.
- **Functional API**: Build flows using a simplified functional API that abstracts away
  the underlying object-oriented implementation details, using just a few simple
  functions.

Getting started
---------------

- See the `FLUXUS Documentation <#>`_ for a comprehensive User Guide, Examples,
  API reference, and more.
- See `Contributing <CONTRIBUTING.md>`_ or visit our detailed `Contributor Guide <#>`_
  for information on contributing.
- We have an `FAQ <#>`_ for common questions. For anything else, please reach out to
  ARTKIT@bcg.com.

.. _Introduction:


Introduction
------------

…


User Installation
-----------------

Install using ``pip``:

::

    pip install fluxus

or ``conda``:

::

    conda install -c bcg_x fluxus


Optional dependencies
^^^^^^^^^^^^^^^^^^^^^

To enable visualizations of flow diagrams, install `GraphViz <https://graphviz.org/>`_
and ensure it is in your system's PATH variable:

- For MacOS and Linux users, instructions provided on `GraphViz Downloads <https://www.graphviz.org/download/>`_ automatically add GraphViz to your path.
- Windows users may need to manually add GraphViz to your PATH (see `Simplified Windows installation procedure <https://forum.graphviz.org/t/new-simplified-installation-procedure-on-windows/224>`_).
- Run ``dot -V`` in Terminal or Command Prompt to verify installation.


Environment Setup
-----------------

Virtual environment
^^^^^^^^^^^^^^^^^^^

We recommend working in a dedicated environment, e.g., using ``venv``:

::

    python -m venv fluxus
    source fluxus/bin/activate

or ``conda``:

::

    conda env create -f environment.yml
    conda activate fluxus



Quick Start
-----------

The core FLUXUS functions are:

2. ``step``: A single flow step which produces a dictionary or an iterable of dictionaries
3. ``chain``: A set of steps that run in sequence
4. ``parallel``: A set of steps that run in parallel
1. ``run``: Execute one or more pipeline steps

Below, we develop a simple example flow with the following steps:

1. …

To begin, import ``fluxus.functional``.

Next, define a few functions that will be used as flow steps.

ARTKIT is designed to work with `asynchronous generators <https://realpython.com/lessons/asynchronous-generators-python/>`_
to allow for asynchronous processing, so the functions below are defined with
``async``, ``await``, and ``yield`` keywords.


.. code-block:: python

    # A function that rephrases input prompts to have a specified tone
    async def rephrase_tone(prompt: str, tone: str, llm: ak.ChatModel):

    …




For a more thorough introduction to FLUXUS, please visit our `User Guide <#>`_ and `Examples <#>`_!


Contributing
------------

Contributions to ARTKIT are welcome and appreciated! Please see the `Contributing <CONTRIBUTING.md>`_ section for information.


License
-------

This project is under the Apache License 2.0, allowing free use, modification, and distribution with added protections against patent litigation. 
See the `LICENSE <LICENSE>`_ file for more details or visit `Apache 2.0 <https://www.apache.org/licenses/LICENSE-2.0>`_.
