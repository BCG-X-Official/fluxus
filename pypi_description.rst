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

.. Begin-Badges

|pypi| |conda| |python_versions| |code_style| |made_with_sphinx_doc| |License_badge|

.. End-Badges

License
---------------------------

*fluxus* is licensed under Apache 2.0 as described in the
`LICENSE <https://github.com/BCG-X-Official/fluxus/blob/develop/LICENSE>`_ file.

.. |conda| image:: https://anaconda.org/bcg_gamma/fluxus/badges/version.svg
    :target: https://anaconda.org/BCGX/fluxus

.. |pypi| image:: https://badge.fury.io/py/fluxus.svg
    :target: https://pypi.org/project/fluxus/

.. |python_versions| image:: https://img.shields.io/badge/python-3.7|3.8|3.9-blue.svg
    :target: https://www.python.org/downloads/release/python-380/

.. |code_style| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. |made_with_sphinx_doc| image:: https://img.shields.io/badge/Made%20with-Sphinx-1f425f.svg
    :target: https://bcg-x-official.github.io/pytools/index.html

.. |license_badge| image:: https://img.shields.io/badge/License-Apache%202.0-olivegreen.svg
    :target: https://opensource.org/licenses/Apache-2.0
    