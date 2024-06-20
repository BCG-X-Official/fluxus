# Contributing

We welcome and appreciate contributions to *fluxus*!

### Ways to contribute

There are many ways to contribute, including:

- Create issues for bugs or feature requests
- Participate in community discussions
- Address an open [issue](https://github.com/BCG-X-Official/fluxus/issues)
- Create tutorials
- Improve documentation
- Submit pull requests

Known opportunities:

- Add new model connectors
- Add or improve unit tests

We especially encourage contributions that integrate additional model providers and enhance our documentation.

### How to contribute

All contributions must be reviewed and merged by a member of the core *fluxus* team.

For detailed guidance on how to contribute to *fluxus*, please see the [Contributor Guide](https://bcg-x-official.github.io/fluxus/contributor_guide/index.html).

For major contributions, reach out to the *fluxus* team in advance (ARTKIT@bcg.com).

---

## Setup

### Pre-requisites

The basic requirements for developing this library are:

- [Python](https://www.python.org/downloads/) version 3.11.x or later
- [git](https://git-scm.com/downloads) for cloning and contributing to the library
- [pip](https://pip.pypa.io/en/stable/installation/) or [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) for installing and managing Python libraries

We recommend using an IDE such as [VS Code](https://code.visualstudio.com/) or [PyCharm](https://www.jetbrains.com/pycharm/).

### Clone git repository

Clone a local version of the library using HTTPS:

```
git clone https://github.com/BCG-X-Official/fluxus.git
```

or SSH:

```
git clone git@github.com:BCG-X-Official/fluxus.git
```

### Create virtual environment

From the project root, create a dedicated environment for your project using, e.g., `venv`:

```
python -m venv fluxus-env
```

and activate it with

```
source fluxus-env/bin/activate
```

on mac or `.\fluxus\scripts\Activate` on Windows.

### Enable import of local *fluxus* modules

We recommend installing the project locally in developer mode to enable importing local *fluxus* modules in
scripts or notebooks as if the library is installed, but with local changes immediately reflected.

To install *fluxus* in developer mode, run the following from your project root:

```
pip install -e ".[testing]"
```
This makes sure that dependencies for testing are installed along with the *fluxus* package.
As an alternative approach, you can add the folder `fluxus/src` to your `PYTHONPATH`, and this will
enable importing local *fluxus* modules into scripts or notebooks.

### Install dependencies

The following installations are required for full functionality.

#### GraphViz

[GraphViz](https://graphviz.org/) is required for generating pipeline flow diagrams. Install the library and ensure it is in your system's PATH variable:

- For MacOS and Linux users, simple instructions provided on [GraphViz Downloads](https://www.graphviz.org/download/) should automatically add GraphViz to your path
- Windows users may need to manually add GraphViz to your PATH (see [Simplified Windows installation procedure](https://forum.graphviz.org/t/new-simplified-installation-procedure-on-windows/224))
- Run `dot -V` in Terminal or Command Prompt to verify installation

#### Pandoc

Pandoc is required to render Jupyter Notebooks for building the sphinx documentation.

- MacOS users can install Pandoc with `brew install pandoc`
- Windows and Linux users should follow the [Pandoc installation](https://pandoc.org/installing.html) instructions for their system

### Install pre-commit hooks

This project uses [pre-commit hooks](https://pre-commit.com/) to automatically enforce uniform coding standards in commits:

```
pre-commit install
```

To execute the pre-commit hooks on demand, use `pre-commit run` from the command line.

### Run unit tests

This project uses [pytest](https://docs.pytest.org/en/8.0.x/) to support functional testing. To run the test suite:

```
pytest
```

To maintain high standard for test coverage, the testing pipeline is configured to require at least 90% test coverage of the codebase, otherwise `pytest` will exit with a failure status.

## Contribution Guidelines

Visit the [Contributor Guide](https://bcg-x-official.github.io/fluxus/contributor_guide/index.html) for detailed standards, best practices, and processes for contributors.
