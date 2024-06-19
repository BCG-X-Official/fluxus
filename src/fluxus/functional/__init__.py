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
Functional API for the flow module.

The :mod:`artkit.flow` module provides a functional API for defining and executing
complex and asynchronous workflows. The API is inspired by the functional programming
paradigm and is designed to be simple, expressive, and composable.

The functions :func:`step`, :func:`chain`, and :func:`parallel` are used to
define the steps of a flow. Function :func:`step` defines a single step, function
:func:`chain` composes steps or flows sequentially, and the :func:`parallel` composes
steps or flows in parallel.

Alternatively, the ``>>`` operator can be used to chain steps or flows, and the
``&`` operator can be used to compose steps or flows in parallel.

Function :func:`passthrough` can be included in a parallel composition to pass the
unchanged input through to the next step.

Function :func:`run` is used to execute a flow. The flow is executed asynchronously,
and all results are returned as a :class:`RunResult` instance. Run results include
the input to the flow as well as all intermediate and final results along all paths
through the flow.

Example:

.. code-block:: python

    from artkit.flow import step, chain, parallel, passthrough, run

    def add_one(x) -> dict[str, Any]:
        # We can return a single dict
        return dict(x=x + 1)

    async def add_two(x) -> dict[str, Any]:
        # We can make the function asynchronous
        return dict(x=x + 2)

    async def add_three_or_five(x) -> AsyncIterator[dict[str, Any]]:
        # We can return an iterator to generate multiple values
        yield dict(x=x + 3)

    flow = chain(
        step("add_one", add_one),
        parallel(
            step("add_two", add_two),
            step("add_three", add_three),
            passthrough(),
        ),
    )

    result = run(flow, input=[dict(x=1), dict(x=10)])

    print(result)

This will output:

.. code-block:: python

    RunResult(
        [
            {'input': {'x': 1}, 'add_one': {'x': 2}, 'add_two': {'x': 4}},
            {'input': {'x': 10}, 'add_one': {'x': 11}, 'add_two': {'x': 13}}
        ],
        [
            {'input': {'x': 1}, 'add_one': {'x': 2}, 'add_three': {'x': 5}},
            {'input': {'x': 10}, 'add_one': {'x': 11}, 'add_three': {'x': 14}}
        ],
        [
            {'input': {'x': 1}, 'add_one': {'x': 2}},
            {'input': {'x': 10}, 'add_one': {'x': 11}}
        ]
    )
"""

from ._functions import *
from ._result import *
