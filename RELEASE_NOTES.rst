Release Notes
=============

*fluxus* 1.1
-------------

*fluxus* 1.1 introduces the concept of `flow controls` that allow the contstruction
of more complex, non-linear workflows.

*fluxus* 1.1.0
~~~~~~~~~~~~~~

- API: New abstract class :class:`.FlowControl` for flow controls. Flow controls can
  be applied to conduits or composite conduits using the ``@`` operator
- API: New :class:`.Repeat` flow control that repeats the execution of a sub-flow until
  a condition is met
- API: New function :func:`.repeat` added to the functional API to repeat one or more
  steps until a condition is met


*fluxus* 1.0
------------

*fluxus* 1.0.1
~~~~~~~~~~~~~~

- BUILD: Add backward compatibility for Python 3.10.


*fluxus* 1.0.0
~~~~~~~~~~~~~~

- Initial release of *fluxus*.