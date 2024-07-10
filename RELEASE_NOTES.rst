Release Notes
=============

*fluxus* 1.1
------------

*fluxus* 1.1.0
~~~~~~~~~~~~~~

- API: Removed functions `iter()` and `aiter()` from class :class:`.SerialTransformer`,
  to further streamline the API and given they can be easily replaced by repeated calls
  to :meth:`.SerialTransformer.transform` and :meth:`.SerialTransformer.atransform`.


*fluxus* 1.0
------------

*fluxus* 1.0.2
~~~~~~~~~~~~~~

- FIX: Allow asynchronous step functions to return iterators and asynchronous iterators.


*fluxus* 1.0.1
~~~~~~~~~~~~~~

- BUILD: Add backward compatibility for Python 3.10.


*fluxus* 1.0.0
~~~~~~~~~~~~~~

- Initial release of *fluxus*.