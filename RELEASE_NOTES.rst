Release Notes
=============

*fluxus* 1.0
------------

*fluxus* 1.0.3
~~~~~~~~~~~~~~

- API: Rename methods ``iter_concurrent_conduits`` to ``iter_concurrent_producers``,
  and ``aiter_concurrent_conduits`` to ``aiter_concurrent_producers``
- API: Return iterators not lists from :meth:`.SerialTransformer.process` and
  :meth:`.SerialTransformer.aprocess` for greater flexibility in processing the results
- API: Removed functions ``iter()`` and ``aiter()`` from class
  :class:`.SerialTransformer`, to further streamline the API and given they can be
  easily replaced by repeated calls to :meth:`.SerialTransformer.process` and
  :meth:`.SerialTransformer.aprocess`
- FIX: Updated logic for iterating over concurrent producers and transformers to ensure
  shared conduits never run more than once


*fluxus* 1.0.2
~~~~~~~~~~~~~~

- FIX: Allow asynchronous step functions to return iterators and asynchronous iterators.


*fluxus* 1.0.1
~~~~~~~~~~~~~~

- BUILD: Add backward compatibility for Python 3.10.


*fluxus* 1.0.0
~~~~~~~~~~~~~~

- Initial release of *fluxus*.