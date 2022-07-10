DVC objects
===========

|PyPI| |Status| |Python Version| |License|

|Tests| |Codecov| |pre-commit| |Black|

.. |PyPI| image:: https://img.shields.io/pypi/v/dvc-objects.svg
   :target: https://pypi.org/project/dvc-objects/
   :alt: PyPI
.. |Status| image:: https://img.shields.io/pypi/status/dvc-objects.svg
   :target: https://pypi.org/project/dvc-objects/
   :alt: Status
.. |Python Version| image:: https://img.shields.io/pypi/pyversions/dvc-objects
   :target: https://pypi.org/project/dvc-objects
   :alt: Python Version
.. |License| image:: https://img.shields.io/pypi/l/dvc-objects
   :target: https://opensource.org/licenses/Apache-2.0
   :alt: License
.. |Tests| image:: https://github.com/iterative/dvc-objects/workflows/Tests/badge.svg
   :target: https://github.com/iterative/dvc-objects/actions?workflow=Tests
   :alt: Tests
.. |Codecov| image:: https://codecov.io/gh/iterative/dvc-objects/branch/main/graph/badge.svg
   :target: https://app.codecov.io/gh/iterative/dvc-objects
   :alt: Codecov
.. |pre-commit| image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit
.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Black


Features
--------

* serverless
* supports multiple storages (local, s3, gs, gdrive, ssh/sftp, etc)
* based on fsspec


Requirements
------------

Storage must support these operations:

* uploading
* downloading
* listing
* copying
* quasiatomic rename


Installation
------------

You can install *DVC objects* via pip_ from PyPI_:

.. code:: console

   $ pip install dvc-objects


Usage
-----

This is used in `dvc`_ and `dvc-data`_.

Contributing
------------

Contributions are very welcome.
To learn more, see the `Contributor Guide`_.


License
-------

Distributed under the terms of the `Apache 2.0 license`_,
*DVC objects* is free and open source software.


Issues
------

If you encounter any problems,
please `file an issue`_ along with a detailed description.


.. _Apache 2.0 license: https://opensource.org/licenses/Apache-2.0
.. _PyPI: https://pypi.org/
.. _file an issue: https://github.com/iterative/dvc-objects/issues
.. _pip: https://pip.pypa.io/
.. github-only
.. _Contributor Guide: CONTRIBUTING.rst
.. _dvc: https://github.com/iterative/dvc
.. _dvc-data: https://github.com/iterative/dvc-data
