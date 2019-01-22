.. lsm-db documentation master file, created by
   sphinx-quickstart on Mon Aug  3 01:29:51 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

python lsm-db
=============

.. image:: http://media.charlesleifer.com/blog/photos/lsm.png

Fast Python bindings for `SQLite's LSM key/value store <http://www.sqlite.org/src4/doc/trunk/www/lsmusr.wiki>`_.
The LSM storage engine was initially written as part of the experimental
SQLite4 rewrite (now abandoned). More recently, the LSM source code was moved
into the SQLite3 `source tree <https://www.sqlite.org/cgi/src/dir?ci=e148cdad35520e66&name=ext/lsm1>`_
and has seen some improvements and fixes. This project uses the LSM code from
the SQLite3 source tree.

Features:

* Embedded zero-conf database.
* Keys support in-order traversal using cursors.
* Transactional (including nested transactions).
* Single writer/multiple reader MVCC based transactional concurrency model.
* On-disk database stored in a single file.
* Data is durable in the face of application or power failure.
* Thread-safe.
* Python 2.x and 3.x.

Limitations:

* Not tested on Windoze.

The source for Python lsm-db is `hosted on GitHub <https://github.com/coleifer/python-lsm-db>`_.

.. note::
  If you encounter any bugs in the library, please `open an issue <https://github.com/coleifer/python-lsm-db/issues/new>`_, including a description of the bug and any related traceback.

Contents:

.. toctree::
   :maxdepth: 2
   :glob:

   installation
   quickstart
   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

