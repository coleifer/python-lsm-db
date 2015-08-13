.. _installation:

Installation
============

You can use ``pip`` to install ``lsm-db``:

.. code-block:: console

    pip install lsm-db

The project is hosted at https://github.com/coleifer/python-lsm-db and can be installed from source:

.. code-block:: console

    git clone https://github.com/coleifer/python-lsm-db
    cd lsm-db
    python setup.py build
    python setup.py install

.. note::
    ``lsm-db`` depends on `Cython <http://www.cython.org/>`_ to generate the Python extension. By default, lsm-db ships with a pre-generated C source file, so it is not strictly necessary to install Cython in order to compile ``lsm-db``, but you may wish to install Cython to ensure the generated source is compatible with your setup.

After installing lsm-db, you can run the unit tests by executing the ``tests`` module:

.. code-block:: console

    python tests.py
