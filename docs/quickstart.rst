.. _quickstart:

Quick-start
===========

Below is a sample interactive console session designed to show some of the basic features and functionality of the ``lsm-db`` Python library. Also check out the :ref:`API documentation <api>`.

To begin, instantiate a :py:class:`lsm.LSM` object, specifying a path to a database file.

.. code-block:: pycon

    >>> from lsm import LSM
    >>> db = LSM('test.ldb')

Key/Value Features
------------------

``lsm-db`` is a key/value store, and has a dictionary-like API:

.. code-block:: pycon

    >>> db['foo'] = 'bar'
    >>> print db['foo']
    bar

    >>> for i in range(4):
    ...     db['k%s' % i] = str(i)
    ...

    >>> 'k3' in db
    True
    >>> 'k4' in db
    False

    >>> del db['k3']
    >>> db['k3']
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "lsm.pyx", line 973, in lsm.LSM.__getitem__ (lsm.c:7142)
      File "lsm.pyx", line 777, in lsm.LSM.fetch (lsm.c:5756)
      File "lsm.pyx", line 778, in lsm.LSM.fetch (lsm.c:5679)
      File "lsm.pyx", line 1289, in lsm.Cursor.seek (lsm.c:12122)
      File "lsm.pyx", line 1311, in lsm.Cursor.seek (lsm.c:12008)
    KeyError: 'k3'

By default when you attempt to look up a key, ``lsm-db`` will search for an exact match. You can also search for the closest key, if the specific key you are searching for does not exist:

.. code-block:: pycon

    >>> from lsm import SEEK_LE, SEEK_GE
    >>> db['k1xx', SEEK_LE]  # Here we will match "k1".
    '1'
    >>> db['k1xx', SEEK_GE]  # Here we will match "k2".
    '2'

:py:class:`LSM` supports other common dictionary methods such as:

* :py:meth:`~lsm.LSM.keys`
* :py:meth:`~lsm.LSM.values`
* :py:meth:`~lsm.LSM.update`

Slices and Iteration
--------------------

The database can be iterated through directly, or sliced. When you are slicing the database the start and end keys need not exist -- ``lsm-db`` will find the closest key (details can be found in the :py:meth:`~lsm.LSM.fetch` documentation).

.. code-block:: pycon

    >>> [item for item in db]
    [('foo', 'bar'), ('k0', '0'), ('k1', '1'), ('k2', '2')]

    >>> db['k0':'k99']
    <generator object at 0x7f2ae93072f8>

    >>> list(db['k0':'k99'])
    [('k0', '0'), ('k1', '1'), ('k2', '2')]

You can use open-ended slices. If the lower- or upper-bound is outside the range of keys an empty list is returned.

.. code-block:: pycon

    >>> list(db['k0':])
    [('k0', '0'), ('k1', '1'), ('k2', '2')]

    >>> list(db[:'k1'])
    [('foo', 'bar'), ('k0', '0'), ('k1', '1')]

    >>> list(db[:'aaa'])
    []

To retrieve keys in reverse order, simply use a higher key as the first parameter of your slice. If you are retrieving an open-ended slice, you can specify ``True`` as the ``step`` parameter of the slice.

.. code-block:: pycon

    >>> list(db['k1':'aaa'])  # Since 'k1' > 'aaa', keys are retrieved in reverse:
    [('k1', '1'), ('k0', '0'), ('foo', 'bar')]

    >>> list(db['k1'::True])  # Open-ended slices specify True for step:
    [('k1', '1'), ('k0', '0'), ('foo', 'bar')]

You can also **delete** slices of keys, but note that the delete **will not** include the keys themselves:

.. code-block:: pycon

    >>> del db['k0':'k99']

    >>> list(db)  # Note that 'k0' still exists.
    [('foo', 'bar'), ('k0', '0')]

Cursors
-------

While slicing may cover most use-cases, for finer-grained control you can use cursors for traversing records.

.. code-block:: pycon

    >>> with db.cursor() as cursor:
    ...     for key, value in cursor:
    ...         print key, '=>', value
    ...
    foo => bar
    k0 => 0

    >>> db.update({'k1': '1', 'k2': '2', 'k3': '3'})

    >>> with db.cursor() as cursor:
    ...     cursor.first()
    ...     print cursor.key()
    ...     cursor.last()
    ...     print cursor.key()
    ...     cursor.previous()
    ...     print cursor.key()
    ...
    foo
    k3
    k2

    >>> with db.cursor() as cursor:
    ...     cursor.seek('k0', SEEK_GE)
    ...     print list(cursor.fetch_until('k99'))
    ...
    [('k0', '0'), ('k1', '1'), ('k2', '2'), ('k3', '3')]

.. note::
    It is very important to close a cursor when you are through using it. For this reason, it is recommended you use the :py:meth:`~lsm.LSM.cursor` context-manager, which ensures the cursor is closed properly.

Transactions
------------

``lsm-db`` supports nested transactions. The simplest way to use transactions is with the :py:meth:`~lsm.LSM.transaction` method, which doubles as a context-manager or decorator.

.. code-block:: pycon

    >>> with db.transaction() as txn:
    ...     db['k1'] = '1-mod'
    ...     with db.transaction() as txn2:
    ...         db['k2'] = '2-mod'
    ...         txn2.rollback()
    ...
    True
    >>> print db['k1'], db['k2']
    1-mod 2

You can commit or roll-back transactions part-way through a wrapped block:

.. code-block:: pycon

    >>> with db.transaction() as txn:
    ...    db['k1'] = 'outer txn'
    ...    txn.commit()  # The write is preserved.
    ...
    ...    db['k1'] = 'outer txn-2'
    ...    with db.transaction() as txn2:
    ...        db['k1'] = 'inner-txn'  # This is commited after the block ends.
    ...    print db['k1']  # Prints "inner-txn".
    ...    txn.rollback()  # Rolls back both the changes from txn2 and the preceding write.
    ...    print db['k1']
    ...
    1              <- Return value from call to commit().
    inner-txn      <- Printed after end of txn2.
    True           <- Return value of call to rollback().
    outer txn      <- Printed after rollback.

If you like, you can also explicitly call :py:meth:`~lsm.LSM.begin`, :py:meth:`~lsm.LSM.commit`, and :py:meth:`~lsm.LSM.rollback`:

.. code-block:: pycon

    >>> db.begin()
    >>> db['foo'] = 'baze'
    >>> print db['foo']
    baze
    >>> db.rollback()
    True
    >>> print db['foo']
    bar
