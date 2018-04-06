.. _api:

API Documentation
=================

.. py:module:: lsm

.. autoclass:: LSM
    :members:
      __init__,
      open,
      close,
      page_size,
      block_size,
      multiple_processes,
      readonly,
      write_safety,
      autoflush,
      autowork,
      automerge,
      autocheckpoint,
      mmap,
      transaction_log,
      pages_written,
      pages_read,
      checkpoint_size,
      tree_size,
      __enter__,
      insert,
      update,
      fetch,
      fetch_bulk,
      fetch_range,
      delete,
      delete_range,
      __getitem__,
      __setitem__,
      __delitem__,
      __contains__,
      __iter__,
      __reversed__,
      keys,
      values,
      flush,
      work,
      checkpoint,
      begin,
      commit,
      rollback,
      transaction,
      cursor


.. autoclass:: Cursor
    :members:
      open,
      close,
      __enter__,
      __iter__,
      compare,
      seek,
      is_valid,
      first,
      last,
      next,
      previous,
      fetch_until,
      fetch_range,
      key,
      value,
      keys,
      values


.. autoclass:: Transaction
    :members:
      commit,
      rollback

Constants
---------

Seek methods, can be used when fetching records or slices.

``SEEK_EQ``
  The cursor is left at EOF (invalidated). A call to lsm_csr_valid()
  returns non-zero.

``SEEK_LE``
  The cursor is left pointing to the largest key in the database that
  is smaller than (pKey/nKey). If the database contains no keys smaller
  than (pKey/nKey), the cursor is left at EOF.

``SEEK_GE``
  The cursor is left pointing to the smallest key in the database that
  is larger than (pKey/nKey). If the database contains no keys larger
  than (pKey/nKey), the cursor is left at EOF.

If the fourth parameter is ``SEEK_LEFAST``, this function searches the
database in a similar manner to ``SEEK_LE``, with two differences:

Even if a key can be found (the cursor is not left at EOF), the
lsm_csr_value() function may not be used (attempts to do so return
LSM_MISUSE).

The key that the cursor is left pointing to may be one that has
been recently deleted from the database. In this case it is
guaranteed that the returned key is larger than any key currently
in the database that is less than or equal to (pKey/nKey).

``SEEK_LEFAST`` requests are intended to be used to allocate database
keys.

Used in calls to :py:meth:`LSM.set_safety`.

* ``SAFETY_OFF``
* ``SAFETY_NORMAL``
* ``SAFETY_FULL``
