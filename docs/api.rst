.. _api:

API Documentation
=================

.. py:module:: lsm

.. autoclass:: LSM
    :members:
      __init__,
      open,
      close,
      autoflush,
      set_page_size,
      set_safety,
      set_block_size,
      config_mmap,
      set_automerge,
      set_multiple_processes,
      set_auto_checkpoint,
      set_readonly,
      pages_written,
      pages_read,
      checkpoint_size,
      __enter__,
      insert,
      update,
      fetch,
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
