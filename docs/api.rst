.. _api:

API Documentation
=================

.. py:module:: lsm

.. autoclass:: LSM
    :members:
      __init__,
      open,
      close,
      fetch,
      fetch_range,
      insert,
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
      cursor,
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
      checkpoint_size
