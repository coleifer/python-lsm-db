# cython: language_level=3
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.version cimport PY_MAJOR_VERSION
import struct
import sys

try:
    from os import fsencode
except ImportError:
    try:
        from sys import getfilesystemencoding as _getfsencoding
    except ImportError:
        _fsencoding = 'utf-8'
    else:
        _fsencoding = _getfsencoding()
    fsencode = lambda s: s.encode(_fsencoding)


cdef extern from "src/lsm.h" nogil:
    ctypedef struct lsm_db
    ctypedef struct lsm_env
    ctypedef struct lsm_cursor
    ctypedef struct lsm_file

    ctypedef long long int lsm_i64

    cdef int LSM_LOCK_UNLOCK = 0
    cdef int LSM_LOCK_SHARED = 1
    cdef int LSM_LOCK_EXCL = 2

    cdef int LSM_OK = 0
    cdef int LSM_ERROR = 1
    cdef int LSM_BUSY = 5
    cdef int LSM_NOMEM = 7
    cdef int LSM_READONLY = 8
    cdef int LSM_IOERR = 10
    cdef int LSM_CORRUPT = 11
    cdef int LSM_FULL = 13
    cdef int LSM_CANTOPEN = 14
    cdef int LSM_PROTOCOL = 15
    cdef int LSM_MISUSE = 21
    cdef int LSM_MISMATCH = 50

    # Connections.
    cdef int lsm_new(lsm_env *env, lsm_db **ppDb)
    cdef int lsm_close(lsm_db *pDb)
    cdef int lsm_open(lsm_db *pDb, const char *zFilename)

    cdef int lsm_config(lsm_db *pDb, int verb, ...)

    cdef int LSM_CONFIG_AUTOFLUSH = 1
    cdef int LSM_CONFIG_PAGE_SIZE = 2
    cdef int LSM_CONFIG_SAFETY = 3
    cdef int LSM_CONFIG_BLOCK_SIZE = 4
    cdef int LSM_CONFIG_AUTOWORK = 5
    cdef int LSM_CONFIG_MMAP = 7
    cdef int LSM_CONFIG_USE_LOG = 8
    cdef int LSM_CONFIG_AUTOMERGE = 9
    cdef int LSM_CONFIG_MAX_FREELIST = 10
    cdef int LSM_CONFIG_MULTIPLE_PROCESSES = 11
    cdef int LSM_CONFIG_AUTOCHECKPOINT = 12
    cdef int LSM_CONFIG_SET_COMPRESSION = 13
    cdef int LSM_CONFIG_GET_COMPRESSION = 14
    cdef int LSM_CONFIG_SET_COMPRESSION_FACTORY = 15
    cdef int LSM_CONFIG_READONLY = 16

    cdef int LSM_SAFETY_OFF =0
    cdef int LSM_SAFETY_NORMAL =1
    cdef int LSM_SAFETY_FULL =2

    # Query for operational statistics.
    cdef int lsm_info(lsm_db *pDb, int verb, ...)

    cdef int LSM_INFO_NWRITE = 1
    cdef int LSM_INFO_NREAD = 2
    cdef int LSM_INFO_DB_STRUCTURE = 3
    cdef int LSM_INFO_LOG_STRUCTURE = 4
    cdef int LSM_INFO_ARRAY_STRUCTURE = 5
    cdef int LSM_INFO_PAGE_ASCII_DUMP = 6
    cdef int LSM_INFO_PAGE_HEX_DUMP = 7
    cdef int LSM_INFO_FREELIST = 8
    cdef int LSM_INFO_ARRAY_PAGES = 9
    cdef int LSM_INFO_CHECKPOINT_SIZE = 10
    cdef int LSM_INFO_TREE_SIZE = 11
    cdef int LSM_INFO_FREELIST_SIZE = 12
    cdef int LSM_INFO_COMPRESSION_ID = 13

    # Transactions.
    cdef int lsm_begin(lsm_db *pDb, int iLevel)
    cdef int lsm_commit(lsm_db *pDb, int iLevel)
    cdef int lsm_rollback(lsm_db *pDb, int iLevel)

    # Writing to the database.
    cdef int lsm_insert(lsm_db *pDb, const void *pKey, int nKey, const void *pVal, int nVal)
    cdef int lsm_delete(lsm_db *pDb, const void *pKey, int nKey)
    cdef int lsm_delete_range(lsm_db *pDb, const void *pKey, int nKey, const void *pKey2, int nKey2)

    cdef int lsm_work(lsm_db *pDb, int nMerge, int nKB, int *pnWrite)
    cdef int lsm_flush(lsm_db *pDb)
    cdef int lsm_checkpoint(lsm_db *pDb, int *pNumKBWritten)

    # Cursors.
    cdef int lsm_csr_open(lsm_db *pDb, lsm_cursor **ppCsr)
    cdef int lsm_csr_close(lsm_cursor *pCsr)

    # CAPI: Positioning Database Cursors
    #
    # If the fourth parameter is LSM_SEEK_EQ, LSM_SEEK_GE or LSM_SEEK_LE,
    # this function searches the database for an entry with key (pKey/nKey).
    # If an error occurs, an LSM error code is returned. Otherwise, LSM_OK.
    #
    # If no error occurs and the requested key is present in the database, the
    # cursor is left pointing to the entry with the specified key. Or, if the
    # specified key is not present in the database the state of the cursor
    # depends on the value passed as the final parameter, as follows:
    #
    # LSM_SEEK_EQ:
    #   The cursor is left at EOF (invalidated). A call to lsm_csr_valid()
    #   returns non-zero.
    #
    # LSM_SEEK_LE:
    #   The cursor is left pointing to the largest key in the database that
    #   is smaller than (pKey/nKey). If the database contains no keys smaller
    #   than (pKey/nKey), the cursor is left at EOF.
    #
    # LSM_SEEK_GE:
    #   The cursor is left pointing to the smallest key in the database that
    #   is larger than (pKey/nKey). If the database contains no keys larger
    #   than (pKey/nKey), the cursor is left at EOF.
    #
    # If the fourth parameter is LSM_SEEK_LEFAST, this function searches the
    # database in a similar manner to LSM_SEEK_LE, with two differences:
    #
    # Even if a key can be found (the cursor is not left at EOF), the
    # lsm_csr_value() function may not be used (attempts to do so return
    # LSM_MISUSE).
    #
    # The key that the cursor is left pointing to may be one that has
    # been recently deleted from the database. In this case it is
    # guaranteed that the returned key is larger than any key currently
    # in the database that is less than or equal to (pKey/nKey).
    #
    # LSM_SEEK_LEFAST requests are intended to be used to allocate database
    # keys.
    cdef int LSM_SEEK_LEFAST = -2
    cdef int LSM_SEEK_LE = -1
    cdef int LSM_SEEK_EQ = 0
    cdef int LSM_SEEK_GE = 1

    cdef int lsm_csr_seek(lsm_cursor *pCsr, const void *pKey, int nKey, int eSeek)

    cdef int lsm_csr_first(lsm_cursor *pCsr)
    cdef int lsm_csr_last(lsm_cursor *pCsr)

    # Advance the specified cursor to the next or previous key in the database.
    # Return LSM_OK if successful, or an LSM error code otherwise.
    #
    # Functions lsm_csr_seek(), lsm_csr_first() and lsm_csr_last() are "seek"
    # functions. Whether or not lsm_csr_next and lsm_csr_prev may be called
    # successfully also depends on the most recent seek function called on
    # the cursor. Specifically:
    #
    # At least one seek function must have been called on the cursor.
    # To call lsm_csr_next(), the most recent call to a seek function must
    # have been either lsm_csr_first() or a call to lsm_csr_seek() specifying
    # LSM_SEEK_GE.
    # To call lsm_csr_prev(), the most recent call to a seek function must
    # have been either lsm_csr_first() or a call to lsm_csr_seek() specifying
    # LSM_SEEK_GE.
    #
    # Otherwise, if the above conditions are not met when lsm_csr_next or
    # lsm_csr_prev is called, LSM_MISUSE is returned and the cursor position
    # remains unchanged.
    cdef int lsm_csr_next(lsm_cursor *pCsr)
    cdef int lsm_csr_prev(lsm_cursor *pCsr)

    cdef int lsm_csr_valid(lsm_cursor *pCsr)
    cdef int lsm_csr_key(lsm_cursor *pCsr, const void **ppKey, int *pnKey)
    cdef int lsm_csr_value(lsm_cursor *pCsr, const void **ppVal, int *pnVal)

    # If no error occurs, this function compares the database key passed via
    # the pKey/nKey arguments with the key that the cursor passed as the first
    # argument currently points to. If the cursors key is less than, equal to
    # or greater than pKey/nKey, *piRes is set to less than, equal to or greater
    # than zero before returning. LSM_OK is returned in this case.
    #
    # Or, if an error occurs, an LSM error code is returned and the final
    # value of *piRes is undefined. If the cursor does not point to a valid
    # key when this function is called, LSM_MISUSE is returned.
    cdef int lsm_csr_cmp(lsm_cursor *pCsr, const void *pKey, int nKey, int *piRes)


cdef dict EXC_MAPPING = {
    LSM_NOMEM: MemoryError,
    LSM_READONLY: IOError,
    LSM_IOERR: IOError,
    LSM_CORRUPT: IOError,
    LSM_FULL: IOError,
    LSM_CANTOPEN: IOError,
}

cdef dict EXC_MESSAGE_MAPPING = {
    LSM_ERROR: 'Error',
    LSM_BUSY: 'Busy',
    LSM_NOMEM: 'Out of memory',
    LSM_READONLY: 'Database is read-only',
    LSM_IOERR: 'Unspecified IO error',
    LSM_CORRUPT: 'Database is corrupt',
    LSM_FULL: 'Storage device is full',
    LSM_CANTOPEN: 'Cannot open database',
    LSM_PROTOCOL: 'Protocol error',
    LSM_MISUSE: 'Misuse',
    LSM_MISMATCH: 'Mismatch',
}

cdef inline int ensure_bytes(obj) except -1:
    if PyBytes_Check(obj) or obj is None:
        return 1
    raise ValueError('%r is not a bytes object.', obj)

cdef inline _check(int rc):
    """Check the return value of a call to an LSM function."""
    if rc != LSM_OK:
        exc_class = EXC_MAPPING.get(rc, Exception)
        raise exc_class(EXC_MESSAGE_MAPPING.get(rc, 'Unknown error'))

cdef bint IS_PY3K = sys.version_info[0] == 3

cdef inline bytes encode(obj):
    cdef bytes result = None
    if PyUnicode_Check(obj):
        result = PyUnicode_AsUTF8String(obj)
    elif PyBytes_Check(obj):
        result = <bytes>obj
    elif obj is not None:
        result = PyUnicode_AsUTF8String(unicode(obj))
    return result


cdef set OPTIONS = set([])

def option(name, lsm_flag, bool_to_int=False, pre_open=False):
    global OPTIONS
    OPTIONS.add(name)
    def _getter(LSM self):
        cdef int i = -1
        _check(lsm_config(self.db, lsm_flag, &i))
        return i

    def _setter(LSM self, value):
        cdef int i
        if pre_open and self.was_opened:
            raise ValueError('cannot set option after database has been '
                             'opened.')
        if bool_to_int:
            i = value and 1 or 0
        else:
            i = value
        _check(lsm_config(self.db, lsm_flag, &i))
        self._options[name] = value
        return i
    return property(_getter, _setter)


cdef class LSM(object):
    """
    Python wrapper for SQLite4's LSM implementation.

    http://www.sqlite.org/src4/doc/trunk/www/lsmapi.wiki

    Performance notes
    ^^^^^^^^^^^^^^^^^

    Optimizing database write throughput and responsiveness is done by
    configuring and scheduling work and checkpoint operations, and by
    configuring a few other parameters.

    * ``autocheckpoint``, default=2048 in KB, or 2MB

      Controls how often the database is checkpointed. Increasing this value
      to 8MB may improve overall write throughput.

    * ``autoflush``, default=1024 in KB, or 1MB

      Determines how much data, in KB, is allowed to accumulate in the live
      in-memory tree before the tree is marked as "old". The default, 1024K,
      may be increased to improve overall write throughput. Decreasing this
      value reduces memory usage.

    * ``automerge``, default=4 segments

      If auto-work is enabled, then this option is set to the number of
      segments that the library attempts to merge simultaneously. Increasing
      this value may reduce the total amount of data written to the database
      file. Decreasing it has the opposite effect and also decreases the
      average number of segments in the file, which may improve reads.

      The default value is 4, but may be set to any value between 2 and 8.

    * ``autowork``, enabled by default

      Let the database determine when to perform checkpoints, as a part of
      calls to insert(), delete(), or commit(). If set to 0 (false), then
      the application must schedule these operations.

    * ``mmap``, enabled by default on 64-bit systems

      If LSM is running on 64-bit system, mmap may be set to 1 or 0. On
      32-bit systems mmap is always 0.

      If enabled, the entire database file is memory mapped. If false, data
      is accessed using the OS file primitives. Memory mapping can
      significantly improve the performance of read operations, as pages do
      not have to be copied from OS buffers into user space.

    * ``multiple_processes``, enabled by default

      If set to 0 (false) the library does not use file-locking primitives
      to lock the database, which speeds up transactions. This option is
      enabled by default.

    * ``write_safety``, default=1

      This option determines how often the library pauses to wait for data
      written to the file-system to be synced. Since syncing is much slower
      than simply copying data into OS buffers, this option has a large
      effect on write performance. See set_write_safety() for more info.

    * ``transaction_log``, enabled by default

      This option determines whether the db will write changes to a log
      file. If disabled, writes will be faster but there is a chance for
      data loss in the event of application crash or power failure. Option
      is enabled by default.

    The speed of database read operations is largely determined by the number
    of segments in the database file. So optimizing read operations is also
    linked to the configuring and scheduling of database write operations, as
    these policies determine the number of segments that are present in the
    database file at any time.
    """
    cdef:
        lsm_db *db
        bint open_database
        bint was_opened
        bytes encoded_filename
        dict _options
        readonly bint is_open
        readonly int transaction_depth
        readonly filename

    def __cinit__(self):
        self.db = <lsm_db *>0
        self.is_open = False
        self.transaction_depth = 0
        self.was_opened = False

    def __dealloc__(self):
        if self.is_open and self.db:
            lsm_close(self.db)

    def __init__(self, filename, open_database=True, **options):
        """
        :param str filename: Path to database file.
        :param bool open_database: Whether to open the database automatically
            when the class is instantiated.
        :param options: Values for the various tunable options.
        """
        self.filename = filename
        if isinstance(filename, unicode):
            self.encoded_filename = fsencode(filename)
        else:
            self.encoded_filename = encode(filename)

        bad_options = set(options) - OPTIONS
        if bad_options:
            raise ValueError('The following options were not recognized: %s. '
                             'Valid options are:\n%s' %
                             (', '.join(sorted(bad_options)),
                              '\n'.join(sorted(OPTIONS))))
        self._options = options

        self.open_database = open_database
        if self.open_database:
            self.open()

    cpdef open(self):
        """
        Open the database. If the database was already open, this will return
        False, otherwise returns True on success.
        """
        cdef:
            char *filename = self.encoded_filename

        if self.is_open:
            return False

        _check(lsm_new(NULL, &self.db))

        # Configure database handle with any default configuration values.
        for key, value in self._options.items():
            setattr(self, key, value)

        _check(lsm_open(self.db, filename))
        self.is_open = True
        self.was_opened = True
        return True

    cpdef close(self):
        """
        Close the database. If the database was already closed, this will
        return False, otherwise returns True on success.

        .. warning::

            You must close all cursors before attempting to close the db,
            otherwise an ``IOError`` will be raised.
        """
        cdef int rc

        if not self.is_open:
            return False

        rc = lsm_close(self.db)
        if rc in (LSM_BUSY, LSM_MISUSE):
            raise IOError('Unable to close database, one or more '
                          'cursors may still be in use.')
        self.db = <lsm_db *>0
        self.is_open = False
        _check(rc)
        return True

    page_size = option('page_size', LSM_CONFIG_PAGE_SIZE, pre_open=True)
    """
    Set the page size (in bytes). Default value is 4096 bytes, but may
    be between 256 and 64K.

    .. warning:: This may only be set prior to calling `lsm_open()`.
    """

    block_size = option('block_size', LSM_CONFIG_BLOCK_SIZE, pre_open=True)
    """
    Must be set to a power of two between 64 and 65536, inclusive (block
    sizes between 64KB and 64MB).

    If the connection creates a new database, the block size of the new
    database is set to the value of this option in KB. After lsm_open()
    has been called, querying this parameter returns the actual block
    size of the opened database.

    The default value is 1024 (1MB blocks).

    .. warning:: This may only be set prior to calling `lsm_open()`.
    """

    multiple_processes = option('multiple_processes',
                                LSM_CONFIG_MULTIPLE_PROCESSES, pre_open=True,
                                bool_to_int=True)
    """
    If true, the library uses shared-memory and posix advisory locks to
    co-ordinate access by clients from within multiple processes.
    Otherwise, if false, all database clients must be located in the same
    process.

    The default value is 1, or true.

    .. warning:: This may only be set prior to calling `lsm_open()`.
    """

    readonly = option('readonly', LSM_CONFIG_READONLY, pre_open=True,
                      bool_to_int=True)
    """
    Configure read-only mode for the database.

    .. warning:: This may only be set prior to calling `lsm_open()`.
    """

    write_safety = option('write_safety', LSM_CONFIG_SAFETY)
    """
    From a performance point of view, this option determines how often the
    library pauses to wait for data written to the file-system to be
    stored on the persistent media (e.g. hard disk or solid-state memory).
    This is also known as "syncing" data to disk. Since this is orders of
    magnitude slower than simply copying data into operating system
    buffers, the value of this option has a large effect on write
    performance.

    If LSM_CONFIG_SAFETY is set to 2 (FULL), then the library syncs the
    data written to the log file to disk whenever a transaction is
    committed. Or, if LSM_CONFIG_SAFETY is set to 1 (NORMAL), then data
    is only synced to disk when a checkpoint is performed. Finally, if it
    is set to 0 (OFF), then no data is ever synced to disk.

    The default value is 1 (NORMAL).

    * 0 (off):    No robustness. A system crash may corrupt the database.

    * 1 (normal): Some robustness. A system crash may not corrupt the
                  database file, but recently committed transactions may
                  be lost following recovery.

    * 2 (full):   Full robustness. A system crash may not corrupt the
                  database file. Following recovery the database file
                  contains all successfully committed transactions.
    """

    autoflush = option('autoflush', LSM_CONFIG_AUTOFLUSH)
    """
    This value determines the amount of data allowed to accumulate in a
    live in-memory tree before it is marked as old. After committing a
    transaction, a connection checks if the size of the live in-memory
    tree, including data structure overhead, is greater than the value of
    this option in KB. If it is, and there is not already an old in-memory
    tree, the live in-memory tree is marked as old.

    An old in-memory tree is immutable - new data is always inserted into
    the live tree. There may be at most one old tree in memory at a time.

    The maximum allowable value is 1048576 (1GB). There is no minimum
    value. If this parameter is set to zero, then an attempt is made to
    mark the live in-memory tree as old after each transaction is
    committed.

    The default value is 1024 (1MB).
    """

    autowork = option('autowork', LSM_CONFIG_AUTOWORK, bool_to_int=True)
    """
    If auto-work is enabled, then this option is set to the number of
    segments that the library attempts to merge simultaneously. Increasing
    this value may reduce the total amount of data written to the database
    file. Decreasing it increases the amount of data written to the file,
    but also decreases the average number of segments present in the file,
    which can improve the performance of database read operations.

    Additionally, whether or not auto-work is enabled, this option is used
    to determine the maximum number of segments of a given age that are
    allowed to accumulate in the database file.

    May be set to 1 or 0, the default being 1.
    """

    automerge = option('automerge', LSM_CONFIG_AUTOMERGE)
    """
    If auto-work is enabled, then this option is set to the number of
    segments that the library attempts to merge simultaneously. Increasing
    this value may reduce the total amount of data written to the database
    file. Decreasing it increases the amount of data written to the file,
    but also decreases the average number of segments present in the file,
    which can improve the performance of database read operations.

    Additionally, whether or not auto-work is enabled, this option is used
    to determine the maximum number of segments of a given age that are
    allowed to accumulate in the database file. This is described in the
    compulsary work and checkpoints section below.

    The default value is 4. This option must be set to a value between
    2 and 8, inclusive.
    """

    autocheckpoint = option('autocheckpoint', LSM_CONFIG_AUTOCHECKPOINT)
    """
    If this option is set to non-zero value N, then a checkpoint is
    automatically attempted after each N KB of data have been written to
    the database file.

    The amount of uncheckpointed data already written to the database file
    is a global parameter. After performing database work (writing to the
    database file), the process checks if the total amount of
    uncheckpointed data exceeds the value of this paramter. If so, a
    checkpoint is performed. This means that this option may cause the
    connection to perform a checkpoint even if the current connection has
    itself written very little data into the database file.

    The default value is 2048 (checkpoint every 2MB).
    """

    mmap = option('mmap', LSM_CONFIG_MMAP)
    """
    If LSM is running on a system with a 64-bit address space, this option
    may be set to either 1 (true) or 0 (false). On a 32-bit platform, it
    is always set to 0.

    If it is set to true, the entire database file is memory mapped. Or,
    if it is false, data is accessed using ordinary OS file read and write
    primitives. Memory mapping the database file can significantly improve
    the performance of read operations, as database pages do not have to
    be copied from operating system buffers into user space buffers before
    they can be examined.

    This option may not be set if there is a read or write transaction
    open on the database.
    """

    transaction_log = option('transaction_log', LSM_CONFIG_USE_LOG,
                             bool_to_int=True)
    """
    This is another option that may be set to either 1 (true) or 0 (false).
    The default value is 1 (true). If it is set to false, then the library
    does not write data into the database log file. This makes writing
    faster, but also means that if an application crash or power failure
    occurs, it is very likely that any recently committed transactions
    will be lost.

    If this option is set to true, then an application crash cannot cause
    data loss. Whether or not data loss may occur in the event of a power
    failure depends on the value of the LSM_CONFIG_SAFETY parameter.

    This option can only be set if the connection does not currently have
    an open write transaction.
    """

    cpdef int pages_written(self):
        """
        The number of 4KB pages written to the database file during the
        lifetime of this connection.
        """
        cdef int npages
        _check(lsm_info(self.db, LSM_INFO_NWRITE, &npages))
        return npages

    cpdef int pages_read(self):
        """
        The number of 4KB pages read from the database file during the
        lifetime of this connection.
        """
        cdef int npages
        _check(lsm_info(self.db, LSM_INFO_NREAD, &npages))
        return npages

    cpdef int checkpoint_size(self):
        """
        The number of KB written to the database file since the most recent
        checkpoint.
        """
        cdef int nkb
        _check(lsm_info(self.db, LSM_INFO_CHECKPOINT_SIZE, &nkb))
        return nkb

    cpdef tuple tree_size(self):
        """
        At any time, there are either one or two tree structures held in shared
        memory that new database clients will access (there may also be
        additional tree structures being used by older clients - this API does
        not provide information on them). One tree structure - the current
        tree - is used to accumulate new data written to the database. The
        other tree structure - the old tree - is a read-only tree holding
        older data and may be flushed to disk at any time.

        Assuming no error occurs, the location pointed to by the first of the
        two (int *) arguments is set to the size of the old in-memory tree in
        KB. The second is set to the size of the current, or live in-memory
        tree.
        """
        cdef int t1, t2
        _check(lsm_info(self.db, LSM_INFO_TREE_SIZE, &t1, &t2))
        return (t1, t2)

    def __enter__(self):
        """
        Use the database as a context manager. The database will be closed
        when the wrapped block exits.
        """
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    cpdef insert(self, key, value):
        """
        Insert a key/value pair to the database. If the key exists, the
        previous value will be overwritten.

        .. note::

            Rather than calling :py:meth:`~LSM.insert`, you can simply treat
            your database as a dictionary and use the
            :py:meth:`~LSM.__setitem__` API:

            .. code-block:: python

                # These are equivalent:
                lsm_db.insert('key', 'value')
                lsm_db['key'] = 'value'
        """
        cdef:
            bytes bkey = encode(key)
            bytes bvalue = encode(value)
            char *kbuf
            char *vbuf
            int rc
            Py_ssize_t klen, vlen

        PyBytes_AsStringAndSize(bkey, &kbuf, &klen)
        PyBytes_AsStringAndSize(bvalue, &vbuf, &vlen)

        _check(lsm_insert(
            self.db,
            kbuf,
            klen,
            vbuf,
            vlen))

    cpdef update(self, dict values):
        """
        Add an arbitrary number of key/value pairs. Unlike the Python
        ``dict.update`` method, :py:meth:`~LSM.update` does not accept
        arbitrary keyword arguments and only takes a single dictionary as
        the parameter.

        :param dict values: A dictionary of key/value pairs.
        """
        for key in values:
            self.insert(key, values[key])

    cpdef fetch(self, key, int seek_method=LSM_SEEK_EQ):
        """
        Retrieve a value from the database.

        :param str key: The key to retrieve.
        :param int seek_method: Instruct the database how to match the key.
        :raises: ``KeyError`` if a matching key cannot be found. See below
            for more details.

        The following seek methods can be specified.

        * ``SEEK_EQ`` (default): match key based on equality. If no match is
            found, then a ``KeyError`` is raised.
        * ``SEEK_LE``: if the key does not exist, return the largest key in the
            database that is *smaller* than the given key. If no smaller key
            exists, then a ``KeyError`` will be raised.
        * ``SEEK_GE``: if the key does not exist, return the smallest key in
            the database that is *larger* than the given key. If no larger key
            exists, then a ``KeyError`` will be raised.

        .. note::

            Instead of calling :py:meth:`fetch`, you can simply treat your
            database like a dictionary.

            Example:

            .. code-block:: python

                # These are equivalent:
                val = lsm_db.fetch('key')
                val = lsm_db['key']

                # You can specify the `seek_method` by passing a tuple:
                val = lsm_db.fetch('other-key', SEEK_LE)
                val = lsm_db['other-key', SEEK_LE]
        """
        cdef:
            lsm_cursor *pcursor = <lsm_cursor *>0
            bytes bkey = encode(key)
            char *kbuf
            char *vbuf
            int rc
            int vlen
            Py_ssize_t klen

        PyBytes_AsStringAndSize(bkey, &kbuf, &klen)

        # Use low-level cursor APIs for performance, since this method could
        # be a hot-spot. Another idea is to use a cursor cache or a shared
        # cursor context. Or the method could accept a cursor as a parameter.
        lsm_csr_open(self.db, &pcursor)
        try:
            rc = lsm_csr_seek(pcursor, <void *>kbuf, klen, seek_method)
            if rc == LSM_OK and lsm_csr_valid(pcursor):
                rc = lsm_csr_value(pcursor, <const void **>(&vbuf), &vlen)
                if rc == LSM_OK:
                    return vbuf[:vlen]
            raise KeyError(key)
        finally:
            lsm_csr_close(pcursor)

    cpdef fetch_bulk(self, keys, int seek_method=LSM_SEEK_EQ):
        """
        Retrieve multiple values from the database.

        :param list keys: Keys to retrieve.
        :param int seek_method: Instruct the database how to match the key.
        :return: dictionary mapping key to value.
        """
        cdef:
            lsm_cursor *pcursor = <lsm_cursor *>0
            bytes bkey
            char *kbuf
            char *vbuf
            dict accum = {}
            int rc
            int vlen
            Py_ssize_t klen

        lsm_csr_open(self.db, &pcursor)

        try:
            for key in keys:
                bkey = encode(key)
                PyBytes_AsStringAndSize(bkey, &kbuf, &klen)

                rc = lsm_csr_seek(pcursor, <void *>kbuf, klen, seek_method)
                if rc == LSM_OK and lsm_csr_valid(pcursor):
                    rc = lsm_csr_value(pcursor, <const void **>(&vbuf), &vlen)
                    if rc == LSM_OK:
                        accum[key] = vbuf[:vlen]
        finally:
            lsm_csr_close(pcursor)

        return accum

    def fetch_range(self, start, end, reverse=False):
        """
        Fetch a range of keys, inclusive of both the start and end keys. If
        the start key is not specified, then the first key in the database will
        be used. If the end key is not specified, then all succeeding keys will
        be fetched.

        If the start key is less than the end key, then the keys will be
        returned in ascending order. The logic for selecting the first and last
        key in the event either key is missing is such that:

        * The start key will be the smallest key in the database that is
          larger than the given key (same as ``SEEK_GE``).
        * The end key will be the largest key in the database that is smaller
          than the given key (same as ``SEEK_LE``).

        If the start key is greater than the end key, then the keys will be
        returned in descending order. The logic for selecting the first and
        last key in the event either key is missing is such that:

        * The start key will be the largest key in the database that is
          smaller than the given key (same as ``SEEK_LE``).
        * The end key will be the smallest key in the database that is larger
          than the given key (same as ``SEEK_GE``).

        .. note::

            If one or both keys is ``None`` and you wish to fetch in reverse,
            you need to specify a third parameter, ``reverse=True``.

        .. note::

            Rather than using :py:meth:`~LSM.fetch_range`, you can use
            the :py:meth:`~LSM.__setitem__` API and pass in a slice. The
            examples below will use the slice API.

        Say we have the following data:

        .. code-block:: python

            db.update({
              'a': 'A',
              'c': 'C',
              'd': 'D',
              'f': 'F',
            })

        Here are some example calls using ascending order:

        .. code-block:: pycon

            >>> db['a':'d']
            [('a', 'A'), ('c', 'C'), ('d', 'D')]

            >>> db['a':'e']
            [('a', 'A'), ('c', 'C'), ('d', 'D')]

            >>> db['b':'e']
            [('c', 'C'), ('d', 'D')]

        If one of the boundaries is not specified (``None``), then it will
        start at the lowest or highest key, respectively.

        .. code-block:: pycon

            >>> db[:'ccc']
            [('a', 'A'), ('c', 'C')]

            >>> db['ccc':]
            [('d', 'D'), ('f', 'F')]

            >>> db[:'x']
            [('a', 'A'), ('c', 'C'), ('d', 'D'), ('f', 'F')]

        If the start key is higher than the highest key, no results are
        returned.

        .. code-block:: pycon

            >>> db['x':]
            []

        If the end key is lower than the lowest key, no results are returned.

        .. code-block:: pycon

            >>> db[:'0']
            []

        .. note::

            If the start key is greater than the end key, lsm-python will
            assume you want the range in reverse order.

        Examples in descending (reverse) order:

        .. code-block:: pycon

            >>> db['d':'a']
            [('d', 'D'), ('c', 'C'), ('a', 'A')]

            >>> db['e':'a']
            [('d', 'D'), ('c', 'C'), ('a', 'A')]

            >>> db['e':'b']
            [('d', 'D'), ('c', 'C')]

        If one of the boundaries is not specified (``None``), then it will
        start at the highest and lowest keys, respectively.

        .. code-block:: pycon

            >>> db['ccc'::True]
            [('c', 'C'), ('a', 'A')]

            >>> db[:'ccc':True]
            [('f', 'F'), ('d', 'D')]

            >>> db['x'::True]
            [('f', 'F'), ('d', 'D'), ('c', 'C'), ('a', 'A')]

        If the end key is higher than the highest key, no results are
        returned.

        .. code-block:: pycon

            >>> db[:'x':True]
            []

        If the start key is lower than the lowest key, no results are
        returned.

        .. code-block:: pycon

            >>> db['0'::True]
            []
        """
        cdef:
            bint first = start is None
            bint last = end is None
            bint one_empty = (first and not last) or (last and not first)
            bint none_empty = not first and not last

        if reverse:
            if one_empty:
                start, end = end, start
            if none_empty and (start < end):
                start, end = end, start

        if none_empty and start > end:
            reverse = True

        try:
            if reverse:
                cursor = self.cursor(reverse=True)
            else:
                cursor = self.cursor()

            for item in cursor.fetch_range(start, end):
                yield item
        finally:
            cursor.close()

    cpdef delete(self, key):
        """
        Remove the specified key and value from the database. If the key does
        not exist, no exception is raised.

        .. note::

            You can delete keys using Python's dictionary API:

            .. code-block:: python

                # These are equivalent:
                lsm_db.delete('some-key')
                del lsm_db['some-key']
        """
        cdef:
            bytes bkey = encode(key)
            char *kbuf
            Py_ssize_t klen

        PyBytes_AsStringAndSize(bkey, &kbuf, &klen)
        _check(lsm_delete(self.db, kbuf, klen))

    cpdef delete_range(self, start, end):
        """
        Delete a range of keys, though the start and end keys themselves
        are not deleted.

        :param str start: Beginning of range. This key is **not** removed.
        :param str end: End of range. This key is **not** removed.

        Rather than using :py:meth:`~LSM.delete_range`, you can use Python's
        ``del`` keyword, specifying a slice of keys.

        Example:

        .. code-block:: pycon

            >>> for key in 'abcdef':
            ...     db[key] = key.upper()

            >>> del db['a':'c']  # This will only delete 'b'.
            >>> 'a' in db, 'b' in db, 'c' in db
            (True, False, True)

            >>> del db['0':'d']
            >>> print list(db)
            [('d', 'D'), ('e', 'E'), ('f', 'F')]
        """
        cdef:
            bytes bstart = encode(start)
            bytes bend = encode(end)
            char *sb
            char *eb
            Py_ssize_t sblen, eblen

        PyBytes_AsStringAndSize(bstart, &sb, &sblen)
        PyBytes_AsStringAndSize(bend, &eb, &eblen)

        _check(lsm_delete_range(self.db, sb, sblen, eb, eblen))

    def __getitem__(self, key):
        """
        Dictionary API wrapper for the :py:meth:`fetch` and
        :py:meth:`fetch_range` methods.

        :param key: Either a string or a slice. Additionally, a second
            parameter can be supplied indicating what seek method to use.

        Examples using single keys:

        * ``['charlie']``, search for the key *charlie*.
        * ``['2014.XXX', SEEK_LE]``, return the key whose value is
          equal to *2014.XXX*. If no such key exists, return the lowest
          key that **does not exceed** *2014.XXX*. If there is no lower key,
          then a ``KeyError`` will be raised.
        * ``['2014.XXX', SEEK_GE]``, return the key whose value is
          equal to *2014.XXX*. If no such key exists, return the greatest
          key that **does not precede** *2014.XXX*. If there is no higher key,
          then a ``KeyError`` will be raised.

        Examples using slices (SEEK_LE and SEEK_GE cannot be used with slices):

        * ``['a':'z']``, return all keys from *a* to *z* in ascending order.
        * ``['z':'a']``, return all keys from *z* to *a* in reverse order.
        * ``['a':]``, return all key/value pairs from ``a`` on up.
        * ``[:'z']``, return all key/value pairs up to and including ``z``.
        * ``['a'::True]``, return all key/value pairs from ``a`` on up in
          reverse order.

        .. note::

            When fetching slices, a ``KeyError`` will not be raised under
            any circumstances.
        """
        cdef int seek_method = LSM_SEEK_EQ
        cdef bint reverse

        if isinstance(key, slice):
            return self.fetch_range(key.start, key.stop, key.step)
        else:
            if isinstance(key, tuple):
                key, seek_method = key
            return self.fetch(key, seek_method)

    def __setitem__(self, key, value):
        """
        Dictionary API wrapper for the :py:meth:`insert` method.
        """
        self.insert(key, value)

    def __delitem__(self, key):
        """
        Dictionary API wrapper for the :py:meth:`delete` and
        :py:meth:`delete_range` methods.

        :param key: Either a string or a slice. Additionally, a second
            parameter can be supplied indicating what seek method to use.

        .. note::

            When deleting a range of keys, the start and end keys themselves
            are **not** deleted, only the intervening keys.
        """
        if isinstance(key, slice):
            self.delete_range(key.start, key.stop)
        else:
            self.delete(key)

    def __contains__(self, key):
        """
        Return a boolean indicating whether the given key exists.
        """
        try:
            self.fetch(key)
        except KeyError:
            return False
        else:
            return True

    def __iter__(self):
        """
        Efficiently iterate through the items in the database. This method
        yields successive key/value pairs.

        .. note::

            The return value is a generator.
        """
        with self.cursor() as cursor:
            for item in cursor:
                yield item

    def __reversed__(self):
        """
        Efficiently iterate through the items in the database in reverse
        order. This method yields successive key/value pairs.
        """
        with self.cursor(True) as cursor:
            for item in cursor:
                yield item

    def keys(self, reverse=False):
        """
        Return a generator that successively yields the keys in the database.

        :param bool reverse: Return the keys in reverse order.
        :rtype: generator
        """
        with self.cursor(reverse) as cursor:
            for key in cursor.keys():
                yield key

    def values(self, reverse=False):
        """
        Return a generator that successively yields the values in the database.
        The values are **ordered based on their key**.

        :param bool reverse: Return the values in reverse key-order.
        :rtype: generator
        """
        with self.cursor(reverse) as cursor:
            for value in cursor.values():
                yield value

    cpdef int incr(self, key):
        cdef bytes value
        cdef int ivalue
        try:
            value = self[key]
        except KeyError:
            ivalue = 0
        else:
            ivalue = struct.unpack('>q', value)[0]
        ivalue += 1
        self[key] = struct.pack('>q', ivalue)
        return ivalue

    cpdef flush(self):
        """
        Flush the in-memory tree to disk, creating a new segment.

        The contents of an old in-memory tree may be written into the database
        file at any point. Once its contents have been written (or "flushed")
        to the database file, the in-memory tree may be discarded. Flushing an
        in-memory tree to the database file creates a new database "segment".
        A database segment is an immutable b-tree structure stored within the
        database file. A single database file may contain up to 64 segments.

        At any point, two or more existing segments within the database file
        may be merged together into a single segment. Once their contents has
        been merged into the new segment, the original segments may be
        discarded.

        After the set of segments in a database file has been modified (either
        by flushing an in-memory tree to disk or by merging existing segments
        together), the changes may be made persistent by "checkpointing" the
        database. Checkpointing involves updating the database file header and
        (usually) syncing the contents of the database file to disk.
        """
        _check(lsm_flush(self.db))

    cpdef int work(self, int nmerge=1, int nkb=4096) except -1:
        """
        Explicitly perform work on the database structure.

        If the database has an old in-memory tree when :py:meth:`work` is
        called, it is flushed to disk. If this means that more than ``nkb`` of
        data is written to the database file, no further work is performed.
        Otherwise, the number of KB written is subtracted from nKB before
        proceeding.

        Typically you will use ``1`` for the parameter in order to *optimize*
        the database.

        :param int nkb: Limit on the number of KB of data that should be
            written to the database file before the call returns. It is a
            hint and is not honored strictly.
        :returns: The number of KB written to the database file.

        .. note::

            A background thread or process is ideal for running this method.
        """
        cdef int nbytes_written
        cdef int rc
        rc = lsm_work(self.db, nmerge, nkb, &nbytes_written)
        if rc == LSM_BUSY:
            raise RuntimeError('Unable to acquire the worker lock. Perhaps '
                               'another thread or process is working on the '
                               'database?')
        _check(rc)
        return nbytes_written

    cpdef int checkpoint(self, int nkb) except -1:
        """
        Write to the database file header. If the current snapshot has already
        been checkpointed, calling this function is a no-op. In this case if
        pnKB is not NULL, *nkb is set to 0. Or, if the current snapshot is
        successfully checkpointed by this function and pbKB is not NULL, *nkb
        is set to the number of bytes written to the database file since the
        previous checkpoint (the same measure as returned by the
        LSM_INFO_CHECKPOINT_SIZE query).
        """
        _check(lsm_checkpoint(self.db, &nkb))
        return nkb

    cpdef begin(self):
        """
        Begin a transaction. Transactions can be nested.

        .. note::

            In most cases it is preferable to use the :py:meth:`transaction`
            context manager/decorator.
        """
        self.transaction_depth += 1
        _check(lsm_begin(self.db, self.transaction_depth))

    cdef int _commit(self) except -1:
        if self.transaction_depth > 0:
            self.transaction_depth -= 1
            _check(lsm_commit(self.db, self.transaction_depth))
            return 1
        return 0

    def commit(self):
        """
        Commit the inner-most transaction.

        :returns: Boolean indicating whether the changes were commited.
        """
        return self._commit() and True or False

    cdef int _rollback(self, bint keep_transaction) except -1:
        if self.transaction_depth > 0:
            if not keep_transaction:
                self.transaction_depth -= 1
            _check(lsm_rollback(self.db, self.transaction_depth))
            return 1
        return 0

    def rollback(self, bint keep_transaction=True):
        """
        Rollback the inner-most transaction. If `keep_transaction` is `True`,
        then the transaction will remain open after the changes were rolled
        back.

        :param bool keep_transaction: Whether the transaction will remain open
            after the changes are rolled back (default=True).
        :returns: Boolean indicating whether the changes were rolled back.
        """
        return self._rollback(keep_transaction) and True or False

    cpdef Transaction transaction(self):
        """
        Create a context manager that runs the wrapped block in a transaction.

        Example:

        .. code-block:: python

            with lsm_db.transaction() as txn:
                lsm_db['k1'] = 'v1'

            with lsm_db.transaction() as txn:
                lsm_db['k1'] = 'v1-1'
                txn.rollback()

            assert lsm_db['k1'] == 'v1'

        You can also use the :py:meth:`transaction` method as a decorator.
        If the wrapped function returns normally, the transaction is committed,
        otherwise it is rolled back.

        .. code-block:: python

            @lsm_db.transaction()
            def transfer_funds(from_account, to_account, amount):
                # transfer money...
                return
        """
        return Transaction.__new__(Transaction, self)

    cpdef Cursor cursor(self, bint reverse=False):
        """
        Create a cursor and return it as a context manager. After the wrapped
        block, the cursor is closed.

        :param bool reverse: Whether the cursor will iterate over keys in
            descending order.

        Example:

        .. code-block:: python

            with lsm_db.cursor() as cursor:
                for key, value in cursor.fetch_range('a', 'z'):
                    # do something with data...

            with lsm_db.cursor(reverse=True) as cursor:
                for key, value in cursor.fetch_range('z', 'a'):
                    # data is now ordered descending order.

        .. note::
            In general the :py:meth:`cursor` context manager should be used as
            it ensures cursors are properly cleaned up when you are done using
            them.

            LSM databases cannot be closed as long as there are any open
            cursors, so it is very important to close them when finished.
        """
        return Cursor.__new__(Cursor, self, reverse)


cdef class Cursor(object):
    """
    Wrapper around the `lsm_cursor` object.

    Functions :py:meth:`seek`, :py:meth:`first`, and :py:meth:`last` are
    *seek* functions. Whether or not :py:meth:`next` and :py:meth:`previous`
    may be called successfully depends on the most recent seek function called
    on the cursor. Specifically,

    * At least one seek function must have been called on the cursor.
    * To call ``next()``, the most recent call to a seek function must have
      been either ``first()`` or a call to ``seek()`` specifying ``SEEK_GE``.
    * To call ``previous()``, the most recent call to a seek function must have
      been either ``last()`` or a call to ``seek()`` specifying ``SEEK_LE``.

    Otherwise, if the above conditions are not met when ``next()`` or
    ``previous()`` is called, ``LSM_MISUSE`` is returned and the cursor
    position remains unchanged.

    For more information, see:

    http://www.sqlite.org/src4/doc/trunk/www/lsmusr.wiki#reading_from_a_database
    """
    cdef:
        LSM lsm
        lsm_cursor *cursor
        bint is_open
        bint _consumed
        readonly bint _reverse

    def __cinit__(self, LSM lsm, bint reverse):
        self.lsm = lsm
        self.cursor = <lsm_cursor *>0
        lsm_csr_open(self.lsm.db, &self.cursor)
        self.is_open = True
        self._consumed = False
        self._reverse = reverse

    def __dealloc__(self):
        if self.is_open:
            lsm_csr_close(self.cursor)

    cdef int _open(self) except -1:
        """
        Open the cursor. In general this method does not need to be called
        by applications, as it is called automatically when a
        :py:class:`Cursor` is instantiated.
        """
        if self.is_open:
            return 0

        _check(lsm_csr_open(self.lsm.db, &self.cursor))
        self.is_open = True
        return 1

    def open(self):
        return self._open() and True or False

    cdef int _close(self):
        """
        Close the cursor.

        .. note::

            If you are using the cursor as a context manager, then it is not
            necessary to call this method.

        .. warning::

            If a cursor is not closed, then the database cannot be closed
            properly.
        """
        if not self.is_open:
            return 0

        lsm_csr_close(self.cursor)
        self.is_open = False
        return 1

    def close(self):
        return self._close() and True or False

    def __enter__(self):
        """
        Expose the cursor as a context manager. After the wrapped block,
        the cursor will be closed, which is very important.
        """
        self._open()
        if self._reverse:
            self.last()
        else:
            self.first()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

    def __iter__(self):
        """
        Iterate from the cursor's current position. The iterator returns
        successive key/value pairs.
        """
        self._consumed = not self.is_valid()
        return self

    def __next__(self):
        cdef int rc

        if self._consumed:
            raise StopIteration

        key = self._key()
        value = self._value()
        try:
            if self._reverse:
                self.previous()
            else:
                self.next()
        except StopIteration:
            self._consumed = True

        return (key, value)

    cpdef int compare(self, key, int nlen=0):
        """
        Compare the given key with key at the cursor's current position.
        """
        cdef:
            bytes bkey = encode(key)
            char *kbuf
            int rc, res
            Py_ssize_t klen

        PyBytes_AsStringAndSize(bkey, &kbuf, &klen)

        if nlen == 0:
            nlen = klen
        rc = lsm_csr_cmp(
            self.cursor,
            kbuf,
            nlen,
            &res)
        _check(rc)
        return res

    cpdef seek(self, key, int method=LSM_SEEK_EQ):
        """
        Seek to the given key using the specified matching method. If the
        operation did not find a valid key, then a ``KeyError`` will be raised.

        * ``SEEK_EQ`` (default): match key based on equality. If no match is
            found then a ``KeyError`` is raised.
        * ``SEEK_LE``: if the key does not exist, return the largest key in the
            database that is *smaller* than the given key. If no smaller key
            exists, then a ``KeyError`` will be raised.
        * ``SEEK_GE``: if the key does not exist, return the smallest key in
            the database that is *larger* than the given key. If no larger key
            exists, then a ``KeyError`` will be raised.

        For more details, read:

        http://www.sqlite.org/src4/doc/trunk/www/lsmapi.wiki#lsm_csr_seek
        """
        cdef:
            bytes bkey = encode(key)
            char *kbuf
            Py_ssize_t klen
            int rc

        PyBytes_AsStringAndSize(bkey, &kbuf, &klen)

        _check(lsm_csr_seek(
            self.cursor,
            <void *>kbuf,  # For some reason a void ptr?
            klen,
            method))
        if not self.is_valid():
            raise KeyError(key)

    cpdef bint is_valid(self):
        """
        Return a boolean indicating whether the cursor is pointing at a
        valid record.
        """
        return lsm_csr_valid(self.cursor) != 0

    cpdef first(self):
        """Jump to the first key in the database."""
        _check(lsm_csr_first(self.cursor))

    cpdef last(self):
        """Jump to the last key in the database."""
        _check(lsm_csr_last(self.cursor))

    cpdef next(self):
        """
        Advance the cursor to the next record. If no next record exists, then
        a ``StopIteration`` will be raised.

        If you encounter an Exception indicating *Misuse (21)* when calling
        this method, then you need to be sure that you are either calling
        :py:meth:`first` or :py:meth:`seek` with a seek method of ``SEEK_GE``.
        """
        cdef int rc
        _check(lsm_csr_next(self.cursor))
        rc = lsm_csr_valid(self.cursor)
        if not rc:
            raise StopIteration

    cpdef previous(self):
        """
        Move the cursor to the previous record. If no previous record exists,
        then a ``StopIteration`` will be raised.

        If you encounter an Exception indicating *Misuse (21)* when calling
        this method, then you need to be sure that you are either calling
        :py:meth:`last` or :py:meth:`seek` with a seek method of ``SEEK_LE``.
        """
        cdef int rc
        _check(lsm_csr_prev(self.cursor))
        rc = lsm_csr_valid(self.cursor)
        if not rc:
            raise StopIteration

    def fetch_until(self, key):
        """
        This method returns a generator that yields key/value pairs obtained
        by iterating from the cursor's current position until it reaches
        the given ``key``.
        """
        cdef:
            int is_reverse = self._reverse
            int res
            int nkey

        if key is not None:
            nkey = len(key)

        while self.is_valid():
            if key is not None:
                res = self.compare(key, nkey)
                if not is_reverse and res > 0:
                    break
                elif is_reverse and res < 0:
                    break

            yield (self._key(), self._value())
            try:
                if not is_reverse:
                    self.next()
                else:
                    self.previous()
            except StopIteration:
                break

    def fetch_range(self, start, end):
        """
        Fetch a range of keys, inclusive of both the start and end keys. If
        the start key is not specified, then the first key will be used. If
        the end key is not specified, then all succeeding keys will be fetched.

        For complete details, see the docstring for
        :py:meth:`LSM.fetch_range`.
        """
        cdef int is_reverse = self._reverse
        cdef int seek_method = is_reverse and LSM_SEEK_LE or LSM_SEEK_GE

        # py3k
        s_lt_e = (start and end and start < end) or not start
        s_gt_e = (start and end and start > end) or not end

        if (is_reverse and s_lt_e) or (not is_reverse and s_gt_e):
            if start and end:
                start, end = end, start

        if not start:
            if is_reverse:
                self.last()
            else:
                self.first()
        else:
            try:
                self.seek(start, seek_method)
            except KeyError:
                raise StopIteration

        for key, value in self.fetch_until(end):
            yield (key, value)

    cdef inline _key(self):
        """Return the key at the cursor's current position."""
        cdef:
            char *k
            int klen

        lsm_csr_key(self.cursor, <const void **>(&k), &klen)
        return k[:klen]

    cdef inline _value(self):
        """Return the value at the cursor's current position."""
        cdef:
            char *v
            int vlen

        lsm_csr_value(self.cursor, <const void **>(&v), &vlen)
        return v[:vlen]

    def key(self):
        return self._key()

    def value(self):
        return self._value()

    def keys(self):
        """Return a generator that successively yields keys."""
        if not self.is_valid():
            raise StopIteration
        while True:
            yield self._key()
            if self._reverse:
                self.previous()
            else:
                self.next()

    def values(self):
        """Return a generator that successively yields values."""
        if not self.is_valid():
            raise StopIteration
        while True:
            yield self._value()
            if self._reverse:
                self.previous()
            else:
                self.next()


cdef class Transaction(object):
    """
    Context manager and decorator to run the wrapped block in a transaction.
    LSM supports nested transactions, so the context manager/decorator can be
    mixed and matched and nested arbitrarily.

    Rather than instantiating this class directly, use
    :py:meth:`LSM.transaction`.

    Example:

    .. code-block:: python

        with lsm_db.transaction() as txn:
            lsm_db['k1'] = 'v1'

        with lsm_db.transaction() as txn:
            lsm_db['k1'] = 'v1-1'
            txn.rollback()

        assert lsm_db['k1'] == 'v1'
    """
    cdef LSM lsm

    def __cinit__(self, lsm):
        self.lsm = lsm

    def __enter__(self):
        self.lsm.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback(False)
        else:
            try:
                self.commit(False)
            except:
                self.lsm._rollback(False)
                raise

    def __call__(self, fn):
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner

    def commit(self, begin=True):
        """
        Commit the transaction and optionally open a new transaction.
        This is especially useful for context managers, where you may commit
        midway through a wrapped block of code, but want to retain
        transactional behavior for the rest of the block.
        """
        cdef int rc
        rc = self.lsm._commit()
        if begin:
            self.lsm.begin()
        return rc

    def rollback(self, begin=True):
        """
        Rollback the transaction and optionally retain the open transaction.
        This is especially useful for context managers, where you may rollback
        midway through a wrapped block of code, but want to retain the
        transactional behavior for the rest of the block.
        """
        return self.lsm._rollback(keep_transaction=begin)


SAFETY_OFF = LSM_SAFETY_OFF
SAFETY_NORMAL = LSM_SAFETY_NORMAL
SAFETY_FULL = LSM_SAFETY_FULL

SEEK_LEFAST = LSM_SEEK_LEFAST
SEEK_LE = LSM_SEEK_LE
SEEK_EQ = LSM_SEEK_EQ
SEEK_GE = LSM_SEEK_GE
"""ADD: # cython: profile=True to top of file to use with cProfile."""
