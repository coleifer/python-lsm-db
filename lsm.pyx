from cpython.bytes cimport PyBytes_AsStringAndSize
import struct
import sys

from libc.stdlib cimport free, malloc

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


cdef extern from "src/lsm.h":
    ctypedef struct lsm_db
    ctypedef struct lsm_env
    ctypedef struct lsm_cursor
    ctypedef struct lsm_file

    ctypedef long long int lsm_i64

    cdef int LSM_LOCK_UNLOCK = 0
    cdef int LSM_LOCK_SHARED = 1
    cdef int LSM_LOCK_EXCL = 2

    cpdef int LSM_OK = 0
    cpdef int LSM_ERROR = 1
    cpdef int LSM_BUSY = 5
    cpdef int LSM_NOMEM = 7
    cpdef int LSM_READONLY = 8
    cpdef int LSM_IOERR = 10
    cpdef int LSM_CORRUPT = 11
    cpdef int LSM_FULL = 13
    cpdef int LSM_CANTOPEN = 14
    cpdef int LSM_PROTOCOL = 15
    cpdef int LSM_MISUSE = 21
    cpdef int LSM_MISMATCH = 50

    # Connections.
    cdef int lsm_new(lsm_env *env, lsm_db **ppDb)
    cdef int lsm_close(lsm_db *pDb)
    cdef int lsm_open(lsm_db *pDb, const char *zFilename)

    cdef int lsm_config(lsm_db *pDb, int verb, ...)

    # LSM_CONFIG_AUTOFLUSH:
    #   A read/write integer parameter.
    #
    #   This value determines the amount of data allowed to accumulate in a
    #   live in-memory tree before it is marked as old. After committing a
    #   transaction, a connection checks if the size of the live in-memory tree,
    #   including data structure overhead, is greater than the value of this
    #   option in KB. If it is, and there is not already an old in-memory tree,
    #   the live in-memory tree is marked as old.
    #
    #   The maximum allowable value is 1048576 (1GB). There is no minimum
    #   value. If this parameter is set to zero, then an attempt is made to
    #   mark the live in-memory tree as old after each transaction is committed.
    #
    #   The default value is 1024 (1MB).
    cdef int LSM_CONFIG_AUTOFLUSH = 1

    # LSM_CONFIG_PAGE_SIZE:
    #   A read/write integer parameter. This parameter may only be set before
    #   lsm_open() has been called.
    cdef int LSM_CONFIG_PAGE_SIZE = 2

    # LSM_CONFIG_SAFETY:
    #   A read/write integer parameter. Valid values are 0, 1 (the default)
    #   and 2. This parameter determines how robust the database is in the
    #   face of a system crash (e.g. a power failure or operating system
    #   crash). As follows:
    #
    #     0 (off):    No robustness. A system crash may corrupt the database.
    #
    #     1 (normal): Some robustness. A system crash may not corrupt the
    #                 database file, but recently committed transactions may
    #                 be lost following recovery.
    #
    #     2 (full):   Full robustness. A system crash may not corrupt the
    #                 database file. Following recovery the database file
    #                 contains all successfully committed transactions.
    cdef int LSM_CONFIG_SAFETY = 3

    # LSM_CONFIG_BLOCK_SIZE:
    #   A read/write integer parameter.
    #
    #   This parameter may only be set before lsm_open() has been called. It
    #   must be set to a power of two between 64 and 65536, inclusive (block
    #   sizes between 64KB and 64MB).
    #
    #   If the connection creates a new database, the block size of the new
    #   database is set to the value of this option in KB. After lsm_open()
    #   has been called, querying this parameter returns the actual block
    #   size of the opened database.
    #
    #   The default value is 1024 (1MB blocks).
    cdef int LSM_CONFIG_BLOCK_SIZE = 4

    # LSM_CONFIG_AUTOWORK:
    #   A read/write integer parameter.
    cdef int LSM_CONFIG_AUTOWORK = 5

    # LSM_CONFIG_MMAP:
    #   A read/write integer parameter. If this value is set to 0, then the
    #   database file is accessed using ordinary read/write IO functions. Or,
    #   if it is set to 1, then the database file is memory mapped and accessed
    #   that way. If this parameter is set to any value N greater than 1, then
    #   up to the first N KB of the file are memory mapped, and any remainder
    #   accessed using read/write IO.
    #
    #   The default value is 1 on 64-bit platforms and 32768 on 32-bit platforms.
    cdef int LSM_CONFIG_MMAP = 7

    # LSM_CONFIG_USE_LOG:
    #   A read/write boolean parameter. True (the default) to use the log
    #   file normally. False otherwise.
    cdef int LSM_CONFIG_USE_LOG = 8

    # LSM_CONFIG_AUTOMERGE:
    #   A read/write integer parameter. The minimum number of segments to
    #   merge together at a time. Default value 4.
    cdef int LSM_CONFIG_AUTOMERGE = 9

    # LSM_CONFIG_MAX_FREELIST:
    #   A read/write integer parameter. The maximum number of free-list
    #   entries that are stored in a database checkpoint (the others are
    #   stored elsewhere in the database).
    #
    #   There is no reason for an application to configure or query this
    #   parameter. It is only present because configuring a small value
    #   makes certain parts of the lsm code easier to test.
    cdef int LSM_CONFIG_MAX_FREELIST = 10

    # LSM_CONFIG_MULTIPLE_PROCESSES:
    #   A read/write boolean parameter. This parameter may only be set before
    #   lsm_open() has been called. If true, the library uses shared-memory
    #   and posix advisory locks to co-ordinate access by clients from within
    #   multiple processes. Otherwise, if false, all database clients must be
    #   located in the same process. The default value is true.
    cdef int LSM_CONFIG_MULTIPLE_PROCESSES = 11

    # LSM_CONFIG_AUTOCHECKPOINT:
    #   A read/write integer parameter.
    #
    #   If this option is set to non-zero value N, then a checkpoint is
    #   automatically attempted after each N KB of data have been written to
    #   the database file.
    #
    #   The amount of uncheckpointed data already written to the database file
    #   is a global parameter. After performing database work (writing to the
    #   database file), the process checks if the total amount of uncheckpointed
    #   data exceeds the value of this paramter. If so, a checkpoint is performed.
    #   This means that this option may cause the connection to perform a
    #   checkpoint even if the current connection has itself written very little
    #   data into the database file.
    #
    #   The default value is 2048 (checkpoint every 2MB).
    cdef int LSM_CONFIG_AUTOCHECKPOINT = 12

    # LSM_CONFIG_SET_COMPRESSION:
    #   Set the compression methods used to compress and decompress database
    #   content. The argument to this option should be a pointer to a structure
    #   of type lsm_compress. The lsm_config() method takes a copy of the
    #   structures contents.
    #
    #   This option may only be used before lsm_open() is called. Invoking it
    #   after lsm_open() has been called results in an LSM_MISUSE error.
    cdef int LSM_CONFIG_SET_COMPRESSION = 13

    # LSM_CONFIG_GET_COMPRESSION:
    #   Query the compression methods used to compress and decompress database
    #   content.
    cdef int LSM_CONFIG_GET_COMPRESSION = 14

    # LSM_CONFIG_SET_COMPRESSION_FACTORY:
    #   Configure a factory method to be invoked in case of an LSM_MISMATCH
    #   error.
    cdef int LSM_CONFIG_SET_COMPRESSION_FACTORY = 15

    cdef int LSM_SAFETY_OFF =0
    cdef int LSM_SAFETY_NORMAL =1
    cdef int LSM_SAFETY_FULL =2

    # Query for operational statistics.
    cdef int lsm_info(lsm_db *pDb, int verb, ...)

    # The following values may be passed as the second argument to lsm_info().
    #
    # LSM_INFO_NWRITE:
    #   The third parameter should be of type (int *). The location pointed
    #   to by the third parameter is set to the number of 4KB pages written to
    #   the database file during the lifetime of this connection.
    #
    # LSM_INFO_NREAD:
    #   The third parameter should be of type (int *). The location pointed
    #   to by the third parameter is set to the number of 4KB pages read from
    #   the database file during the lifetime of this connection.
    #
    # LSM_INFO_DB_STRUCTURE:
    #   The third argument should be of type (char **). The location pointed
    #   to is populated with a pointer to a nul-terminated string containing
    #   the string representation of a Tcl data-structure reflecting the
    #   current structure of the database file. Specifically, the current state
    #   of the worker snapshot. The returned string should be eventually freed
    #   by the caller using lsm_free().
    #
    #   The returned list contains one element for each level in the database,
    #   in order from most to least recent. Each element contains a
    #   single element for each segment comprising the corresponding level,
    #   starting with the lhs segment, then each of the rhs segments (if any)
    #   in order from most to least recent.
    #
    #   Each segment element is itself a list of 4 integer values, as follows:
    #
    #   <ol><li> First page of segment
    #       <li> Last page of segment
    #       <li> Root page of segment (if applicable)
    #       <li> Total number of pages in segment
    #   </ol>
    #
    # LSM_INFO_ARRAY_STRUCTURE:
    #   There should be two arguments passed following this option (i.e. a
    #   total of four arguments passed to lsm_info()). The first argument
    #   should be the page number of the first page in a database array
    #   (perhaps obtained from an earlier INFO_DB_STRUCTURE call). The second
    #   trailing argument should be of type (char **). The location pointed
    #   to is populated with a pointer to a nul-terminated string that must
    #   be eventually freed using lsm_free() by the caller.
    #
    #   The output string contains the text representation of a Tcl list of
    #   integers. Each pair of integers represent a range of pages used by
    #   the identified array. For example, if the array occupies database
    #   pages 993 to 1024, then pages 2048 to 2777, then the returned string
    #   will be "993 1024 2048 2777".
    #
    #   If the specified integer argument does not correspond to the first
    #   page of any database array, LSM_ERROR is returned and the output
    #   pointer is set to a NULL value.
    #
    # LSM_INFO_LOG_STRUCTURE:
    #   The third argument should be of type (char **). The location pointed
    #   to is populated with a pointer to a nul-terminated string containing
    #   the string representation of a Tcl data-structure. The returned
    #   string should be eventually freed by the caller using lsm_free().
    #
    #   The Tcl structure returned is a list of six integers that describe
    #   the current structure of the log file.
    #
    # LSM_INFO_ARRAY_PAGES:
    #
    # LSM_INFO_PAGE_ASCII_DUMP:
    #   As with LSM_INFO_ARRAY_STRUCTURE, there should be two arguments passed
    #   with calls that specify this option - an integer page number and a
    #   (char **) used to return a nul-terminated string that must be later
    #   freed using lsm_free(). In this case the output string is populated
    #   with a human-readable description of the page content.
    #
    #   If the page cannot be decoded, it is not an error. In this case the
    #   human-readable output message will report the systems failure to
    #   interpret the page data.
    #
    # LSM_INFO_PAGE_HEX_DUMP:
    #   This argument is similar to PAGE_ASCII_DUMP, except that keys and
    #   values are represented using hexadecimal notation instead of ascii.
    #
    # LSM_INFO_FREELIST:
    #   The third argument should be of type (char **). The location pointed
    #   to is populated with a pointer to a nul-terminated string containing
    #   the string representation of a Tcl data-structure. The returned
    #   string should be eventually freed by the caller using lsm_free().
    #
    #   The Tcl structure returned is a list containing one element for each
    #   free block in the database. The element itself consists of two
    #   integers - the block number and the id of the snapshot that freed it.
    #
    # LSM_INFO_CHECKPOINT_SIZE:
    #   The third argument should be of type (int *). The location pointed to
    #   by this argument is populated with the number of KB written to the
    #   database file since the most recent checkpoint.
    #
    # LSM_INFO_TREE_SIZE:
    #   If this value is passed as the second argument to an lsm_info() call, it
    #   should be followed by two arguments of type (int *) (for a total of four
    #   arguments).
    #
    #   At any time, there are either one or two tree structures held in shared
    #   memory that new database clients will access (there may also be additional
    #   tree structures being used by older clients - this API does not provide
    #   information on them). One tree structure - the current tree - is used to
    #   accumulate new data written to the database. The other tree structure -
    #   the old tree - is a read-only tree holding older data and may be flushed
    #   to disk at any time.
    #
    #   Assuming no error occurs, the location pointed to by the first of the two
    #   (int *) arguments is set to the size of the old in-memory tree in KB.
    #   The second is set to the size of the current, or live in-memory tree.
    #
    # LSM_INFO_COMPRESSION_ID:
    #   This value should be followed by a single argument of type
    #   (unsigned int *). If successful, the location pointed to is populated
    #   with the database compression id before returning.

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

    cdef int LSM_SEEK_LEFAST = -2
    cdef int LSM_SEEK_LE = -1
    cdef int LSM_SEEK_EQ = 0
    cdef int LSM_SEEK_GE = 1

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


cdef bint IS_PY3K = sys.version_info[0] == 3

cdef bytes encode(obj):
    if isinstance(obj, unicode):
        return obj.encode('utf-8')
    elif isinstance(obj, bytes):
        return obj
    elif obj is None:
        return obj
    elif IS_PY3K:
        return bytes(str(obj), 'utf-8')
    return bytes(obj)


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

cdef inline _check(int rc):
    """Check the return value of a call to an LSM function."""
    if rc == LSM_OK:
        return

    exc_class = EXC_MAPPING.get(rc, Exception)
    exc_message = EXC_MESSAGE_MAPPING.get(rc, 'Unknown error')
    raise exc_class('%s: %s' % (exc_message, rc))


cdef class LSM(object):
    """
    Python wrapper for SQLite4's LSM implementation.

    http://www.sqlite.org/src4/doc/trunk/www/lsmapi.wiki
    """
    cdef:
        lsm_db *db
        bint open_database
        bint was_opened
        bytes encoded_filename
        readonly bint is_open
        readonly int transaction_depth
        readonly filename

    def __cinit__(self):
        self.db = <lsm_db *>0
        lsm_new(NULL, &self.db)
        self.is_open = False
        self.transaction_depth = 0
        self.was_opened = False

    def __dealloc__(self):
        if self.is_open and self.db:
            lsm_close(self.db)

    def __init__(self, filename, open_database=True, page_size=None,
                 block_size=None, safety_level=None,
                 enable_multiple_processes=True):
        """
        :param str filename: Path to database file.
        :param bool open_database: Whether to open the database automatically
            when the class is instantiated.
        :param int page_size: Page size in bytes. Default is 4096.
        :param int block_size: Block size in kb. Default is 1024 (1MB).
        :param int safety_level: Safety level in face of crash.
        :param bool enable_multiple_processes: Allow multiple processes to
            access the database. Default is `True`.
        """
        self.filename = filename
        if isinstance(filename, unicode):
            self.encoded_filename = fsencode(filename)
        else:
            self.encoded_filename = bytes(filename)
        if page_size:
            self.set_page_size(page_size)
        if block_size:
            self.set_block_size(block_size)
        if safety_level is not None:
            self.set_safety(safety_level)
        if not enable_multiple_processes:
            self.set_multiple_processes(False)
        self.open_database = open_database
        if self.open_database:
            self.open()

    cpdef bint open(self):
        """
        Open the database. If the database was already open, this will return
        False, otherwise returns True on success.
        """
        cdef int rc
        if self.is_open:
            return False

        _check(lsm_new(NULL, &self.db))
        _check(lsm_open(self.db, self.encoded_filename))
        self.is_open = True
        self.was_opened = True
        return True

    cpdef bint close(self):
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

    cpdef int autoflush(self, int nkb):
        """
        This value determines the amount of data allowed to accumulate in a
        live in-memory tree before it is marked as old. After committing a
        transaction, a connection checks if the size of the live in-memory
        tree, including data structure overhead, is greater than the value of
        this option in KB. If it is, and there is not already an old in-memory
        tree, the live in-memory tree is marked as old.

        The maximum allowable value is 1048576 (1GB). There is no minimum
        value. If this parameter is set to zero, then an attempt is made to
        mark the live in-memory tree as old after each transaction is
        committed.

        The default value is 1024 (1MB).
        """
        _check(lsm_config(self.db, LSM_CONFIG_AUTOFLUSH, &nkb))
        return nkb

    cpdef int set_page_size(self, int nbytes):
        """
        Set the page size (in bytes). Default value is 4096 bytes.

        .. warning::

            This may only be set before calling ``lsm_open()``. The best
            way to set this value is when you instantiate your :py:class:`LSM`
            object.

            If the database is already open, then a ``ValueError`` will be
            raised.
        """
        if self.was_opened:
            raise ValueError('Unable to set page size. Page size can only '
                             'be set before calling open().')
        _check(lsm_config(self.db, LSM_CONFIG_PAGE_SIZE, &nbytes))
        return nbytes

    cpdef int set_safety(self, int safety_level):
        """
        Valid values are 0, 1 (the default) and 2. This parameter determines
        how robust the database is in the face of a system crash (e.g. a power
        failure or operating system crash). As follows:

        * 0 (off):    No robustness. A system crash may corrupt the database.

        * 1 (normal): Some robustness. A system crash may not corrupt the
                      database file, but recently committed transactions may
                      be lost following recovery.

        * 2 (full):   Full robustness. A system crash may not corrupt the
                      database file. Following recovery the database file
                      contains all successfully committed transactions.

        Values:

        * ``SAFETY_OFF``
        * ``SAFETY_NORMAL``
        * ``SAFETY_FULL``
        """
        _check(lsm_config(self.db, LSM_CONFIG_SAFETY, &safety_level))
        return safety_level

    cpdef int set_block_size(self, int nkb):
        """
        Must be set to a power of two between 64 and 65536, inclusive (block
        sizes between 64KB and 64MB).

        If the connection creates a new database, the block size of the new
        database is set to the value of this option in KB. After lsm_open()
        has been called, querying this parameter returns the actual block
        size of the opened database.

        The default value is 1024 (1MB blocks).

        .. warning::

            This may only be set before calling ``lsm_open()``. The best
            way to set this value is when you instantiate your :py:class:`LSM`
            object.

            If the database is already open, then a ``ValueError`` will be
            raised.
        """
        if self.was_opened:
            raise ValueError('Unable to set block size. Block size can only '
                             'be set before calling open().')
        _check(lsm_config(self.db, LSM_CONFIG_BLOCK_SIZE, &nkb))
        return nkb

    cpdef config_mmap(self, int mmap_kb):
        """
        If this value is set to 0, then the database file is accessed using
        ordinary read/write IO functions. Or, if it is set to 1, then the
        database file is memory mapped and accessed that way. If this parameter
        is set to any value N greater than 1, then up to the first N KB of the
        file are memory mapped, and any remainder accessed using read/write IO.
        """
        _check(lsm_config(self.db, LSM_CONFIG_MMAP, &mmap_kb))

    cpdef set_automerge(self, int nsegments):
        """
        The minimum number of segments to merge together at a time.

        The default value is 4.
        """
        _check(lsm_config(self.db, LSM_CONFIG_AUTOMERGE, &nsegments))

    cpdef set_multiple_processes(self, bint enable_multiple_processes):
        """
        If true, the library uses shared-memory and posix advisory locks to
        co-ordinate access by clients from within multiple processes.
        Otherwise, if false, all database clients must be located in the same
        process.

        The default value is ``True``.

        .. warning::

            This may only be set before calling ``lsm_open()``. The best
            way to set this value is when you instantiate your :py:class:`LSM`
            object.

            If the database is already open, then a ``ValueError`` will be
            raised.
        """
        if self.was_opened:
            raise ValueError('Unable to set process flag. Multi-process flag '
                             'can only be set before calling open().')
        _check(lsm_config(
            self.db,
            LSM_CONFIG_MULTIPLE_PROCESSES,
            &enable_multiple_processes))

    cpdef set_auto_checkpoint(self, int nbytes):
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
        _check(lsm_config(self.db, LSM_CONFIG_AUTOCHECKPOINT, &nbytes))

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
            char *kbuf
            char *vbuf
            int rc
            Py_ssize_t klen, vlen

        if IS_PY3K:
            key = encode(key)
            value = encode(value)

        PyBytes_AsStringAndSize(key, &kbuf, &klen)
        PyBytes_AsStringAndSize(value, &vbuf, &vlen)

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
            char *kbuf
            char *vbuf
            int rc
            int vlen
            Py_ssize_t klen

        if IS_PY3K:
            key = encode(key)
        PyBytes_AsStringAndSize(key, &kbuf, &klen)

        # Use low-level cursor APIs for performance, since this method could
        # be a hot-spot. Another idea is to use a cursor cache or a shared
        # cursor context. Or the method could accept a cursor as a parameter.
        lsm_csr_open(self.db, &pcursor)
        try:
            rc = lsm_csr_seek(pcursor, <void *>kbuf, klen, seek_method)
            if rc == LSM_OK and lsm_csr_valid(pcursor):
                rc = lsm_csr_value(pcursor, <const void **>(&vbuf), &vlen)
                if rc == LSM_OK:
                    value = vbuf[:vlen]
                    if IS_PY3K:
                        try:
                            return value.decode('utf-8')
                        except UnicodeDecodeError:
                            pass
                    return value
            raise KeyError(key)
        finally:
            lsm_csr_close(pcursor)

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
        first = start is None
        last = end is None
        one_empty = (first and not last) or (last and not first)
        none_empty = not first and not last
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
            char *kbuf
            Py_ssize_t klen

        if IS_PY3K:
            key = encode(key)
        PyBytes_AsStringAndSize(key, &kbuf, &klen)
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
            char *sb
            char *eb
            Py_ssize_t sblen, eblen

        if IS_PY3K:
            start = encode(start)
            end = encode(end)
        PyBytes_AsStringAndSize(start, &sb, &sblen)
        PyBytes_AsStringAndSize(end, &eb, &eblen)

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
            value = encode(self[key])
        except KeyError:
            ivalue = 0
        else:
            ivalue = struct.unpack('>q', value)[0]
        ivalue += 1
        self[key] = struct.pack('>q', ivalue)
        return ivalue

    cpdef flush(self):
        """Flush the in-memory tree to disk, creating a new segment."""
        _check(lsm_flush(self.db))

    cpdef work(self, int nmerge=1, int nkb=4096):
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

    cpdef checkpoint(self, int nkb):
        """Write to the database file header."""
        _check(lsm_checkpoint(self.db, &nkb))

    cpdef begin(self):
        """
        Begin a transaction. Transactions can be nested.

        .. note::

            In most cases it is preferable to use the :py:meth:`transaction`
            context manager/decorator.
        """
        self.transaction_depth += 1
        _check(lsm_begin(self.db, self.transaction_depth))

    cpdef bint commit(self):
        """
        Commit the inner-most transaction.

        :returns: Boolean indicating whether the changes were commited.
        """
        if self.transaction_depth > 0:
            self.transaction_depth -= 1
            _check(lsm_commit(self.db, self.transaction_depth))
            return True
        return False

    cpdef bint rollback(self, keep_transaction=True):
        """
        Rollback the inner-most transaction. If `keep_transaction` is `True`,
        then the transaction will remain open after the changes were rolled
        back.

        :param bool keep_transaction: Whether the transaction will remain open
            after the changes are rolled back (default=True).
        :returns: Boolean indicating whether the changes were rolled back.
        """
        if self.transaction_depth > 0:
            if not keep_transaction:
                self.transaction_depth -= 1
            _check(lsm_rollback(self.db, self.transaction_depth))
            return True
        return False

    def transaction(self):
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
        return Transaction(self)

    cpdef cursor(self, bint reverse=False):
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
        return Cursor(self, reverse)


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

    cpdef open(self):
        """
        Open the cursor. In general this method does not need to be called
        by applications, as it is called automatically when a
        :py:class:`Cursor` is instantiated.
        """
        if self.is_open:
            return False

        lsm_csr_open(self.lsm.db, &self.cursor)
        self.is_open = True
        return True

    cpdef close(self):
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
            return False

        lsm_csr_close(self.cursor)
        self.is_open = False
        return True

    def __enter__(self):
        """
        Expose the cursor as a context manager. After the wrapped block,
        the cursor will be closed, which is very important.
        """
        self.open()
        if self._reverse:
            self.last()
        else:
            self.first()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

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
            char *kbuf
            int rc, res
            Py_ssize_t klen

        if IS_PY3K:
            key = encode(key)
        PyBytes_AsStringAndSize(key, &kbuf, &klen)

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
            char *kbuf
            Py_ssize_t klen
            int rc

        if IS_PY3K:
            key = encode(key)
        PyBytes_AsStringAndSize(key, &kbuf, &klen)

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
        key = k[:klen]
        if IS_PY3K:
            try:
                return key.decode('utf-8')
            except UnicodeDecodeError:
                pass
        return key

    cdef inline _value(self):
        """Return the value at the cursor's current position."""
        cdef:
            char *v
            int vlen

        lsm_csr_value(self.cursor, <const void **>(&v), &vlen)
        value = v[:vlen]
        if IS_PY3K:
            try:
                return value.decode('utf-8')
            except UnicodeDecodeError:
                pass
        return value

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

    def __init__(self, lsm):
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
                self.lsm.rollback(False)
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
        rc = self.lsm.commit()
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
        return self.lsm.rollback(keep_transaction=begin)


include "lsm.pxi"
