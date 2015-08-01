from libc.stdlib cimport free, malloc


cdef extern from "src/lsm.h":
    ctypedef struct lsm_db
    ctypedef struct lsm_env
    ctypedef struct lsm_cursor
    ctypedef struct lsm_file

    ctypedef long long int lsm_i64

    cdef int LSM_LOCK_UNLOCK = 0
    cdef int LSM_LOCK_SHARED = 1
    cdef int LSM_LOCK_EXCL = 2

    cdef int LSM_OPEN_READONLY = 0x0001

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

    # The following values may be passed as the second argument to lsm_config().
    #
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
    #
    # LSM_CONFIG_PAGE_SIZE:
    #   A read/write integer parameter. This parameter may only be set before
    #   lsm_open() has been called.
    #
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
    #
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
    #
    # LSM_CONFIG_AUTOWORK:
    #   A read/write integer parameter.
    #
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
    #
    # LSM_CONFIG_MMAP:
    #   A read/write integer parameter. If this value is set to 0, then the
    #   database file is accessed using ordinary read/write IO functions. Or,
    #   if it is set to 1, then the database file is memory mapped and accessed
    #   that way. If this parameter is set to any value N greater than 1, then
    #   up to the first N KB of the file are memory mapped, and any remainder
    #   accessed using read/write IO.
    #
    #   The default value is 1 on 64-bit platforms and 32768 on 32-bit platforms.
    #
    #
    # LSM_CONFIG_USE_LOG:
    #   A read/write boolean parameter. True (the default) to use the log
    #   file normally. False otherwise.
    #
    # LSM_CONFIG_AUTOMERGE:
    #   A read/write integer parameter. The minimum number of segments to
    #   merge together at a time. Default value 4.
    #
    # LSM_CONFIG_MAX_FREELIST:
    #   A read/write integer parameter. The maximum number of free-list
    #   entries that are stored in a database checkpoint (the others are
    #   stored elsewhere in the database).
    #
    #   There is no reason for an application to configure or query this
    #   parameter. It is only present because configuring a small value
    #   makes certain parts of the lsm code easier to test.
    #
    # LSM_CONFIG_MULTIPLE_PROCESSES:
    #   A read/write boolean parameter. This parameter may only be set before
    #   lsm_open() has been called. If true, the library uses shared-memory
    #   and posix advisory locks to co-ordinate access by clients from within
    #   multiple processes. Otherwise, if false, all database clients must be
    #   located in the same process. The default value is true.
    #
    # LSM_CONFIG_SET_COMPRESSION:
    #   Set the compression methods used to compress and decompress database
    #   content. The argument to this option should be a pointer to a structure
    #   of type lsm_compress. The lsm_config() method takes a copy of the
    #   structures contents.
    #
    #   This option may only be used before lsm_open() is called. Invoking it
    #   after lsm_open() has been called results in an LSM_MISUSE error.
    #
    # LSM_CONFIG_GET_COMPRESSION:
    #   Query the compression methods used to compress and decompress database
    #   content.
    #
    # LSM_CONFIG_SET_COMPRESSION_FACTORY:
    #   Configure a factory method to be invoked in case of an LSM_MISMATCH
    #   error.
    #
    # LSM_CONFIG_READONLY:
    #   A read/write boolean parameter. This parameter may only be set before
    #   lsm_open() is called.

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


cdef class LSM(object):
    cdef lsm_db *db
    cdef readonly basestring filename
    cdef readonly bint is_open
    cdef bint open_database
    cdef readonly int transaction_depth

    def __cinit__(self):
        self.db = <lsm_db *>0
        self.is_open = False
        self.transaction_depth = 0

    def __dealloc__(self):
        if self.is_open and self.db:
            lsm_close(self.db)

    def __init__(self, filename, open_database=True):
        self.filename = filename
        self.open_database = open_database
        if self.open_database:
            self.open()

    cpdef open(self):
        cdef int rc
        if self.is_open:
            self.close()

        self.check(lsm_new(NULL, &self.db))
        self.check(lsm_open(self.db, self.filename))
        self.is_open = True

    cpdef close(self):
        cdef int rc
        if self.is_open:
            rc = lsm_close(self.db)
            if rc in (LSM_BUSY, LSM_MISUSE):
                raise IOError('Unable to close database, one or more '
                              'cursors may still be in use.')
            self.check(rc)
            self.is_open = False

    cpdef check(self, int rc):
        if rc == LSM_OK:
            return
        raise Exception('Operation failed: %s' % rc)

    def __enter__(self):
        if not self.is_open:
            self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    cpdef insert(self, basestring key, basestring value):
        cdef char *c_key = key
        cdef char *c_val = value
        self.check(lsm_insert(
            self.db,
            c_key,
            len(key),
            c_val,
            len(value)))

    cpdef fetch(self, basestring key):
        cdef lsm_cursor *cursor = <lsm_cursor *>0
        cdef char *c_key = key
        cdef char *value
        cdef char **pvalue = &value
        cdef int value_length
        cdef int rc

        self.check(lsm_csr_open(self.db, &cursor))
        self.check(lsm_csr_seek(cursor, c_key, len(key), LSM_SEEK_EQ))
        rc = lsm_csr_valid(cursor)
        if rc != 0:
            self.check(lsm_csr_value(
                cursor,
                <const void **>pvalue,
                &value_length))
            lsm_csr_close(cursor)
            return str(value[:value_length])

        raise KeyError(key)

    cpdef delete(self, basestring key):
        cdef char *c_key = key
        self.check(lsm_delete(
            self.db,
            c_key,
            len(key)))

    cpdef delete_range(self, basestring start, basestring end):
        """
        Delete a range of keys, though the start and end keys themselves
        are not deleted.
        """
        cdef char *c_start = start
        cdef char *c_end = end
        self.check(lsm_delete_range(
            self.db,
            c_start,
            len(start),
            c_end,
            len(end)))

    def __getitem__(self, basestring key):
        return self.fetch(key)

    def __setitem__(self, basestring key, basestring value):
        self.insert(key, value)

    def __delitem__(self, key):
        if isinstance(key, slice):
            self.delete_range(key.start, key.stop)
        else:
            self.delete(key)

    cpdef begin(self):
        self.transaction_depth += 1
        self.check(lsm_begin(self.db, self.transaction_depth))

    cpdef commit(self):
        self.transaction_depth -= 1
        self.check(lsm_commit(self.db, self.transaction_depth))

    cpdef rollback(self, keep_transaction=True):
        if not keep_transaction:
            self.transaction_depth -= 1
        self.check(lsm_rollback(self.db, self.transaction_depth))

    def transaction(self):
        return Transaction(self)

    def commit_on_success(self, fn):
        def wrapper(*args, **kwargs):
            with self.transaction():
                return fn(*args, **kwargs)
        return wrapper

    cpdef cursor(self):
        return Cursor(self)


cdef class Cursor(object):
    cdef LSM lsm
    cdef lsm_cursor *cursor
    cdef bint is_open
    cdef bint _consumed

    def __cinit__(self, LSM lsm):
        self.lsm = lsm
        self.cursor = <lsm_cursor *>0
        lsm_csr_open(self.lsm.db, &self.cursor)
        self.is_open = True
        self._consumed = False

    def __dealloc__(self):
        if self.is_open:
            self.close()

    cpdef open(self):
        if not self.is_open:
            lsm_csr_open(self.lsm.db, &self.cursor)
            self.is_open = True

    cpdef close(self):
        if self.is_open:
            lsm_csr_close(self.cursor)
            self.is_open = False

    def __enter__(self):
        self.open()
        self.first()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        self._consumed = False
        return self

    def __next__(self):
        pass

    cpdef seek(self, basestring key, int method=LSM_SEEK_EQ):
        cdef int rc
        self.lsm.check(lsm_csr_seek(
            self.cursor,
            <void *>key,
            len(key),
            method))
        rc = lsm_csr_valid(self.cursor)
        if not rc:
            raise KeyError(key)

    cpdef first(self):
        self.lsm.check(lsm_csr_first(self.cursor))

    cpdef last(self):
        self.lsm.check(lsm_csr_last(self.cursor))

    cpdef next(self):
        cdef int rc
        self.lsm.check(lsm_csr_next(self.cursor))
        rc = lsm_csr_valid(self.cursor)
        if not rc:
            raise StopIteration

    cpdef previous(self):
        cdef int rc
        self.lsm.check(lsm_csr_prev(self.cursor))
        rc = lsm_csr_valid(self.cursor)
        if not rc:
            raise StopIteration

    cpdef basestring key(self):
        cdef char *k
        cdef int klen
        lsm_csr_key(self.cursor, <const void **>(&k), &klen)
        return str(k[:klen])

    cpdef basestring value(self):
        cdef char *v
        cdef int vlen
        lsm_csr_value(self.cursor, <const void **>(&v), &vlen)
        return str(v[:vlen])


cdef class Transaction(object):
    cdef LSM lsm

    def __init__(self, lsm):
        self.lsm = lsm

    def __enter__(self):
        self.lsm.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.lsm.rollback(keep_transaction=False)
        else:
            try:
                self.lsm.commit()
            except:
                self.lsm.rollback(keep_transaction=False)
                raise


def test_me():
    cdef lsm_db *db = <lsm_db *>0
    cdef lsm_cursor *cursor = <lsm_cursor *>0
    cdef char *key
    cdef char **keyPtr = &key
    cdef int keyLen
    lsm_new(NULL, &db)
    lsm_open(db, 'tmp2.lsm')
    lsm_insert(db, 'foo', 3, 'nugget', 6)
    lsm_insert(db, 'bar', 3, 'zaizee', 6)
    lsm_csr_open(db, &cursor)
    lsm_csr_first(cursor)
    lsm_csr_key(cursor, <const void **>keyPtr, &keyLen)
    print 'key len=%s' % keyLen
    print 'key=%s' % str(key[:keyLen])
    lsm_csr_close(cursor)
    lsm_close(db)
