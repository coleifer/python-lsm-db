import os
import sys
import tempfile
import threading
import unittest

try:
    import lsm
except ImportError:
    sys.stderr.write('Unable to import `lsm`. Make sure it is properly '
                     'installed.\n')
    sys.stderr.flush()
    raise


def b(s):
    return s.encode('utf-8') if not isinstance(s, bytes) else s


class BaseTestLSM(unittest.TestCase):
    def setUp(self):
        self.filename = tempfile.mktemp()
        self.db = lsm.LSM(self.filename)

    def tearDown(self):
        if self.db.is_open:
            while self.db.transaction_depth > 0:
                self.db.rollback(False)
            self.db.close()
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def assertMissing(self, key):
        self.assertRaises(KeyError, lambda: self.db[key])

    def assertBEqual(self, lhs, rhs):
        if isinstance(lhs, list):
            if lhs and isinstance(lhs[0], tuple):
                self.assertEqual(lhs, [tuple(b(si) for si in i)
                                       for i in rhs])
            else:
                self.assertEqual(lhs, [b(i) for i in rhs])
        else:
            self.assertEqual(lhs, b(rhs))


class TestLSM(BaseTestLSM):
    def test_db_open_close(self):
        self.db['foo'] = b('bar')

    def test_dict_api(self):
        self.db['k1'] = 'v1'
        self.db['k2'] = 'v2'
        self.db['k3'] = 'v3'
        self.assertBEqual(self.db['k1'], 'v1')
        self.assertBEqual(self.db['k3'], 'v3')
        self.assertMissing('k4')

        del self.db['k1']
        self.assertMissing('k1')

        # No error is raised trying to delete a key that doesn't exist.
        del self.db['k1']

        self.assertTrue('k2' in self.db)
        self.assertFalse('k1' in self.db)

        self.assertBEqual(self.db['k22', lsm.SEEK_GE], 'v3')
        self.assertBEqual(self.db['k22', lsm.SEEK_LE], 'v2')

        self.db.update({'foo': 'bar', 'nug': 'nizer'})
        self.assertBEqual(self.db['foo'], 'bar')
        self.assertBEqual(self.db['nug'], 'nizer')

    def test_keys_values(self):
        for i in range(1, 5):
            self.db['k%s' % i] = 'v%s' % i

        keys = [key for key in self.db.keys()]
        self.assertBEqual(keys, ['k1', 'k2', 'k3', 'k4'])

        keys = [key for key in self.db.keys(True)]
        self.assertBEqual(keys, ['k4', 'k3', 'k2', 'k1'])

        values = [value for value in self.db.values()]
        self.assertBEqual(values, ['v1', 'v2', 'v3', 'v4'])

        values = [value for value in self.db.values(True)]
        self.assertBEqual(values, ['v4', 'v3', 'v2', 'v1'])

    def test_fetch(self):
        self.db['k1'] = 'v1'
        self.db['k2'] = 'v2'
        self.db['k3'] = 'v3'
        self.assertBEqual(self.db.fetch('k2'), 'v2')
        self.assertBEqual(self.db.fetch('k2', lsm.SEEK_LE), 'v2')
        self.assertBEqual(self.db.fetch('k2', lsm.SEEK_GE), 'v2')

        self.assertRaises(KeyError, self.db.fetch, 'k22')
        self.assertBEqual(self.db.fetch('k22', lsm.SEEK_LE), 'v2')
        self.assertBEqual(self.db.fetch('k22', lsm.SEEK_GE), 'v3')

    def test_fetch_bulk(self):
        self.db.update({'k1': 'v1', 'k2': 'v2', 'k3': 'v3', 'k4': 'v4'})
        res = self.db.fetch_bulk(['k1', 'k2', 'k3', 'kx'])
        self.assertEqual(res, {'k1': b'v1', 'k2': b'v2', 'k3': b'v3'})

        res = self.db.fetch_bulk(['k4'])
        self.assertEqual(res, {'k4': b'v4'})

        res = self.db.fetch_bulk(['foo', 'bar'])
        self.assertEqual(res, {})

    def assertIterEqual(self, i, expected):
        self.assertBEqual(list(i), expected)

    def test_fetch_range(self):
        results = []
        for i in range(1, 10):
            self.db['k%s' % i] = 'v%s' % i
            results.append(('k%s' % i, 'v%s' % i))

        res = self.db['k2':'k5']
        self.assertIterEqual(res, [
            ('k2', 'v2'),
            ('k3', 'v3'),
            ('k4', 'v4'),
            ('k5', 'v5'),
        ])

        # Empty start.
        res = self.db[:'k3']
        self.assertIterEqual(res, [
            ('k1', 'v1'),
            ('k2', 'v2'),
            ('k3', 'v3'),
        ])

        # Empty end.
        res = self.db['k7':]
        self.assertIterEqual(res, [
            ('k7', 'v7'),
            ('k8', 'v8'),
            ('k9', 'v9'),
        ])

        # Missing end.
        res = self.db['k7':'k88']
        self.assertIterEqual(res, [
            ('k7', 'v7'),
            ('k8', 'v8'),
        ])

        # Missing start.
        res = self.db['k33':'k5']
        self.assertIterEqual(res, [
            ('k4', 'v4'),
            ('k5', 'v5'),
        ])

        # Start exceeds highest key.
        res = self.db['xx':'yy']
        self.assertIterEqual(res, [])

        res = self.db['xx':]
        self.assertIterEqual(res, [])

        # End preceds lowest key.
        res = self.db['aa':'bb']
        self.assertIterEqual(res, [])

        res = self.db[:'bb']
        self.assertIterEqual(res, [])

        res = self.db[:]
        self.assertIterEqual(res, results)

    def test_fetch_range_reverse(self):
        results = []
        for i in range(1, 10):
            self.db['k%s' % i] = 'v%s' % i
            results.append(('k%s' % i, 'v%s' % i))

        res = self.db['k5':'k2':True]
        self.assertIterEqual(res, [
            ('k5', 'v5'),
            ('k4', 'v4'),
            ('k3', 'v3'),
            ('k2', 'v2'),
        ])

        # Empty end.
        res = self.db['k7'::True]
        self.assertIterEqual(res, [
            ('k9', 'v9'),
            ('k8', 'v8'),
            ('k7', 'v7'),
        ])

        # Empty start.
        res = self.db[:'k3':True]
        self.assertIterEqual(res, [
            ('k3', 'v3'),
            ('k2', 'v2'),
            ('k1', 'v1'),
        ])

        # Missing start.
        res = self.db['k88':'k7':True]
        self.assertIterEqual(res, [
            ('k8', 'v8'),
            ('k7', 'v7'),
        ])

        # Missing end.
        res = self.db['k5':'k33':True]
        self.assertIterEqual(res, [
            ('k5', 'v5'),
            ('k4', 'v4'),
        ])

        # End exceeds highest key.
        res = self.db[:'xx':True]
        self.assertIterEqual(res, list(reversed(results)))

        # Start exceeds highest key.
        res = self.db['yy':'xx':True]
        self.assertIterEqual(res, [])

        res = self.db['xx'::True]
        self.assertIterEqual(res, [])

        # End preceds lowest key.
        res = self.db['bb':'aa':True]
        self.assertIterEqual(res, [])

        res = self.db[:'bb':True]
        self.assertIterEqual(res, [])

        # Missing both.
        res = self.db[::True]
        self.assertIterEqual(res, list(reversed(results)))

    def test_fetch_range_implicit(self):
        for i in range(1, 10):
            self.db['k%s' % i] = 'v%s' % i

        self.assertIterEqual(self.db['k7':'k4'], [
            ('k7', 'v7'),
            ('k6', 'v6'),
            ('k5', 'v5'),
            ('k4', 'v4'),
        ])

        self.assertIterEqual(self.db['k4':'k7'], [
            ('k4', 'v4'),
            ('k5', 'v5'),
            ('k6', 'v6'),
            ('k7', 'v7'),
        ])

    def test_delete_range(self):
        for i in range(1, 10):
            self.db['k%s' % i] = 'v%s' % i

        # delete_range does not include the start/end keys.
        del self.db['k3':'k7']
        self.assertBEqual(self.db['k3'], 'v3')
        self.assertBEqual(self.db['k7'], 'v7')

        for key in ['k4', 'k5', 'k6']:
            self.assertMissing(key)

        # Missing start key.
        del self.db['k4':'k8']
        self.assertBEqual(self.db['k8'], 'v8')
        self.assertMissing('k7')

        # Invalid start key.
        del self.db['k0':'k2']
        self.assertBEqual(self.db['k2'], 'v2')
        self.assertMissing('k1')

        self.assertBEqual(self.db['k9'], 'v9')

        # Invalid end key.
        del self.db['k8':'xx']
        self.assertMissing('k9')

        # Invalid start and end keys low.
        del self.db['aa':'bb']

        with self.db.cursor() as cursor:
            accum = [val for val in cursor]
        self.assertBEqual(accum, [
            ('k2', 'v2'),
            ('k3', 'v3'),
            ('k8', 'v8'),
        ])

        # Invalid start and end keys high.
        del self.db['xx':'yy']

        with self.db.cursor() as cursor:
            accum = [val for val in cursor]
        self.assertBEqual(accum, [
            ('k2', 'v2'),
            ('k3', 'v3'),
            ('k8', 'v8'),
        ])

    def test_iter_database(self):
        for i in range(1, 5):
            self.db['k%s' % i] = 'v%s' % i

        items = list(self.db)
        self.assertIterEqual(items, [
            ('k1', 'v1'),
            ('k2', 'v2'),
            ('k3', 'v3'),
            ('k4', 'v4'),
        ])

        items = list(reversed(self.db))
        self.assertIterEqual(items, [
            ('k4', 'v4'),
            ('k3', 'v3'),
            ('k2', 'v2'),
            ('k1', 'v1'),
        ])

    def test_incr(self):
        self.assertEqual(self.db.incr('i0'), 1)
        self.assertEqual(self.db.incr('i0'), 2)
        self.assertEqual(self.db.incr('i0'), 3)

    def test_data_types(self):
        key = b('k\xe2\x80\x941')
        self.db[key] = key
        ret = self.db[key]
        self.assertEqual(ret, key)

        self.assertRaises(KeyError, lambda: self.db[1])
        self.assertRaises(KeyError, lambda: self.db[1.0])
        self.assertRaises(TypeError, lambda: self.db.insert(key, None))


class TestTransactions(BaseTestLSM):
    def assertDepth(self, value):
        self.assertEqual(self.db.transaction_depth, value)

    def test_transaction_apis(self):
        self.db.begin()
        self.db['k1'] = 'v1'
        self.assertTrue(self.db.rollback(keep_transaction=True))
        self.assertDepth(1)
        self.assertMissing('k1')

        self.db['k2'] = 'v2'
        self.assertTrue(self.db.rollback(keep_transaction=False))
        self.assertDepth(0)
        self.assertMissing('k2')

        # Cannot commit, no transaction is open.
        self.assertFalse(self.db.commit())

        # Cannot rollback, no transaction is open.
        self.assertFalse(self.db.rollback())

        self.db.begin()  # 1.
        self.db['k1'] = 'v1'

        self.db.begin()  # 2.
        self.db['k1'] = 'v1-1'
        self.db.rollback()
        self.assertBEqual(self.db['k1'], 'v1')
        self.assertDepth(2)
        self.db['k1'] = 'v1-2'
        self.db.commit()

        self.assertDepth(1)
        self.db.rollback(False)
        self.assertMissing('k1')
        self.assertDepth(0)

    def test_transaction_context_manager(self):
        with self.db.transaction():
            self.db['k1'] = 'v1'
            self.assertDepth(1)

        self.assertBEqual(self.db['k1'], 'v1')
        self.assertDepth(0)

        with self.db.transaction() as txn:
            self.db['k2'] = 'v2'
            self.assertTrue(txn.rollback())
            self.assertDepth(1)

            self.db['k3'] = 'v3'
            self.assertTrue(txn.rollback())
            self.assertDepth(1)

            self.db['k4'] = 'v4'

        self.assertDepth(0)
        self.assertMissing('k2')
        self.assertMissing('k3')
        self.assertBEqual(self.db['k4'], 'v4')

    def test_transaction_nesting(self):
        with self.db.transaction() as txn1:
            with self.db.transaction() as txn2:
                self.db['k0'] = 'v0'

                with self.db.transaction() as txn3:
                    self.db['k1'] = 'v1'
                    del self.db['k0']
                    self.assertDepth(3)
                    txn3.rollback()

                self.assertMissing('k1')
                self.assertBEqual(self.db['k0'], 'v0')
                self.db['k2'] = 'v2'
                del self.db['k0']

            self.db['k3'] = 'v3'
            self.assertDepth(1)

        self.assertMissing('k0')
        self.assertMissing('k1')
        self.assertBEqual(self.db['k2'], 'v2')
        self.assertBEqual(self.db['k3'], 'v3')

    def test_transaction_nesting_2(self):
        with self.db.transaction() as txn1:
            self.db['k1'] = 'v1'
            with self.db.transaction() as txn2:
                self.db['k2'] = 'v2'
                txn2.commit()

                self.db['k2'] = 'v2-1'

                with self.db.transaction() as txn3:
                    self.db['k2'] = 'v2-2'
                    txn3.rollback()

                self.assertBEqual(self.db['k2'], 'v2-1')
                txn2.rollback()

            self.assertBEqual(self.db['k2'], 'v2')

        self.assertDepth(0)
        self.assertBEqual(self.db['k1'], 'v1')
        self.assertBEqual(self.db['k2'], 'v2')

    def test_transaction_decorator(self):
        class FuncError(Exception):
            pass

        @self.db.transaction()
        def txn_func(error_out=False, **kwargs):
            for key, value in kwargs.items():
                self.db[key] = value
            if error_out:
                raise FuncError()

        txn_func(k1='v1', k2='v2')
        self.assertBEqual(self.db['k1'], 'v1')
        self.assertBEqual(self.db['k2'], 'v2')

        self.assertRaises(FuncError, txn_func, error_out=True, k1='v1-1')
        self.assertBEqual(self.db['k1'], 'v1')
        self.assertBEqual(self.db['k2'], 'v2')


class TestCursors(BaseTestLSM):
    def setUp(self):
        super(TestCursors, self).setUp()
        self.db['bb'] = 'bbb'
        self.db['gg'] = 'ggg'
        self.db['aa'] = 'aaa'
        self.db['dd'] = 'ddd'
        self.db['zz'] = 'zzz'
        self.db['ee'] = 'eee'
        self.db['bbb'] = 'bbb'

    def test_cursor_simple(self):
        with self.db.cursor() as cursor:
            items = list(cursor)

        self.assertBEqual(items, [
            ('aa', 'aaa'),
            ('bb', 'bbb'),
            ('bbb', 'bbb'),
            ('dd', 'ddd'),
            ('ee', 'eee'),
            ('gg', 'ggg'),
            ('zz', 'zzz'),
        ])

    def test_cursor_reversed(self):
        with self.db.cursor(True) as cursor:
            items = list(cursor)

        self.assertBEqual(items, [
            ('zz', 'zzz'),
            ('gg', 'ggg'),
            ('ee', 'eee'),
            ('dd', 'ddd'),
            ('bbb', 'bbb'),
            ('bb', 'bbb'),
            ('aa', 'aaa'),
        ])

    def test_seek_and_iterate(self):
        with self.db.cursor() as cursor:
            cursor.seek('dd', lsm.SEEK_GE)
            items = list(cursor)

        self.assertBEqual(items, [
            ('dd', 'ddd'),
            ('ee', 'eee'),
            ('gg', 'ggg'),
            ('zz', 'zzz'),
        ])

        with self.db.cursor() as cursor:
            cursor.seek('dd', lsm.SEEK_EQ)
            try:
                cursor.next()
            except Exception as exc:
                if (sys.version_info > (3,0)):
                    self.assertEqual(exc.args[0], 'Misuse')
                else:
                    self.assertEqual(exc.message, 'Misuse')
            else:
                raise AssertionError('Mis-use exception not raised.')

    def test_seek_and_iterate_reverse(self):
        with self.db.cursor(True) as cursor:
            cursor.seek('dd', lsm.SEEK_LE)
            items = list(cursor)

        self.assertBEqual(items, [
            ('dd', 'ddd'),
            ('bbb', 'bbb'),
            ('bb', 'bbb'),
            ('aa', 'aaa'),
        ])

        with self.db.cursor() as cursor:
            cursor.seek('dd', lsm.SEEK_EQ)
            try:
                cursor.previous()
            except Exception as exc:
                if (sys.version_info > (3,0)):
                    self.assertEqual(exc.args[0], 'Misuse')
                else:
                    self.assertEqual(exc.message, 'Misuse')
            else:
                raise AssertionError('Mis-use exception not raised.')

    def test_seek_missing(self):
        with self.db.cursor() as cursor:
            self.assertRaises(KeyError, cursor.seek, 'missing')

        with self.db.cursor(True) as cursor:
            self.assertRaises(KeyError, cursor.seek, 'missing')

    def test_seek_missing_for_iteration(self):
        with self.db.cursor() as cursor:
            cursor.seek('cccc', lsm.SEEK_GE)
            self.assertBEqual(cursor.key(), 'dd')
            self.assertBEqual(cursor.value(), 'ddd')

            items = [item for item in cursor]
            self.assertBEqual(items, [
                ('dd', 'ddd'),
                ('ee', 'eee'),
                ('gg', 'ggg'),
                ('zz', 'zzz'),
            ])

        with self.db.cursor(True) as cursor:
            cursor.seek('cccc', lsm.SEEK_LE)
            self.assertBEqual(cursor.key(), 'bbb')
            self.assertBEqual(cursor.value(), 'bbb')

            items = [item for item in cursor]
            self.assertBEqual(items, [
                ('bbb', 'bbb'),
                ('bb', 'bbb'),
                ('aa', 'aaa'),
            ])

    def test_fetch_until(self):
        with self.db.cursor() as cursor:
            cursor.seek('bbb', lsm.SEEK_GE)
            items = [item for item in cursor.fetch_until('ee')]

        self.assertBEqual(items, [
            ('bbb', 'bbb'),
            ('dd', 'ddd'),
            ('ee', 'eee'),
        ])

        # Invalid end key.
        with self.db.cursor() as cursor:
            cursor.seek('bbb', lsm.SEEK_GE)
            items = [item for item in cursor.fetch_until('ef')]

        self.assertBEqual(items, [
            ('bbb', 'bbb'),
            ('dd', 'ddd'),
            ('ee', 'eee'),
        ])

        # Invalid start key.
        with self.db.cursor() as cursor:
            cursor.seek('cccc', lsm.SEEK_GE)
            items = [item for item in cursor.fetch_until('foo')]

        self.assertBEqual(items, [
            ('dd', 'ddd'),
            ('ee', 'eee'),
        ])

        # Start key precedes lowest key.
        with self.db.cursor() as cursor:
            cursor.seek('a', lsm.SEEK_GE)
            items = [item for item in cursor.fetch_until('bx')]

        self.assertBEqual(items, [
            ('aa', 'aaa'),
            ('bb', 'bbb'),
            ('bbb', 'bbb'),
        ])

        # End with key that exceeds highest key.
        with self.db.cursor() as cursor:
            cursor.seek('dd', lsm.SEEK_GE)
            items = [item for item in cursor.fetch_until('zzzzzz')]

        self.assertBEqual(items, [
            ('dd', 'ddd'),
            ('ee', 'eee'),
            ('gg', 'ggg'),
            ('zz', 'zzz'),
        ])

    def test_fetch_range(self):
        with self.db.cursor() as cursor:
            items = [item for item in cursor.fetch_range('bb', 'ee')]

        self.assertBEqual(items, [
            ('bb', 'bbb'),
            ('bbb', 'bbb'),
            ('dd', 'ddd'),
            ('ee', 'eee'),
        ])

        with self.db.cursor() as cursor:
            items = [item for item in cursor.fetch_range('a', 'cc')]

        self.assertBEqual(items, [
            ('aa', 'aaa'),
            ('bb', 'bbb'),
            ('bbb', 'bbb'),
        ])

        with self.db.cursor() as cursor:
            items = [item for item in cursor.fetch_range('foo', 'zzzz')]

        self.assertBEqual(items, [
            ('gg', 'ggg'),
            ('zz', 'zzz'),
        ])

        with self.db.cursor() as cursor:
            items = [item for item in cursor.fetch_range('zzzz', 'zzzzz')]

        self.assertEqual(items, [])

        with self.db.cursor() as cursor:
            items = [item for item in cursor.fetch_range('a', 'aA')]

        self.assertEqual(items, [])

        with self.db.cursor() as cursor:
            items = [item for item in cursor.fetch_range('eee', 'ba')]

        self.assertBEqual(items, [
            ('bb', 'bbb'),
            ('bbb', 'bbb'),
            ('dd', 'ddd'),
            ('ee', 'eee'),
        ])

    def test_fetch_range_reverse(self):
        with self.db.cursor(True) as cursor:
            items = [item for item in cursor.fetch_range('ee', 'bb')]

        self.assertBEqual(items, [
            ('ee', 'eee'),
            ('dd', 'ddd'),
            ('bbb', 'bbb'),
            ('bb', 'bbb'),
        ])

        with self.db.cursor(True) as cursor:
            items = [item for item in cursor.fetch_range('cc', 'a')]

        self.assertBEqual(items, [
            ('bbb', 'bbb'),
            ('bb', 'bbb'),
            ('aa', 'aaa'),
        ])

        with self.db.cursor(True) as cursor:
            items = [item for item in cursor.fetch_range('zzzz', 'foo')]

        self.assertBEqual(items, [
            ('zz', 'zzz'),
            ('gg', 'ggg'),
        ])

        with self.db.cursor(True) as cursor:
            items = [item for item in cursor.fetch_range('zzzzz', 'zzzz')]

        self.assertEqual(items, [])

        with self.db.cursor(True) as cursor:
            items = [item for item in cursor.fetch_range('aA', 'a')]

        self.assertEqual(items, [])

        with self.db.cursor(True) as cursor:
            items = [item for item in cursor.fetch_range('ba', 'eee')]

        self.assertBEqual(items, [
            ('ee', 'eee'),
            ('dd', 'ddd'),
            ('bbb', 'bbb'),
            ('bb', 'bbb'),
        ])

    def test_cursor_consumed(self):
        for reverse in (False, True):
            with self.db.cursor(reverse=reverse) as cursor:
                l1 = [item for item in cursor]
                l2 = [item for item in cursor]
                if reverse:
                    cursor.last()
                else:
                    cursor.first()
                k1 = [key for key in cursor.keys()]
                k2 = [key for key in cursor.keys()]
                if reverse:
                    cursor.last()
                else:
                    cursor.first()
                v1 = [value for value in cursor.values()]
                v2 = [value for value in cursor.values()]

            self.assertTrue(len(l1) == 7)
            self.assertEqual(l2, [])
            self.assertTrue(len(k1) == 7)
            self.assertEqual(k2, [])
            self.assertTrue(len(v1) == 7)
            self.assertEqual(v2, [])

    def test_keys_and_values(self):
        t_keys = ['aa', 'bb', 'bbb', 'dd', 'ee', 'gg', 'zz']
        t_values = ['aaa', 'bbb', 'bbb', 'ddd', 'eee', 'ggg', 'zzz']

        with self.db.cursor() as cursor:
            keys = [key for key in cursor.keys()]
            cursor.first()
            values = [value for value in cursor.values()]

        self.assertBEqual(keys, t_keys)
        self.assertBEqual(values, t_values)

        with self.db.cursor(True) as cursor:
            keys = [key for key in cursor.keys()]
            cursor.last()
            values = [value for value in cursor.values()]

        self.assertBEqual(keys, list(reversed(t_keys)))
        self.assertBEqual(values, list(reversed(t_values)))


class TestLSMOptions(BaseTestLSM):
    def test_no_open(self):
        db = lsm.LSM('test.lsm', open_database=False)
        self.assertFalse(db.is_open)
        self.assertFalse(os.path.exists('test.lsm'))

    def test_default_options(self):
        self.assertEqual(self.db.page_size, 4096)
        self.assertEqual(self.db.block_size, 1024)
        self.assertEqual(self.db.multiple_processes, 1)
        self.assertEqual(self.db.readonly, 0)
        self.assertEqual(self.db.write_safety, 1)
        self.assertEqual(self.db.autoflush, 1024)
        self.assertEqual(self.db.autowork, 1)
        self.assertEqual(self.db.automerge, 4)
        self.assertEqual(self.db.autocheckpoint, 2048)
        #self.assertTrue(self.db.mmap in (0, 1))
        self.assertEqual(self.db.transaction_log, 1)

    def test_file_options(self):
        self.db.close()
        os.unlink(self.filename)

        db = lsm.LSM(self.filename, page_size=1024, block_size=4096)
        self.assertEqual(db.page_size, 1024)
        self.assertEqual(db.block_size, 4096)

        # Page and block cannot be modified after creation.
        def set_page():
            db.page_size = 8192
        def set_block():
            db.block_size = 8192
        self.assertRaises(ValueError, set_page)
        self.assertRaises(ValueError, set_block)

        # We can, however, alter the safety level at any time.
        self.assertEqual(db.write_safety, lsm.SAFETY_NORMAL)
        db.write_safety = lsm.SAFETY_FULL
        self.assertEqual(db.write_safety, lsm.SAFETY_FULL)

        for i in range(10):
            db['k%s' % i] = 'v%s' % i

        self.assertBEqual(db['k0'], 'v0')
        self.assertBEqual(db['k9'], 'v9')
        db.close()

        db2 = lsm.LSM(self.filename, page_size=1024, block_size=4096,
                      mmap=0, transaction_log=False, write_safety=0,
                      multiple_processes=False)
        self.assertEqual(db2.page_size, 1024)
        self.assertEqual(db2.block_size, 4096)
        self.assertEqual(db2.mmap, 0)
        self.assertEqual(db2.transaction_log, 0)
        self.assertEqual(db2.write_safety, 0)
        self.assertEqual(db2.multiple_processes, 0)
        self.assertBEqual(db2['k0'], 'v0')
        self.assertBEqual(db2['k9'], 'v9')
        db2.close()

    def test_multithreading(self):
        def create_entries_thread(low, high):
            for i in range(low, high):
                self.db['k%02d' % i] = 'v%s' % i

        threads = []
        for i in range(8):
            threads.append(threading.Thread(
                target=create_entries_thread,
                args=(i * 10, i * 10 + 10)))

        [t.start() for t in threads]
        [t.join() for t in threads]

        keys = [key for key in self.db.keys()]
        self.assertEqual(len(keys), 80)
        self.assertBEqual(keys[0], 'k00')
        self.assertBEqual(keys[-1], 'k79')

        expected = ['k%02d' % i for i in range(80)]
        self.assertBEqual(keys, expected)


class TestLSMInfo(BaseTestLSM):
    def test_lsm_info(self):
        self.db.close()

        # Page size is 1KB.
        db = lsm.LSM(self.filename, page_size=1024, autocheckpoint=4)

        w0 = db.pages_written()
        r0 = db.pages_read()
        c0 = db.checkpoint_size()
        self.assertEqual(w0, 0)
        self.assertEqual(r0, 0)
        self.assertEqual(c0, 0)

        data = '0' * 1024
        for i in range(1024):
            db[str(i)] = data
            r = db[str(i)]

        w1 = db.pages_written()
        r1 = db.pages_read()
        c1 = db.checkpoint_size()
        self.assertEqual(w1, 974)
        self.assertEqual(r1, 0)  # Not sure why not increasing...
        self.assertEqual(c1, 0)


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
