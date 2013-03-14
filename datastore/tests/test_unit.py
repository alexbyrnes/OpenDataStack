import unittest

import datastore.db as db


class TestTypeGetters(unittest.TestCase):
    def test_list(self):
        assert db._get_list(None) is None
        assert db._get_list([]) == []
        assert db._get_list('') == []
        assert db._get_list('foo') == ['foo']
        assert db._get_list('foo, bar') == ['foo', 'bar']
        assert db._get_list('foo_"bar, baz') == ['foo_"bar', 'baz']
        assert db._get_list('"foo", "bar"') == ['foo', 'bar']
        assert db._get_list(u'foo, bar') == ['foo', 'bar']
        assert db._get_list(['foo', 'bar']) == ['foo', 'bar']
        assert db._get_list([u'foo', u'bar']) == ['foo', 'bar']
        assert db._get_list(['foo', ['bar', 'baz']]) == ['foo', ['bar', 'baz']]

    def test_bool(self):
        assert db._get_bool(None) is False
        assert db._get_bool(False) is False
        assert db._get_bool(True) is True
        assert db._get_bool('', True) is True
        assert db._get_bool('', False) is False
        assert db._get_bool('True') is True
        assert db._get_bool('False') is False
        assert db._get_bool('1') is True
        assert db._get_bool('0') is False
        assert db._get_bool('on') is True
        assert db._get_bool('off') is False
