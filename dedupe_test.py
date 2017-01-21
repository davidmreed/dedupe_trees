#!/usr/bin/env python

import unittest
import tempfile
from dedupe import *

class DummyEntry(object):
    def __init__(self, path, source = None):
        self.path = path
        if source is not None:
            self.source = source

    def __repr__(self):
        return self.path


class test_SortBasedDuplicateResolver(unittest.TestCase):
    def setUp(self):
        self.s = SortBasedDuplicateResolver(lambda x: x, False)

    def test_EmptyList(self):
        self.assertEqual(self.s.resolve([]), ([],[]))

    def test_OneElementList(self):
        self.assertEqual(self.s.resolve(['test']), (['test'],[]))

    def test_Sorting(self):
        r = self.s.resolve(['test', 'strings', 'here'])

        self.assertEqual(r, (['here'], ['strings', 'test']))

    def test_Sorting_Multi(self):
        r = self.s.resolve(['test', 'here', 'strings', 'here'])

        self.assertEqual(r, (['here', 'here'], ['strings', 'test']))

    def test_Sorting_Reverse(self):
        self.s.reverse = True

        r = self.s.resolve(['test', 'strings', 'here'])

        self.assertEqual(r, (['test'], ['strings', 'here']))

    def test_Sorting_Equals(self):
        r = self.s.resolve(['test', 'test', 'test'])

        self.assertEqual(r, (['test', 'test', 'test'], []))


class test_AttrBasedDuplicateResolver(unittest.TestCase):
    pass


class test_PathLengthDuplicateResolver(unittest.TestCase):
    def test_Sorting(self):
        source_one = DummyEntry('test1')
        source_two = DummyEntry(os.path.join('root', 'test2'))
        source_three = DummyEntry('test3')

        file_one = DummyEntry(os.path.join(source_one.path, 'sub1', 'file1'), source_one)
        file_two = DummyEntry(os.path.join(source_two.path, 'file2'), source_two)
        file_three = DummyEntry(os.path.join(source_three.path, 'sub3', 'sub33', 'file3'), source_three)

        r = PathLengthDuplicateResolver().resolve([file_one, file_two, file_three])

        self.assertEqual(r, ([file_two], [file_one, file_three]))


class test_CopyPatternDuplicateResolver(unittest.TestCase):
    def test_Patterns(self):
        file_one = DummyEntry('Copy of test')
        file_two = DummyEntry('test')
        file_three = DummyEntry('test copy 3.txt')

        r = CopyPatternDuplicateResolver().resolve([file_one, file_two, file_three])

        self.assertEqual(r, ([file_two], [file_one, file_three]))


class test_DeleteDuplicateFileSink(unittest.TestCase):
    pass


class test_SequesterDuplicateFileSink(unittest.TestCase):
    pass


class test_OutputOnlyDuplicateFileSink(unittest.TestCase):
    pass


class test_FileEntry(unittest.TestCase):
    pass


class test_FileCatalog(unittest.TestCase):
    pass


class test_Source(unittest.TestCase):
    pass


class test_DeduplicateOperation(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
