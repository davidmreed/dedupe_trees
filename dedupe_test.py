#!/usr/bin/env python

import unittest
import tempfile
import os
from dedupe import *

class DummyEntry(object):
    def __init__(self, path, digest = None, source = None):
        self.path = path
        self.digest = digest
        self.source = source

    def __repr__(self):
        return self.path

    def get_digest(self):
        return self.digest

    def get_size(self):
        return self.digest


class DummySource(Source):
    def __init__(self, files):
        self.files = files
        self.path = 'TestSource'

    def walk(self, ctx):
        for f in self.files:
            ctx.add_entry(f)


class DummySink(DuplicateFileSink):
    def __init__(self):
        self.sunk = []

    def sink(self, files):
        self.sunk.extend(files)


class DummyResolver(DuplicateResolver):
    def __init__(self, key):
        self.key = key

    def resolve(self, flist):
        return filter(lambda x: x.path != self.key, flist), filter(lambda x: x.path == self.key, flist)


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


class test_PathLengthDuplicateResolver(unittest.TestCase):
    def test_Sorting(self):
        source_one = DummyEntry('test1')
        source_two = DummyEntry(os.path.join('root', 'test2'))
        source_three = DummyEntry('test3')

        file_one = DummyEntry(os.path.join(source_one.path, 'sub1', 'file1'), source = source_one)
        file_two = DummyEntry(os.path.join(source_two.path, 'file2'), source = source_two)
        file_three = DummyEntry(os.path.join(source_three.path, 'sub3', 'sub33', 'file3'), source = source_three)

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
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.files = []
        for i in range(0, 10):
            (handle, path) = tempfile.mkstemp(dir=self.temp_dir)

            f = os.fdopen(handle, 'w')
            f.write('test')
            f.close()

            self.files.append(path)

    def test_Deletion(self):
        s = DeleteDuplicateFileSink()

        s.sink([DummyEntry(f) for f in self.files[:3]])

        for i in range(0, 10):
            if i < 3:
                self.assertFalse(os.path.exists(self.files[i]))
            else:
                self.assertTrue(os.path.exists(self.files[i]))

        s.sink([DummyEntry(f) for f in self.files[3:]])

        self.assertEquals(len(os.listdir(self.temp_dir)), 0)

    def tearDown(self):
        for cwd, subdirs, fs in os.walk(self.temp_dir, topdown=False):
            for f in fs:
                os.unlink(os.path.join(cwd, f))
            for d in subdirs:
                os.rmdir(os.path.join(cwd, d))

        os.rmdir(self.temp_dir)


class test_SequesterDuplicateFileSink(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.files = []
        for i in range(0, 10):
            (handle, path) = tempfile.mkstemp(dir=self.temp_dir)

            f = os.fdopen(handle, 'w')
            f.write('test')
            f.close()

            self.files.append(path)

        self.temp_dir_two = tempfile.mkdtemp()

    def test_Sequestration(self):
        s = SequesterDuplicateFileSink(self.temp_dir_two)

        s.sink([DummyEntry(f) for f in self.files])

        for f in self.files:
            self.assertTrue(os.path.exists(os.path.join(self.temp_dir_two, *f.split(os.path.sep))))
            self.assertFalse(os.path.exists(f))

    def tearDown(self):
        for cwd, subdirs, fs in os.walk(self.temp_dir, topdown=False):
            for f in fs:
                os.unlink(os.path.join(cwd, f))
            for d in subdirs:
                os.rmdir(os.path.join(cwd, d))

        for cwd, subdirs, fs in os.walk(self.temp_dir_two, topdown=False):
            for f in fs:
                os.unlink(os.path.join(cwd, f))
            for d in subdirs:
                os.rmdir(os.path.join(cwd, d))

        os.rmdir(self.temp_dir)
        os.rmdir(self.temp_dir_two)



class test_FileEntry(unittest.TestCase):
    pass


class test_FileCatalog(unittest.TestCase):
    def test_FileCatalog(self):
        c = FileCatalog(lambda x: x)

        c.add_entry('test')
        self.assertEquals(c.get_groups(), [])

        c.add_entry('test')
        self.assertEquals(c.get_groups(), [['test', 'test']])

        c.add_entry('foo')
        self.assertEquals(c.get_groups(), [['test', 'test']])
        c.add_entry('foo')
        self.assertEquals(c.get_groups(), [['test', 'test'], ['foo', 'foo']])


class test_Source(unittest.TestCase):
    pass


class test_DeduplicateOperation(unittest.TestCase):
    def test_DDO(self):
        r1 = DummyResolver('test1')
        r2 = DummyResolver('test2')
        r3 = DummyResolver('test3')
        f1 = DummyEntry('test1', digest='test')
        f2 = DummyEntry('test2', digest='test')
        f3 = DummyEntry('test3', digest='test')
        f4 = DummyEntry('test4', digest='test')
        f5 = DummyEntry('test5', digest='test5')
        rs = [r1, r2, r3]
        so = DummySource([f1, f2, f3, f4])

        s = DummySink()
        o = DeduplicateOperation([DummySource([f1, f2, f3, f4, f5])], [r1], s)
        o.run()
        self.assertEquals(s.sunk, [f1])

        s = DummySink()
        o = DeduplicateOperation([DummySource([f1, f2, f3, f4, f5])], [r1, r2], s)
        o.run()
        self.assertEquals(s.sunk, [f1, f2])

        s = DummySink()
        o = DeduplicateOperation([DummySource([f1, f2, f3, f4, f5])], [r1, r2, r3], s)
        o.run()
        self.assertEquals(s.sunk, [f1, f2, f3])

        s = DummySink()
        o = DeduplicateOperation([DummySource([f1, f2, f3, f4, f5])], [], s)
        o.run()
        self.assertEquals(s.sunk, [])

if __name__ == '__main__':
    unittest.main()
