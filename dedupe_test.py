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
        self.order = 1

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

class test_UtilityFunctions(unittest.TestCase):
    def test_join_paths_componentwise(self):
        # this test assumes a Linux environment
        self.assertEqual('/test/source/file', join_paths_componentwise('/test', 'source/file'))
        self.assertEqual('/test/source/file', join_paths_componentwise('/test', '/source/file'))
        self.assertEqual('/test/depth/source/file', join_paths_componentwise('/test/depth', 'source/file'))
        self.assertEqual('test/source/file', join_paths_componentwise('test', 'source/file'))
        self.assertEqual('test/source/file', join_paths_componentwise('test', '/source/file'))
        self.assertEqual('test/depth/source/file', join_paths_componentwise('test/depth', 'source/file'))

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
    def test_PathLengthSorting(self):
        source_one = DummyEntry('test1')
        source_two = DummyEntry(os.path.join('root', 'test2'))
        source_three = DummyEntry('test3')

        file_one = DummyEntry(os.path.join(source_one.path, 'sub1', 'file1'), source = source_one)
        file_two = DummyEntry(os.path.join(source_two.path, 'file2'), source = source_two)
        file_three = DummyEntry(os.path.join(source_three.path, 'sub3', 'sub33', 'file3'), source = source_three)
        file_four = DummyEntry(os.path.join(source_two.path, 'file4'), source = source_two)
        file_five = DummyEntry(os.path.join(source_one.path, 'sub1', 'file5'), source = source_three)

        # The order of entries within each list isn't guaranteed by this resolver
        r = PathLengthDuplicateResolver().resolve([file_one, file_two, file_three, file_four, file_five])
        print r
        self.assertEqual(set(r[0]), set([file_two, file_four]))
        self.assertEqual(set(r[1]), set([file_one, file_three, file_five]))


class test_CopyPatternDuplicateResolver(unittest.TestCase):
    def test_Patterns(self):
        file_one = DummyEntry('Copy of test')
        file_two = DummyEntry('test')
        file_three = DummyEntry('test copy 3.txt')

        r = CopyPatternDuplicateResolver().resolve([file_one, file_two, file_three])

        self.assertEqual(r, ([file_two], [file_one, file_three]))


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


# Below are tests that directly touch the filesystem, all of which inherit
# from test_FileSystemTestBase.

class FileSystemTestException(Exception):
    pass

class test_FileSystemTestBase(object):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        for entry in self.entry_state:
            # We'll create a file for each item in self.entry_state.
            # Each entry is a (path, contents) tuple.
            # We'll supply sensible defaults for None.

            if entry is None:
                entry = (None, None)

            (pathname, contents) = entry
            if contents is None:
                contents = 'Test'

            if pathname is None:
                (handle, path) = tempfile.mkstemp(dir=self.temp_dir)
                f = os.fdopen(handle, mode='wb')
            else:
                new_path = self.get_absolute_path(pathname)
                if not os.path.exists(os.path.dirname(new_path)):
                    os.makedirs(os.path.dirname(new_path))
                f = open(new_path, mode='wb')

            f.write(contents)
            f.close()

    def get_absolute_path(self, path):
        return os.path.join(self.temp_dir, *(os.path.splitdrive(os.path.normpath(path))[1].split(os.path.sep)))

    def check_exit_state(self, exitStates):
        # Walk the tree of files. Ensure that the states provided are a 1:1 match with the files in the directory tree
        # including contents if supplied.

        # Prepare a dictionary based on the states supplied.


        print "Checking exit states: \n"
        print exitStates
        states = {}
        for path, contents in exitStates:
             states[self.get_absolute_path(path)] = contents
        print states
        for cwd, subdirs, fs in os.walk(self.temp_dir, topdown=False):
            for f in fs:
                this_file_path = os.path.join(cwd, f)
                if states.has_key(this_file_path):
                    if states[this_file_path] is not None:
                        this_file = open(this_file_path, mode='rb')
                        contents = this_file.read()

                        self.assertEquals(states[this_file_path], contents)

                    del states[this_file_path]
                else:
                    raise FileSystemTestException("Exit state check failed: file {0} is present and should not be.".format(this_file_path))

        # There should be no further keys remaining in the dictionary

        self.assertEquals({}, states)

    def tearDown(self):
        for cwd, subdirs, fs in os.walk(self.temp_dir, topdown=False):
            for f in fs:
                os.unlink(os.path.join(cwd, f))
            for d in subdirs:
                os.rmdir(os.path.join(cwd, d))

        os.rmdir(self.temp_dir)


class test_DeleteDuplicateFileSink(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [('test ' + str(i), None) for i in range(1, 10)]
        super(test_DeleteDuplicateFileSink, self).setUp()

    def test_Deletion(self):
        s = DeleteDuplicateFileSink()

        s.sink([DummyEntry(self.get_absolute_path(f)) for (f, c) in self.entry_state[:3]])

        self.check_exit_state(self.entry_state[3:])

        s.sink([DummyEntry(self.get_absolute_path(f)) for (f, c) in self.entry_state[3:]])

        self.check_exit_state([])


class test_SequesterDuplicateFileSink(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [(os.path.join('source', 'test ' + str(i)), None) for i in range(1, 10)]
        super(test_SequesterDuplicateFileSink, self).setUp()

    def test_Sequestration(self):
        s = SequesterDuplicateFileSink(self.get_absolute_path('sink'))

        s.sink([DummyEntry(self.get_absolute_path(f)) for (f, c) in self.entry_state])

        self.check_exit_state([(s.construct_sequestered_path(self.get_absolute_path(p))[len(self.temp_dir):], None) for (p, c) in self.entry_state])


class test_Integration(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join('source1','file1'), 'Contents1'),
            (os.path.join('source1','file2'), 'Contents1'),
            (os.path.join('source1','file3'), 'Contents2'),
            (os.path.join('sources','source2', 'file4'), 'Contents1'),
            (os.path.join('sources','source2', 'file5'), 'Contents2'),
            (os.path.join('sources','source2', 'file6'), 'Contents3'),
            (os.path.join('sources','source3', 'file7'), 'Contents4'),
            (os.path.join('sources','source3', 'file8'), 'Contents5'),
            (os.path.join('sources','source4', 'file9'), 'Contents1'),
            (os.path.join('sources','source4', 'file10'), 'Contents5')
        ]
        super(test_Integration, self).setUp()


    def test_Integration_DepthAndSourceOrder_DeleteSink(self):
        o = DeduplicateOperation([Source(self.get_absolute_path('source1'), 1),
                                  Source(self.get_absolute_path(os.path.join('sources','source2')), 2),
                                  Source(self.get_absolute_path(os.path.join('sources','source3')), 3),
                                  Source(self.get_absolute_path(os.path.join('sources','source4')), 4)],
                                 [PathLengthDuplicateResolver(), SourceOrderDuplicateResolver()],
                                 DeleteDuplicateFileSink())

        o.run()

        self.check_exit_state([
            (os.path.join('source1','file1'), 'Contents1'),
            (os.path.join('source1','file2'), 'Contents1'),
            (os.path.join('source1','file3'), 'Contents2'),
            (os.path.join('sources','source2', 'file6'), 'Contents3'),
            (os.path.join('sources','source3', 'file7'), 'Contents4'),
            (os.path.join('sources','source3', 'file8'), 'Contents5'),
        ])

    def test_Integration_DepthAndSourceOrder_SequesterSink(self):
        o = DeduplicateOperation([Source(self.get_absolute_path('source1'), 1),
                                  Source(self.get_absolute_path(os.path.join('sources','source2')), 2),
                                  Source(self.get_absolute_path(os.path.join('sources','source3')), 3),
                                  Source(self.get_absolute_path(os.path.join('sources','source4')), 4)],
                                 [PathLengthDuplicateResolver(), SourceOrderDuplicateResolver()],
                                 SequesterDuplicateFileSink(self.get_absolute_path(os.path.join('sources','sequestration'))))

        o.run()

        self.check_exit_state([
            (os.path.join('source1','file1'), 'Contents1'),
            (os.path.join('source1','file2'), 'Contents1'),
            (os.path.join('source1','file3'), 'Contents2'),
            (os.path.join('sources','source2', 'file6'), 'Contents3'),
            (os.path.join('sources','source3', 'file7'), 'Contents4'),
            (os.path.join('sources','source3', 'file8'), 'Contents5'),
            (join_paths_componentwise(os.path.join('sources','sequestration'), self.get_absolute_path(os.path.join('sources', 'source2', 'file4'))), 'Contents1'),
            (join_paths_componentwise(os.path.join('sources','sequestration'), self.get_absolute_path(os.path.join('sources', 'source2', 'file5'))), 'Contents2'),
            (join_paths_componentwise(os.path.join('sources','sequestration'), self.get_absolute_path(os.path.join('sources', 'source4', 'file9'))), 'Contents1'),
            (join_paths_componentwise(os.path.join('sources', 'sequestration'), self.get_absolute_path(os.path.join('sources', 'source4', 'file10'))), 'Contents5')
        ])

if __name__ == '__main__':
    unittest.main()
