import hashlib
import io
import os
import re
import tempfile
import unittest
import unittest.mock

from dedupe_trees import (
    AttrBasedDuplicateResolver,
    ConfiguredSourceFilter,
    CopyPatternDuplicateResolver,
    DeduplicateOperation,
    DeleteDuplicateFileSink,
    DuplicateFileSink,
    DuplicateResolver,
    FileCatalog,
    FileEntry,
    FilenameSortDuplicateResolver,
    InteractiveDuplicateResolver,
    ModificationDateDuplicateResolver,
    OutputOnlyDuplicateFileSink,
    PathLengthDuplicateResolver,
    SequesterDuplicateFileSink,
    SortBasedDuplicateResolver,
    Source,
    SourceOrderDuplicateResolver,
    UserCanceledException,
    join_paths_componentwise,
)

# Dummy/stub objects for testing


class DummyEntry:
    def __init__(self, path, digest=None, source=None):
        self.path = path
        self.digest = digest
        self.source = source

    def __repr__(self):
        return self.path

    def get_digest(self):
        return self.digest

    def get_size(self):
        return self.digest

    def __eq__(self, other):
        if type(other) is type(self):
            return self.path == other.path and self.source == other.source

        return False


class DummySource(Source):
    def __init__(self, files):
        self.files = files
        self.path = "TestSource"
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
        return (
            list(filter(lambda x: x.path != self.key, flist)),
            list(filter(lambda x: x.path == self.key, flist)),
        )


class DummyCatalog(FileCatalog):
    def __init__(self):
        self.entries = []

    def add_entry(self, entry):
        self.entries.append(entry)


class DevilDuplicateResolver(DuplicateResolver):
    # Returns all files as duplicates
    def resolve(self, flist):
        return [], flist


class DevilOriginalDuplicateResolver(DuplicateResolver):
    # Returns all files as originals
    def resolve(self, flist):
        return flist, []


# Tests for global utility functions


class test_UtilityFunctions(unittest.TestCase):
    def test_join_paths_componentwise(self):
        # this test assumes a Linux environment
        self.assertEqual(
            "/test/source/file", join_paths_componentwise("/test", "source/file")
        )
        self.assertEqual(
            "/test/source/file", join_paths_componentwise("/test", "/source/file")
        )
        self.assertEqual(
            "/test/depth/source/file",
            join_paths_componentwise("/test/depth", "source/file"),
        )
        self.assertEqual(
            "test/source/file", join_paths_componentwise("test", "source/file")
        )
        self.assertEqual(
            "test/source/file", join_paths_componentwise("test", "/source/file")
        )
        self.assertEqual(
            "test/depth/source/file",
            join_paths_componentwise("test/depth", "source/file"),
        )


# Tests for resolvers (in-memory only, no disk access)

# Tests for base resolver classes


class test_SortBasedDuplicateResolver(unittest.TestCase):
    def setUp(self):
        self.s = SortBasedDuplicateResolver(lambda x: x, False)

    def test_EmptyList(self):
        self.assertEqual(self.s.resolve([]), ([], []))

    def test_OneElementList(self):
        r = self.s.resolve(["test"])

        self.assertEqual(r, (["test"], []))

    def test_Sorting(self):
        r = self.s.resolve(["test", "strings", "here"])

        self.assertEqual(r, (["here"], ["strings", "test"]))

    def test_Sorting_Multi(self):
        r = self.s.resolve(["test", "here", "strings", "here"])

        self.assertEqual(r, (["here", "here"], ["strings", "test"]))

    def test_Sorting_Reverse(self):
        self.s.reverse = True

        r = self.s.resolve(["test", "strings", "here"])

        self.assertEqual(r, (["test"], ["strings", "here"]))

    def test_Sorting_Equals(self):
        r = self.s.resolve(["test", "test", "test"])

        self.assertEqual(r, (["test", "test", "test"], []))


class test_AttrBasedDuplicateResolver(unittest.TestCase):
    def setUp(self):
        self.s = AttrBasedDuplicateResolver("path", False)

    def test_EmptyList(self):
        self.assertEqual(self.s.resolve([]), ([], []))

    def test_OneElementList(self):
        r = self.s.resolve([DummyEntry("test")])

        self.assertEqual(r, ([DummyEntry("test")], []))

    def test_Sorting(self):
        r = self.s.resolve(
            [DummyEntry("test"), DummyEntry("strings"), DummyEntry("here")]
        )

        self.assertEqual(
            r, ([DummyEntry("here")], [DummyEntry("strings"), DummyEntry("test")])
        )

    def test_Sorting_Multi(self):
        r = self.s.resolve(
            [
                DummyEntry("test"),
                DummyEntry("here"),
                DummyEntry("strings"),
                DummyEntry("here"),
            ]
        )

        self.assertEqual(
            r,
            (
                [DummyEntry("here"), DummyEntry("here")],
                [DummyEntry("strings"), DummyEntry("test")],
            ),
        )

    def test_Sorting_Reverse(self):
        self.s.reverse = True

        r = self.s.resolve(
            [DummyEntry("test"), DummyEntry("strings"), DummyEntry("here")]
        )

        self.assertEqual(
            r, ([DummyEntry("test")], [DummyEntry("strings"), DummyEntry("here")])
        )

    def test_Sorting_Equals(self):
        r = self.s.resolve([DummyEntry("test"), DummyEntry("test"), DummyEntry("test")])

        self.assertEqual(
            r, ([DummyEntry("test"), DummyEntry("test"), DummyEntry("test")], [])
        )


# Tests for concrete resolver classes


class test_PathLengthDuplicateResolver(unittest.TestCase):
    def test_PathLengthSorting(self):
        source_one = DummyEntry("test1")
        source_two = DummyEntry(os.path.join("root", "test2"))
        source_three = DummyEntry("test3")

        file_one = DummyEntry(
            os.path.join(source_one.path, "sub1", "file1"), source=source_one
        )
        file_two = DummyEntry(os.path.join(source_two.path, "file2"), source=source_two)
        file_three = DummyEntry(
            os.path.join(source_three.path, "sub3", "sub33", "file3"),
            source=source_three,
        )
        file_four = DummyEntry(
            os.path.join(source_two.path, "file4"), source=source_two
        )
        file_five = DummyEntry(
            os.path.join(source_one.path, "sub1", "file5"), source=source_three
        )

        # The order of entries within each list isn't guaranteed by this resolver
        r = PathLengthDuplicateResolver().resolve(
            [file_one, file_two, file_three, file_four, file_five]
        )
        self.assertCountEqual([file_two, file_four], r[0])
        self.assertCountEqual([file_one, file_three, file_five], r[1])


class test_CopyPatternDuplicateResolver(unittest.TestCase):
    def test_Patterns(self):
        file_one = DummyEntry("Copy of test")
        file_two = DummyEntry("test")
        file_three = DummyEntry("test copy 3.txt")
        file_four = DummyEntry("test (4).txt")
        file_six = DummyEntry("1_est.txt")

        r = CopyPatternDuplicateResolver().resolve(
            [file_one, file_two, file_three, file_four, file_six]
        )

        self.assertEqual(r, ([file_two], [file_one, file_three, file_four, file_six]))


class test_ModificationDateDuplicateResolver(unittest.TestCase):
    def test_ModificationDateDuplicateResolver(self):
        file_one = DummyEntry("test1")
        file_two = DummyEntry("test2")
        file_three = DummyEntry("test3")

        file_one.stat = DummyEntry("foo")
        setattr(file_one.stat, "st_mtime", 3)
        file_two.stat = DummyEntry("foo")
        setattr(file_two.stat, "st_mtime", 2)
        file_three.stat = DummyEntry("foo")
        setattr(file_three.stat, "st_mtime", 1)

        (originals, duplicates) = ModificationDateDuplicateResolver().resolve(
            [file_one, file_two, file_three]
        )

        self.assertCountEqual(originals, [file_three])
        self.assertCountEqual(duplicates, [file_one, file_two])


class test_SourceOrderDuplicateResolver(unittest.TestCase):
    def test_SourceOrder(self):
        # Using dummy entries, but real sources.

        source_one = Source("test1", 1)
        source_two = Source("test2", 2)
        source_three = Source("test3", 3)

        file_one = DummyEntry("file1", source=source_one)
        file_two = DummyEntry("file2", source=source_two)
        file_three = DummyEntry("file3", source=source_three)

        r = SourceOrderDuplicateResolver().resolve([file_three, file_two, file_one])
        self.assertEqual(r, ([file_one], [file_two, file_three]))


class test_InteractiveDuplicateResolver(unittest.TestCase):
    def test_InteractiveDuplicateResolver(self):
        file_one = DummyEntry("File 1")
        file_two = DummyEntry("File 2")
        file_three = DummyEntry("File 3")
        o = io.StringIO()

        with unittest.mock.patch("sys.stdout", o):
            with unittest.mock.patch("builtins.input", return_value="2"):
                r = InteractiveDuplicateResolver().resolve(
                    [file_one, file_two, file_three]
                )

                self.assertEqual(r, ([file_two], [file_one, file_three]))

            self.assertTrue("File 1" in o.getvalue())
            self.assertTrue("File 2" in o.getvalue())
            self.assertTrue("File 3" in o.getvalue())

    def test_InteractiveDuplicateResolver_Skip(self):
        file_one = DummyEntry("File 1")
        file_two = DummyEntry("File 2")
        file_three = DummyEntry("File 3")
        o = io.StringIO()

        with unittest.mock.patch("sys.stdout", o):
            with unittest.mock.patch("builtins.input", return_value="s"):
                r = InteractiveDuplicateResolver().resolve(
                    [file_one, file_two, file_three]
                )

                self.assertEqual(r, ([file_one, file_two, file_three], []))

    def test_InteractiveDuplicateResolver_Exit(self):
        file_one = DummyEntry("File 1")
        file_two = DummyEntry("File 2")
        file_three = DummyEntry("File 3")
        o = io.StringIO()

        with unittest.mock.patch("sys.stdout", o):
            with unittest.mock.patch("builtins.input", return_value="e"):
                with self.assertRaises(UserCanceledException):
                    InteractiveDuplicateResolver().resolve(
                        [file_one, file_two, file_three]
                    )


class test_FilenameSortDuplicateResolver(test_AttrBasedDuplicateResolver):
    def setUp(self):
        self.s = FilenameSortDuplicateResolver()

    def test_Sorting_Multi(self):
        r = self.s.resolve(
            [
                DummyEntry("test"),
                DummyEntry("here"),
                DummyEntry("strings"),
                DummyEntry("here"),
            ]
        )

        self.assertEqual(
            r,
            (
                [DummyEntry("here")],
                [DummyEntry("here"), DummyEntry("strings"), DummyEntry("test")],
            ),
        )

    def test_Sorting_Equals(self):
        r = self.s.resolve([DummyEntry("test"), DummyEntry("test"), DummyEntry("test")])

        self.assertEqual(
            r, ([DummyEntry("test")], [DummyEntry("test"), DummyEntry("test")])
        )

    def test_Sorting_Reverse(self):
        # This isn't really a sort-based resolver
        pass


# Tests for operational classes


class test_FileEntry(unittest.TestCase):
    def test_FileEntry(self):
        contents = "TEST_STRING"
        source = DummySource("test")
        self.temp_dir = tempfile.mkdtemp()

        (handle, path) = tempfile.mkstemp(dir=self.temp_dir)
        with os.fdopen(handle, mode="w") as f:
            f.write(contents)

        entry = FileEntry(path, source)

        self.assertEqual(path, entry.path)
        self.assertEqual(source, entry.source)
        self.assertEqual(os.stat(path), entry.stat)
        self.assertEqual(os.stat(path).st_size, entry.get_size())

        h = hashlib.sha512()
        h.update(contents.encode("utf-8"))

        self.assertEqual(h.hexdigest(), entry.get_digest())


class test_FileCatalog(unittest.TestCase):
    def test_FileCatalog(self):
        c = FileCatalog(lambda x: x.get_digest())

        file_one = DummyEntry("test", "Hash1")
        c.add_entry(file_one)
        self.assertEqual(c.get_groups(), [])

        file_two = DummyEntry("test2", "Hash1")
        c.add_entry(file_two)
        self.assertEqual(c.get_groups(), [[file_one, file_two]])

        c.add_entry(file_two)
        self.assertEqual(c.get_groups(), [[file_one, file_two]])

        file_three = DummyEntry("foo1", "Hash2")
        c.add_entry(file_three)
        self.assertEqual(c.get_groups(), [[file_one, file_two]])

        file_four = DummyEntry("foo2", "Hash2")
        c.add_entry(file_four)
        self.assertEqual(
            sorted(c.get_groups(), key=lambda x: x[0].path),
            [[file_three, file_four], [file_one, file_two]],
        )

    def test_FileCatalog_Exclusions(self):
        c = FileCatalog(lambda entry: None)

        c.add_entry("")
        c.add_entry("")

        self.assertEqual(0, len(c.store))
        self.assertEqual(c.get_groups(), [])


class test_SourceFilter(unittest.TestCase):
    def test_SourceFilter(self):
        names = ["test1"]
        patterns = [re.compile("^[0-9]"), re.compile(".*[0-9]$")]

        sf = ConfiguredSourceFilter(names=names, patterns=patterns)

        self.assertEqual(True, sf.include_file("test", "testdir"))
        self.assertEqual(True, sf.include_file("test_z", "testdir"))
        self.assertEqual(True, sf.include_file("test1q", "testdir"))
        self.assertEqual(False, sf.include_file("1test", "testdir"))
        self.assertEqual(False, sf.include_file("test2", "testdir"))
        self.assertEqual(False, sf.include_file("test1", "testdir"))


class test_DeduplicateOperation(unittest.TestCase):
    def setUp(self):
        self.f1 = DummyEntry("test1", digest="test")
        self.f2 = DummyEntry("test2", digest="test")
        self.f3 = DummyEntry("test3", digest="test")
        self.f4 = DummyEntry("test4", digest="test")
        self.f5 = DummyEntry("test5", digest="test5")
        self.so = DummySource([self.f1, self.f2, self.f3, self.f4, self.f5])
        self.r1 = DummyResolver("test1")
        self.r2 = DummyResolver("test2")
        self.r3 = DummyResolver("test3")

    def perform(self, resolvers):
        s = DummySink()
        o = DeduplicateOperation([self.so], resolvers, s)
        o.run()
        return s.sunk

    def test_DDO_Real_Resolvers(self):
        self.assertEqual([self.f1], self.perform([self.r1]))
        self.assertEqual([self.f1, self.f2], self.perform([self.r1, self.r2]))
        self.assertEqual(
            [self.f1, self.f2, self.f3], self.perform([self.r1, self.r2, self.r3])
        )
        self.assertEqual([], self.perform([]))

    def test_DDO_Devils(self):
        # Run exactly the same tests, but inserting "devil" resolvers, which always
        # return either all originals or all duplicates.
        # Should have no effect on the results.
        devil_duplicates = DevilDuplicateResolver()
        devil_originals = DevilOriginalDuplicateResolver()

        file_lists = [[self.f1], [self.f1, self.f2], [self.f1, self.f2, self.f3], []]

        resolver_lists = [
            [self.r1],
            [self.r1, self.r2],
            [self.r1, self.r2, self.r3],
            [],
        ]

        for (fl, rl) in zip(file_lists, resolver_lists):
            # Perform the test, inserting each devil resolver at each possible location
            # and asserting correct behavior each time.

            for i in range(0, len(rl)):
                self.assertEqual(fl, self.perform(rl[:i] + [devil_duplicates] + rl[i:]))
                self.assertEqual(fl, self.perform(rl[:i] + [devil_originals] + rl[i:]))


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
                contents = "Test"

            if pathname is None:
                (handle, path) = tempfile.mkstemp(dir=self.temp_dir)
                with os.fdopen(handle, mode="w") as f:
                    f.write(contents)
            else:
                new_path = self.get_absolute_path(pathname)
                if not os.path.exists(os.path.dirname(new_path)):
                    os.makedirs(os.path.dirname(new_path))
                with open(new_path, mode="w") as f:
                    f.write(contents)

    def get_absolute_path(self, path):
        return os.path.join(
            self.temp_dir,
            *(os.path.splitdrive(os.path.normpath(path))[1].split(os.path.sep)),
        )

    def check_exit_state(self, exitStates):
        # Walk the tree of files. Ensure that the states provided are a 1:1 match with the files in the directory tree
        # including contents if supplied.

        # Prepare a dictionary based on the states supplied.

        states = {}
        for path, contents in exitStates:
            states[self.get_absolute_path(path)] = contents

        for cwd, subdirs, fs in os.walk(self.temp_dir, topdown=False):
            for f in fs:
                this_file_path = os.path.join(cwd, f)
                if this_file_path in states:
                    if states[this_file_path] is not None:
                        with open(this_file_path, mode="r") as this_file:
                            contents = this_file.read()

                        self.assertEqual(states[this_file_path], contents)

                    del states[this_file_path]
                else:
                    raise FileSystemTestException(
                        "Exit state check failed: file {0} is present and should not be.".format(
                            this_file_path
                        )
                    )

        # There should be no further keys remaining in the dictionary

        self.assertEqual({}, states)

    def tearDown(self):
        for cwd, subdirs, fs in os.walk(self.temp_dir, topdown=False):
            for f in fs:
                os.unlink(os.path.join(cwd, f))
            for d in subdirs:
                os.rmdir(os.path.join(cwd, d))

        os.rmdir(self.temp_dir)


# Tests for core classes, with filesystem access


class test_FS_FileEntry(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [(os.path.join("source1", "file1"), "Contents1")]
        super(test_FS_FileEntry, self).setUp()

    def test_FS_FileEntry(self):
        path = self.get_absolute_path(self.entry_state[0][0])
        fe = FileEntry(path, None)

        self.assertEqual(fe.path, path)
        self.assertEqual(os.stat(path).st_size, fe.get_size())
        self.assertEqual(None, fe.digest)
        # The double-assert exercises the caching function
        self.assertEqual(
            hashlib.sha512("Contents1".encode("utf-8")).hexdigest(), fe.get_digest()
        )
        self.assertEqual(
            hashlib.sha512("Contents1".encode("utf-8")).hexdigest(), fe.get_digest()
        )


class test_FS_Source(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", "file1"), "Contents1"),
            (os.path.join("source1", "subdir1", "file2"), "Contents1"),
            (os.path.join("source1", "file3"), "Contents2"),
            (os.path.join("source1", "subdir2", "subdir", "file4"), "Contents2"),
        ]
        super(test_FS_Source, self).setUp()

    def test_FS_Source(self):
        s = Source(self.get_absolute_path("source1"), 2)
        f = DummyCatalog()

        self.assertEqual(2, s.order)
        s.walk(f)

        self.assertCountEqual(
            [self.get_absolute_path(f) for (f, c) in self.entry_state],
            [fe.path for fe in f.entries],
        )

    def test_FS_Source_WithFilter(self):
        names = ["subdir1", "file1"]
        patterns = [re.compile("f.*[4-5]$")]

        sf = ConfiguredSourceFilter(names=names, patterns=patterns)
        s = Source(self.get_absolute_path("source1"), 1, sf)
        f = DummyCatalog()

        s.walk(f)
        self.assertEqual(1, len(f.entries))
        self.assertEqual(
            self.get_absolute_path(self.entry_state[2][0]), f.entries[0].path
        )


# Tests for resolvers (individual, with real entry and source objects but no sink)


class test_FS_InteractiveDuplicateResolver(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", "file1"), "Contents1"),
            (os.path.join("source1", "subdir1", "file2"), "Contents1"),
        ]
        super(test_FS_InteractiveDuplicateResolver, self).setUp()

    def test_FS_InteractiveDuplicateResolver(self):
        fc = FileCatalog(lambda x: x.get_digest())
        s_one = Source(self.get_absolute_path("source1"), 1)

        s_one.walk(fc)

        self.assertEqual(1, len(fc.get_groups()))

        with unittest.mock.patch("builtins.input", return_value="2"):
            (originals, duplicates) = InteractiveDuplicateResolver().resolve(
                fc.get_groups()[0]
            )

        self.assertEqual(1, len(originals))
        self.assertEqual(1, len(duplicates))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source1", "file1")), duplicates[0].path
        )
        self.assertEqual(
            self.get_absolute_path(os.path.join("source1", "subdir1", "file2")),
            originals[0].path,
        )


class test_FS_PathLengthDuplicateResolver(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", "file1"), "Contents1"),
            (os.path.join("source1", "subdir1", "file2"), "Contents1"),
            (os.path.join("source2", "file3"), "Contents2"),
            (os.path.join("sources", "source3", "subdir", "file4"), "Contents2"),
        ]
        super(test_FS_PathLengthDuplicateResolver, self).setUp()

    def test_FS_PathLengthDuplicateResolver_OneSource(self):
        fc = FileCatalog(lambda x: x.get_digest())
        s = Source(self.get_absolute_path("source1"), 1)

        s.walk(fc)

        self.assertEqual(1, len(fc.get_groups()))

        (originals, duplicates) = PathLengthDuplicateResolver().resolve(
            fc.get_groups()[0]
        )

        self.assertEqual(1, len(originals))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source1", "file1")), originals[0].path
        )
        self.assertEqual(1, len(duplicates))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source1", "subdir1", "file2")),
            duplicates[0].path,
        )

    def test_FS_PathLengthDuplicateResolver_TwoSources(self):
        fc = FileCatalog(lambda x: x.get_digest())
        s_one = Source(self.get_absolute_path("source2"), 1)
        s_two = Source(self.get_absolute_path(os.path.join("sources", "source3")), 2)

        s_one.walk(fc)
        s_two.walk(fc)

        self.assertEqual(1, len(fc.get_groups()))

        (originals, duplicates) = PathLengthDuplicateResolver().resolve(
            fc.get_groups()[0]
        )

        self.assertEqual(1, len(originals))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source2", "file3")), originals[0].path
        )
        self.assertEqual(1, len(duplicates))
        self.assertEqual(
            self.get_absolute_path(
                os.path.join("sources", "source3", "subdir", "file4")
            ),
            duplicates[0].path,
        )


class test_FS_SourceOrderDuplicateResolver(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", "file1"), "Contents1"),
            (os.path.join("source2", "file2"), "Contents1"),
        ]
        super(test_FS_SourceOrderDuplicateResolver, self).setUp()

    def test_FS_SourceOrderDuplicateResolver(self):
        fc = FileCatalog(lambda x: x.get_digest())
        s_one = Source(self.get_absolute_path("source1"), 1)
        s_two = Source(self.get_absolute_path("source2"), 2)

        s_one.walk(fc)
        s_two.walk(fc)

        self.assertEqual(1, len(fc.get_groups()))

        (originals, duplicates) = SourceOrderDuplicateResolver().resolve(
            fc.get_groups()[0]
        )

        self.assertEqual(1, len(originals))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source1", "file1")), originals[0].path
        )
        self.assertEqual(1, len(duplicates))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source2", "file2")), duplicates[0].path
        )


class test_FS_ModificationDateDuplicateResolver(
    test_FileSystemTestBase, unittest.TestCase
):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", "file1"), "Contents1"),
            (os.path.join("source2", "file2"), "Contents1"),
        ]
        super(test_FS_ModificationDateDuplicateResolver, self).setUp()

        os.utime(
            self.get_absolute_path(os.path.join("source1", "file1")), (10000, 10000)
        )
        os.utime(
            self.get_absolute_path(os.path.join("source1", "file1")), (20000, 20000)
        )

    def test_FS_ModificationDateDuplicateResolver(self):
        fc = FileCatalog(lambda x: x.get_digest())
        s_one = Source(self.get_absolute_path("source1"), 1)
        s_two = Source(self.get_absolute_path("source2"), 2)

        s_one.walk(fc)
        s_two.walk(fc)

        self.assertEqual(1, len(fc.get_groups()))

        (originals, duplicates) = ModificationDateDuplicateResolver().resolve(
            fc.get_groups()[0]
        )

        self.assertEqual(1, len(originals))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source1", "file1")), originals[0].path
        )
        self.assertEqual(1, len(duplicates))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source2", "file2")), duplicates[0].path
        )


class test_FS_CopyPatternDuplicateResolver(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", "file1"), "Contents1"),
            (os.path.join("source2", "Copy of file1"), "Contents1"),
        ]
        super(test_FS_CopyPatternDuplicateResolver, self).setUp()

    def test_FS_CopyPatternDuplicateResolver(self):
        fc = FileCatalog(lambda x: x.get_digest())
        s_one = Source(self.get_absolute_path("source1"), 1)
        s_two = Source(self.get_absolute_path("source2"), 2)

        s_one.walk(fc)
        s_two.walk(fc)

        self.assertEqual(1, len(fc.get_groups()))

        (originals, duplicates) = SourceOrderDuplicateResolver().resolve(
            fc.get_groups()[0]
        )

        self.assertEqual(1, len(originals))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source1", "file1")), originals[0].path
        )
        self.assertEqual(1, len(duplicates))
        self.assertEqual(
            self.get_absolute_path(os.path.join("source2", "Copy of file1")),
            duplicates[0].path,
        )


# Tests for sinks (with file system access)
class test_FS_DeleteDuplicateFileSink(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [("test " + str(i), None) for i in range(1, 10)]
        super(test_FS_DeleteDuplicateFileSink, self).setUp()

    def test_Deletion(self):
        s = DeleteDuplicateFileSink()

        s.sink(
            [DummyEntry(self.get_absolute_path(f)) for (f, c) in self.entry_state[:3]]
        )

        self.check_exit_state(self.entry_state[3:])

        s.sink(
            [DummyEntry(self.get_absolute_path(f)) for (f, c) in self.entry_state[3:]]
        )

        self.check_exit_state([])


class test_FS_SequesterDuplicateFileSink(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source", "test " + str(i)), None) for i in range(1, 10)
        ]
        super(test_FS_SequesterDuplicateFileSink, self).setUp()

    def test_Sequestration(self):
        s = SequesterDuplicateFileSink(self.get_absolute_path("sink"))

        s.sink([DummyEntry(self.get_absolute_path(f)) for (f, c) in self.entry_state])

        self.check_exit_state(
            [
                (
                    s.construct_sequestered_path(self.get_absolute_path(p))[
                        len(self.temp_dir) :
                    ],
                    None,
                )
                for (p, c) in self.entry_state
            ]
        )


class test_FS_OutputOnlyDuplicateFileSink(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [("test " + str(i), None) for i in range(1, 10)]
        super(test_FS_OutputOnlyDuplicateFileSink, self).setUp()

    def test_OutputOnly(self):
        o = io.StringIO()
        s = OutputOnlyDuplicateFileSink(path=o)

        s.sink([DummyEntry(self.get_absolute_path(f)) for (f, c) in self.entry_state])

        self.assertEqual(
            "\n".join(
                [self.get_absolute_path(f) for (f, c) in self.entry_state]
            ).strip(),
            o.getvalue().strip(),
        )

        self.check_exit_state(self.entry_state)


# Integration tests (executing against the disk with groups of resolvers and a sink)


class test_Integration(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", "file1"), "Contents1"),
            (os.path.join("source1", "Copy of file1"), "Contents1"),
            (os.path.join("source1", "file3"), "Contents2"),
            (os.path.join("sources", "source2", "file4"), "Contents1"),
            (os.path.join("sources", "source2", "file5"), "Contents2"),
            (os.path.join("sources", "source2", "file6"), "Contents3"),
            (os.path.join("sources", "source3", "file7"), "Contents4"),
            (os.path.join("sources", "source3", "file8"), "Contents5"),
            (os.path.join("sources", "source4", "file9"), "Contents1"),
            (os.path.join("sources", "source4", "file10"), "Contents5"),
        ]
        super(test_Integration, self).setUp()

    def perform(self, resolvers, sink, exit_states):
        o = DeduplicateOperation(
            [
                Source(self.get_absolute_path("source1"), 1),
                Source(self.get_absolute_path(os.path.join("sources", "source2")), 2),
                Source(self.get_absolute_path(os.path.join("sources", "source3")), 3),
                Source(self.get_absolute_path(os.path.join("sources", "source4")), 4),
            ],
            resolvers,
            sink,
        )

        o.run()

        self.check_exit_state(exit_states)

    def test_Integration_Interactive_OutputSink(self):
        o = io.StringIO()
        s = OutputOnlyDuplicateFileSink(path=o)

        # Group 1 is `Contents1` files. Group 2 is `Contents2` files. Group 3 is `Contents5` files.
        with unittest.mock.patch("builtins.input", side_effect=["2", "2", "2"]):
            self.perform([InteractiveDuplicateResolver()], s, self.entry_state)

        to_be_sunk = [
            (os.path.join("source1", "Copy of file1"), "Contents1"),
            (os.path.join("source1", "file3"), "Contents2"),
            (os.path.join("sources", "source2", "file4"), "Contents1"),
            (os.path.join("sources", "source3", "file8"), "Contents5"),
            (os.path.join("sources", "source4", "file9"), "Contents1"),
        ]

        self.assertCountEqual(
            [self.get_absolute_path(f) for (f, c) in to_be_sunk],
            o.getvalue().strip().split("\n"),
        )

    def test_Integration_DepthAndSourceOrder_DeleteSink(self):
        self.perform(
            [PathLengthDuplicateResolver(), SourceOrderDuplicateResolver()],
            DeleteDuplicateFileSink(),
            [
                (os.path.join("source1", "file1"), "Contents1"),
                (os.path.join("source1", "Copy of file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
            ],
        )

    def test_Integration_DepthAndSourceOrder_SequesterSink(self):
        self.perform(
            [PathLengthDuplicateResolver(), SourceOrderDuplicateResolver()],
            SequesterDuplicateFileSink(
                self.get_absolute_path(os.path.join("sources", "sequestration"))
            ),
            [
                (os.path.join("source1", "file1"), "Contents1"),
                (os.path.join("source1", "Copy of file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source2", "file4")
                        ),
                    ),
                    "Contents1",
                ),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source2", "file5")
                        ),
                    ),
                    "Contents2",
                ),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source4", "file9")
                        ),
                    ),
                    "Contents1",
                ),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source4", "file10")
                        ),
                    ),
                    "Contents5",
                ),
            ],
        )

    def test_Integration_CopyPattern_DeleteSink(self):
        self.perform(
            [CopyPatternDuplicateResolver()],
            DeleteDuplicateFileSink(),
            [
                (os.path.join("source1", "file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file4"), "Contents1"),
                (os.path.join("sources", "source2", "file5"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
                (os.path.join("sources", "source4", "file9"), "Contents1"),
                (os.path.join("sources", "source4", "file10"), "Contents5"),
            ],
        )


# Command-line integration tests (pass real parameter sets to main and execute against disk)


class test_CommandLine_Integration(test_Integration):
    def perform(self, args, exit_states):
        import dedupe_trees.__main__ as ddt

        with unittest.mock.patch("sys.argv", args):
            ddt.main()

        self.check_exit_state(exit_states)

    def test_Integration_Interactive_OutputSink(self):
        # Group 1 is `Contents1` files. Group 2 is `Contents2` files. Group 3 is `Contents5` files.
        to_be_sunk = [
            self.get_absolute_path(os.path.join("source1", "Copy of file1")),
            self.get_absolute_path(os.path.join("sources", "source2", "file4")),
            self.get_absolute_path(os.path.join("sources", "source4", "file9")),
            self.get_absolute_path(os.path.join("sources", "source3", "file8")),
            self.get_absolute_path(os.path.join("source1", "file3")),
        ]

        with unittest.mock.patch("builtins.input", side_effect=["2", "2", "2"]):
            out_state = self.entry_state
            out_state.append(("output.txt", None))
            self.perform(
                [
                    "dedupe_trees.py",
                    "--resolve-interactive",
                    "--sink-output-only",
                    "--sink-output-only-path",
                    self.get_absolute_path("output.txt"),
                    self.get_absolute_path("source1"),
                    self.get_absolute_path(os.path.join("sources", "source2")),
                    self.get_absolute_path(os.path.join("sources", "source3")),
                    self.get_absolute_path(os.path.join("sources", "source4")),
                ],
                out_state,
            )

        # Check the contents of the output file (we supply None above to suppress check,
        # because filename ordering isn't deterministic)
        with open(self.get_absolute_path("output.txt"), mode="r") as this_file:
            contents = this_file.read()

        self.assertCountEqual(
            to_be_sunk, filter(lambda x: len(x) > 0, contents.split("\n"))
        )

    def test_Integration_DepthAndSourceOrder_DeleteSink(self):
        self.perform(
            [
                "run_dedupe_trees.py",
                "--resolve-path-length",
                "--resolve-source-order",
                "--sink-delete",
                self.get_absolute_path("source1"),
                self.get_absolute_path(os.path.join("sources", "source2")),
                self.get_absolute_path(os.path.join("sources", "source3")),
                self.get_absolute_path(os.path.join("sources", "source4")),
            ],
            [
                (os.path.join("source1", "file1"), "Contents1"),
                (os.path.join("source1", "Copy of file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
            ],
        )

    def test_Integration_DepthAndSourceOrder_SequesterSink(self):
        self.perform(
            [
                "run_dedupe_trees.py",
                "--resolve-path-length",
                "--resolve-source-order",
                "--sink-sequester",
                "--sink-sequester-path",
                self.get_absolute_path(os.path.join("sources", "sequestration")),
                self.get_absolute_path("source1"),
                self.get_absolute_path(os.path.join("sources", "source2")),
                self.get_absolute_path(os.path.join("sources", "source3")),
                self.get_absolute_path(os.path.join("sources", "source4")),
            ],
            [
                (os.path.join("source1", "file1"), "Contents1"),
                (os.path.join("source1", "Copy of file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source2", "file4")
                        ),
                    ),
                    "Contents1",
                ),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source2", "file5")
                        ),
                    ),
                    "Contents2",
                ),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source4", "file9")
                        ),
                    ),
                    "Contents1",
                ),
                (
                    join_paths_componentwise(
                        os.path.join("sources", "sequestration"),
                        self.get_absolute_path(
                            os.path.join("sources", "source4", "file10")
                        ),
                    ),
                    "Contents5",
                ),
            ],
        )

    def test_Integration_CopyPattern_DeleteSink(self):
        self.perform(
            [
                "run_dedupe_trees.py",
                "--resolve-copy-pattern",
                "--sink-delete",
                self.get_absolute_path("source1"),
                self.get_absolute_path(os.path.join("sources", "source2")),
                self.get_absolute_path(os.path.join("sources", "source3")),
                self.get_absolute_path(os.path.join("sources", "source4")),
            ],
            [
                (os.path.join("source1", "file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file4"), "Contents1"),
                (os.path.join("sources", "source2", "file5"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
                (os.path.join("sources", "source4", "file9"), "Contents1"),
                (os.path.join("sources", "source4", "file10"), "Contents5"),
            ],
        )


class test_Integration_Config(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = [
            (os.path.join("source1", ".DS_Store"), "Contents1"),
            (os.path.join("source1", "Copy of file1"), "Contents1"),
            (os.path.join("source1", "file3"), "Contents2"),
            (os.path.join("sources", "source2", "file4"), "Contents1"),
            (os.path.join("sources", "source2", "file5"), "Contents2"),
            (os.path.join("sources", "source2", "file6"), "Contents3"),
            (os.path.join("sources", "source3", "file7"), "Contents4"),
            (os.path.join("sources", "source3", "file8"), "Contents5"),
            (os.path.join("sources", "source4", "file9"), "Contents1"),
            (os.path.join("sources", "source4", "file10"), "Contents5"),
            (os.path.join("sources", "source2", ".hg", "hgfile"), "Contents5"),
            (
                "config.json",
                '{ "ignore_patterns": ["file[5-8]"], "ignore_names": ["file10"] }',
            ),
        ]
        super(test_Integration_Config, self).setUp()

    def perform(self, args, exit_states):
        import dedupe_trees.__main__ as ddt

        with unittest.mock.patch("sys.argv", args):
            ddt.main()

        self.check_exit_state(exit_states)

    def test_Integration_DefaultConfig(self):
        self.perform(
            [
                "dedupe_trees.py",
                "--resolve-source-order",
                "--sink-delete",
                self.get_absolute_path("source1"),
                self.get_absolute_path(os.path.join("sources", "source2")),
                self.get_absolute_path(os.path.join("sources", "source3")),
                self.get_absolute_path(os.path.join("sources", "source4")),
            ],
            [
                (os.path.join("source1", ".DS_Store"), "Contents1"),
                (os.path.join("source1", "Copy of file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
                (os.path.join("sources", "source2", ".hg", "hgfile"), "Contents5"),
                (
                    "config.json",
                    '{ "ignore_patterns": ["file[5-8]"], "ignore_names": ["file10"] }',
                ),
            ],
        )

    def test_Integration_DiskConfig(self):
        self.perform(
            [
                "dedupe_trees.py",
                "--resolve-source-order",
                "--sink-delete",
                "-c",
                self.get_absolute_path("config.json"),
                self.get_absolute_path("source1"),
                self.get_absolute_path(os.path.join("sources", "source2")),
                self.get_absolute_path(os.path.join("sources", "source3")),
                self.get_absolute_path(os.path.join("sources", "source4")),
            ],
            [
                (os.path.join("source1", ".DS_Store"), "Contents1"),
                (os.path.join("source1", "Copy of file1"), "Contents1"),
                (os.path.join("source1", "file3"), "Contents2"),
                (os.path.join("sources", "source2", "file5"), "Contents2"),
                (os.path.join("sources", "source2", "file6"), "Contents3"),
                (os.path.join("sources", "source3", "file7"), "Contents4"),
                (os.path.join("sources", "source3", "file8"), "Contents5"),
                (os.path.join("sources", "source4", "file10"), "Contents5"),
                (os.path.join("sources", "source2", ".hg", "hgfile"), "Contents5"),
                (
                    "config.json",
                    '{ "ignore_patterns": ["file[5-8]"], "ignore_names": ["file10"] }',
                ),
            ],
        )


class test_ResolverAction(unittest.TestCase):
    def test_ResolverAction(self):
        import argparse
        import dedupe_trees.__main__ as ddt

        r = ddt.ResolverAction([], "resolvers")
        n = argparse.Namespace()

        r(None, n, ["asc"], "--resolve-copy-pattern")

        self.assertEqual(1, len(getattr(n, r.dest)))
        resolvers = n.resolvers
        self.assertIsInstance(resolvers[0], CopyPatternDuplicateResolver)
        self.assertEqual(False, resolvers[0].reverse)

        r(None, n, ["desc"], "--resolve-source-order")
        self.assertEqual(2, len(getattr(n, r.dest)))
        resolvers = n.resolvers
        self.assertIsInstance(resolvers[1], SourceOrderDuplicateResolver)
        self.assertEqual(True, resolvers[1].reverse)


class test_ErrorHandling_Arguments(test_FileSystemTestBase, unittest.TestCase):
    def setUp(self):
        self.entry_state = []
        super(test_ErrorHandling_Arguments, self).setUp()

    def test_ErrorHandling_MissingResolver(self):
        import dedupe_trees.__main__ as ddt

        with unittest.mock.patch(
            "sys.argv",
            ["dedupe_trees.py", "--sink-delete", self.get_absolute_path(self.temp_dir)],
        ):
            retval = ddt.main()

            self.assertEqual(1, retval)

    def test_ErrorHandling_MissingSink(self):
        import dedupe_trees.__main__ as ddt

        with unittest.mock.patch(
            "sys.argv",
            [
                "dedupe_trees",
                "--resolve-copy-pattern",
                self.get_absolute_path(self.temp_dir),
            ],
        ):
            retval = ddt.main()

            self.assertEqual(1, retval)

    def test_ErrorHandling_MissingSinkArgument(self):
        import dedupe_trees.__main__ as ddt

        with unittest.mock.patch(
            "sys.argv",
            [
                "dedupe_trees",
                "--resolve-copy-pattern",
                "--sink-sequester",
                self.get_absolute_path(self.temp_dir),
            ],
        ):
            retval = ddt.main()

            self.assertEqual(1, retval)


if __name__ == "__main__":
    unittest.main()
