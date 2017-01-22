import os
import hashlib
import itertools
import operator
import copy
import re
import logging
import sys
import argparse


class DuplicateResolver(object):
    # Abstract base class

    def __init__(self, reverse=False):
        self.reverse = reverse

    def resolve(self, flist):
        return flist, []


class SortBasedDuplicateResolver(DuplicateResolver):
    def __init__(self, rank_function, reverse=False):
        self.rank_function = rank_function
        super(SortBasedDuplicateResolver, self).__init__(reverse)

    def resolve(self, flist):
        if len(flist) > 1:
            q = sorted(flist, key=self.rank_function, reverse=self.reverse)
            c = cmp(self.rank_function(q[0]),
                    self.rank_function(q[1]))

            pivot = None
            rank = self.rank_function(q[0])
            for i in range(1, len(q)):
                if self.rank_function(q[i]) != rank:
                    # Found the point where the sorting is meaningful
                    pivot = i
                    break

            if pivot is not None:
                return q[:pivot], q[pivot:]

        return flist, []


class AttrBasedDuplicateResolver(SortBasedDuplicateResolver):
    def __init__(self, attribute, reverse=False):
        super(AttrBasedDuplicateResolver, self).__init__(operator.attrgetter(attribute), reverse)


class PathLengthDuplicateResolver(SortBasedDuplicateResolver):
    def __init__(self, reverse=False):
        super(PathLengthDuplicateResolver, self).__init__(lambda x:
                                                          len(x.path.split(os.path.sep))
                                                          - len(x.source.path.split(os.path.sep)),
                                                          reverse)


class SourceOrderDuplicateResolver(AttrBasedDuplicateResolver):
    def __init__(self, reverse=False):
        super(SourceOrderDuplicateResolver, self).__init__(reverse, 'source.order')


class ModificationDateDuplicateResolver(AttrBasedDuplicateResolver):
        def __init__(self, reverse=False):
            super(ModificationDateDuplicateResolver, self).__init__(reverse, 'stat.st_mtime')


class CreationDateDuplicateResolver(AttrBasedDuplicateResolver):
        def __init__(self, reverse=False):
            super(CreationDateDuplicateResolver, self).__init__(reverse, 'stat.st_ctime')


class CopyPatternDuplicateResolver(DuplicateResolver):
    copy_patterns = [re.compile('^Copy of'), re.compile('.* copy [0-9]+\.[a-zA-Z0-9]+$')]

    def resolve(self, flist):
        determiner = lambda entry: reduce(operator.or_,
                                          [re.match(pattern, entry.path) is not None
                                           for pattern in self.copy_patterns])

        return (filter(lambda q: not determiner(q), flist),
                filter(determiner, flist))


class InteractiveDuplicateResolver(DuplicateResolver):
    def resolve(self, flist):
        for i in range(len(flist)):
            print '%2d\t%s\n' % (i, flist[i])

        d = int(raw_input('Enter file to retain: '))

        dupes = copy.copy(flist)
        dupes.pop(d)
        return [flist[d]], dupes


class DuplicateFileSink(object):
    # Abstract base class

    arguments = []

    def sink(self, files):
        pass


class DeleteDuplicateFileSink(object):
    def sink(self, files):
        logger = logging.getLogger(__name__)
        for entry in files:
            try:
                logger.debug('Deleting duplicate file %s', entry.path)
                os.unlink(entry.path)
            except Exception as e:
                logger.error('Unable to delete duplicate file %s: %s', entry.path, e)


class SequesterDuplicateFileSink(object):
    arguments = [{'name': 'sequester_path', 'type': 'string', 'nargs': 1}]

    def __init__(self, sequester_path):
        self.sequester_path = sequester_path

    def sink(self, files):
        logger = logging.getLogger(__name__)
        for entry in files:
            try:
                logger.debug('Sequestering duplicate file %s', entry.path)
                # We don't use os.renames because it has the bizarre side effect
                # of pruning directories containing the original file, if empty.

                # os.path.join will not correctly join if a subsequent path component
                # is an absolute path; hence we split before joining.
                new_path = os.path.join(self.sequester_path, *entry.path.split(os.path.sep))

                if not os.path.exists(os.path.split(new_path)[0]):
                    os.makedirs(os.path.split(new_path)[0])
                os.rename(entry.path, new_path)
            except Exception as e:
                print ('Unable to sequester duplicate file %s: %s', entry.path, e)


class OutputOnlyDuplicateFileSink(object):
    arguments = [{'name': 'output_file', 'type': argparse.FileType, 'nargs': 1}]

    def __init__(self, output_file=sys.stdout):
        self.output_file = output_file

    def sink(self, files):
        for entry in files:
            self.output_file.write(entry.path + '\n')


class FileEntry(object):
    def __init__(self, fpath, fsource):
        self.path = fpath
        self.source = fsource
        self.stat = os.stat(fpath)
        self.digest = None

    def get_digest(self):
        if self.digest is None:
            self.run_digest()

        return self.digest

    def run_digest(self):
        with open(self.path, mode='rb') as f:
            d = hashlib.sha512()

            while True:
                buf = f.read(4096)
                if not buf:
                    break

                d.update(buf)

        self.digest = d.hexdigest()
        logging.getLogger(__name__).debug('Found digest %s for path %s.', self.digest, self.path)


class FileCatalog(object):
    def __init__(self, idfunc):
        self.store = {}
        self.idfunc = idfunc

    def add_entry(self, entry):
        self.store.setdefault(self.idfunc(entry), []).append(entry)

    def get_groups(self):
        return [self.store[key] for key in self.store.keys() if len(self.store[key]) > 1]

    def get_grouped_entries(self):
        return reduce(operator.add, self.get_groups())


class Source(object):
    def __init__(self, dpath, order):
        self.path = os.path.abspath(dpath)
        self.order = order

    def walk(self, ctx):
        for cwd, subdirs, files in os.walk(self.path):
            for f in files:
                ctx.add_entry(FileEntry(os.path.join(cwd, f), self))


class DeduplicateOperation(object):
    def __init__(self, sources, resolvers, sink):
        self.sources = sources
        self.resolvers = resolvers
        self.sink = sink

    def run(self):
        size_catalog = FileCatalog(operator.attrgetter('entry.stat.st_size'))
        logger = logging.getLogger(__name__)

        logger.info('Building file catalog...')
        for s in sources:
            logger.info('Walking source %s', s.path)
            s.walk(f)

        logger.info('Identifying duplicate file groups...')

        f = FileCatalog(lambda entry: entry.get_digest())

        for entry in itertools.chain(size_catalog.get_groups()):
            f.add_entry(entry)

        to_keep = []
        to_sink = []

        for g in f.get_groups():
            logger.info('Attempting to resolve group of %d duplicate files:\n%s',
                        len(g),
                        '\n'.join(map(operator.attrgetter('path'), g)))
            n = g

            for r in selected_resolvers:
                logger.debug('Applying resolver %s.', r)
                (n, d) = r.resolve(n)
                logger.debug('Resolver found duplicates:\n%s\n and originals:\n%s',
                             '\n'.join(map(operator.attrgetter('path'), d)),
                             '\n'.join(map(operator.attrgetter('path'), n)))
                to_sink.extend(d)

            if len(n) > 1:
                logger.info('Marking files as originals (unable to resolve duplicates):\n%s',
                            '\n'.join(map(operator.attrgetter('path'), n)))
            else:
                logger.info('Marking file as original:\n%s',
                            n[0].path)

            to_keep.extend(n)

        sink.sink(to_sink)
