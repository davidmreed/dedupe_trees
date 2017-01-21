#!/usr/bin/env python

import argparse
from dedupe import *

class ResolverAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if not hasattr(namespace, self.dest):
            setattr(namespace, self.dest, [])

        resolver = resolvers[option_string[10:]]

        if len(values) > 0:
            resolver.reverse = values[0].lower() == 'desc'

        getattr(namespace, self.dest).append(resolver)

resolvers = {
    'path_length': PathLengthDuplicateResolver,
    'source_order': SourceOrderDuplicateResolver,
    'mod_date': ModificationDateDuplicateResolver,
    'create_date': CreationDateDuplicateResolver,
    'copy_pattern': CopyPatternDuplicateResolver,
    'interactive': InteractiveDuplicateResolver
}

sinks = {
    'delete': DeleteDuplicateFileSink,
    'sequester': SequesterDuplicateFileSink,
    'none': OutputOnlyDuplicateFileSink
}


def __main__(argv):
    parser = argparse.ArgumentParser()

    verbosity_levels = {'quiet': logging.NOTSET, 'errors': logging.ERROR,
                        'normal': logging.INFO, 'verbose': logging.DEBUG}

    parser.add_argument('-v', '--verbosity', type='string',
                        choices=verbosity_levels.keys(),
                        dest='verbosity', default='normal',
                        help='Log all actions')

    for item in resolvers.keys():
        parser.add_argument('--resolve-' + item, dest='resolvers', nargs='?',
                            action='ResolverAction')

    sink_group = parser.add_mutually_exclusive_group()
    for item in sinks.keys():
        sink_group.add_argument('--sink-' + item, dest='sink_class',
                                action='store_const', const=item)

        sink_class = sinks[item]

        for sink_arg in sink_class.arguments:
            parser.add_argument('--sink-' + item.name + '-'
                                + sink_arg.replace('_', '-'),
                                dest='sink_arguments_' + item + '_' + sink_arg,
                                action='store', type=item.type,
                                nargs=item.nargs)

    parser.add_argument('source_dir', action='append', required=True,
                        help='A directory tree to be scanned.')

    a = parser.parse_args()

    logging.getLogger(__name__).setLevel(verbosity_levels[a.verbosity])

    # Create and number sources.
    sources = []

    for i in range(len(a.source_dir)):
        sources.append(Source(a.source_dir[i], i))

    # Create sink, pulling out applicable parameters.
    params = {k[len('sink_arguments_' + a.sink_class):]: v
              for k, v in a
              if k.startswith('sink_arguments_' + a.sink_class)}
    sink = sinks[a.sink_class](**params)

    # Run the operation
    op = DeduplicateOperation(sources, a.resolvers, sink)

    op.run()
