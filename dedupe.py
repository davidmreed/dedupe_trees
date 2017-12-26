#!/usr/bin/env python

import argparse
from dedupe import *

# Establish dictionaries mapping command-line arguments to resolvers and sinks
resolvers = {
    'path-length': PathLengthDuplicateResolver,
    'source-order': SourceOrderDuplicateResolver,
    'mod-date': ModificationDateDuplicateResolver,
    'copy-pattern': CopyPatternDuplicateResolver,
    'interactive': InteractiveDuplicateResolver
}

sinks = {
    'delete': {'class': DeleteDuplicateFileSink, 'args': []},
    'sequester': {'class': SequesterDuplicateFileSink,
                  'args': [{'name': 'path', 'type': str, 'nargs': 1}]},
    'output-only': {'class': OutputOnlyDuplicateFileSink,
                    'args': [{'name': 'path', 'type': argparse.FileType('w'),
                              'nargs': 1, 'default': sys.stdout}]}
}


class ResolverAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if (not hasattr(namespace, self.dest)) or getattr(namespace, self.dest) is None:
            setattr(namespace, self.dest, [])

        # Determine the human-readable name of the resolver by stripping off the
        # --resolve- prefix
        resolver = resolvers[option_string[10:]]()

        # By default, sort in ascending order unless 'desc' is specified
        if values is not None and len(values) > 0:
            resolver.reverse = values[0].lower() == 'desc'

        getattr(namespace, self.dest).append(resolver)


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()

    verbosity_levels = {'quiet': logging.NOTSET, 'errors': logging.ERROR,
                        'normal': logging.INFO, 'verbose': logging.DEBUG}

    parser.add_argument('-v', '--verbosity',
                        choices=verbosity_levels.keys(),
                        dest='verbosity', default='normal',
                        help='Log all actions')

    # Resolvers take an optional argument 'desc' to indicate descending/reverse sorting
    for item in resolvers:
        parser.add_argument('--resolve-' + item, dest='resolvers', nargs='?',
                            action=ResolverAction)

    # Only one sink can be supplied. Each sink can provide its own arguments.
    sink_group = parser.add_mutually_exclusive_group()
    for item in sinks:
        sink_group.add_argument('--sink-' + item, dest='sink_class',
                                action='store_const', const=item)

        sink_arguments = sinks[item]['args']

        for sink_arg in sink_arguments:
            parser.add_argument('--sink-' + item + '-'
                                + sink_arg['name'],
                                dest='sink-arguments-' + item +
                                '-' + sink_arg['name'],
                                action='store', type=sink_arg['type'],
                                nargs=sink_arg['nargs'])

    parser.add_argument('source_dir', nargs='+',
                        help='A directory tree to be scanned.')

    a = parser.parse_args()

    logging.getLogger('dedupe').setLevel(verbosity_levels[a.verbosity])
    logging.getLogger('dedupe').handlers[:] = [logging.StreamHandler()]

    # Create and number sources.
    sources = []

    for i in range(len(a.source_dir)):
        sources.append(Source(a.source_dir[i], i+1))

    # Create sink, pulling out applicable parameters.
    params = {}
    for arg in sinks[a.sink_class]['args']:
        this_arg_name = 'sink-arguments-' + a.sink_class + '-' + arg['name'] 
        if hasattr(a, this_arg_name):
            this_arg = getattr(a, this_arg_name)
            if this_arg is not None:
                params[arg['name']] = this_arg[0]

    sink = sinks[a.sink_class]['class'](**params)

    # Run the operation
    op = DeduplicateOperation(sources, a.resolvers, sink)

    op.run()


if __name__ == '__main__':
    exit(main())
