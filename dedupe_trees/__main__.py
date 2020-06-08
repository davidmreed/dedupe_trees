import argparse
import json
from .dedupe_trees import *

# Establish dictionaries mapping command-line arguments to resolvers and sinks
resolvers = {
    "path-length": PathLengthDuplicateResolver,
    "source-order": SourceOrderDuplicateResolver,
    "mod-date": ModificationDateDuplicateResolver,
    "copy-pattern": CopyPatternDuplicateResolver,
    "interactive": InteractiveDuplicateResolver,
    "arbitrary": FilenameSortDuplicateResolver,
}

sinks = {
    "delete": {"class": DeleteDuplicateFileSink, "args": []},
    "sequester": {
        "class": SequesterDuplicateFileSink,
        "args": [{"name": "path", "type": str, "nargs": 1}],
    },
    "output-only": {
        "class": OutputOnlyDuplicateFileSink,
        "args": [
            {
                "name": "path",
                "type": argparse.FileType("w"),
                "nargs": 1,
                "default": sys.stdout,
            }
        ],
    },
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
            if values[0].lower() not in ["desc", "asc"]:
                raise Exception(
                    "Sort-based resolvers take an argument of 'asc' or 'desc' to specify sorting ({} provided)".format(
                        values[0]
                    )
                )

            resolver.reverse = values[0].lower() == "desc"

        getattr(namespace, self.dest).append(resolver)


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()

    verbosity_levels = {
        "quiet": logging.NOTSET,
        "errors": logging.ERROR,
        "normal": logging.INFO,
        "verbose": logging.DEBUG,
    }

    parser.add_argument(
        "-v",
        "--verbosity",
        choices=verbosity_levels.keys(),
        dest="verbosity",
        default="normal",
        help="Log all actions",
    )

    parser.add_argument(
        "-c",
        "--config-file",
        dest="config",
        default="~/.deduperc",
        help="Configuration file in JSON format, if not ~/.deduperc",
    )

    # Resolvers take an optional argument 'desc' to indicate descending/reverse sorting
    for item in resolvers:
        if issubclass(resolvers[item], SortBasedDuplicateResolver):
            parser.add_argument(
                "--resolve-" + item,
                dest="resolvers",
                nargs="?",
                choices=["asc", "desc"],
                action=ResolverAction,
            )
        else:
            parser.add_argument(
                "--resolve-" + item, dest="resolvers", action=ResolverAction, nargs=0
            )

    # Only one sink can be supplied. Each sink can provide its own arguments.
    sink_group = parser.add_mutually_exclusive_group()
    for item in sinks:
        sink_group.add_argument(
            "--sink-" + item, dest="sink_class", action="store_const", const=item
        )

        sink_arguments = sinks[item]["args"]

        for sink_arg in sink_arguments:
            parser.add_argument(
                "--sink-" + item + "-" + sink_arg["name"],
                dest="sink-arguments-" + item + "-" + sink_arg["name"],
                action="store",
                type=sink_arg["type"],
                nargs=sink_arg["nargs"],
            )

    parser.add_argument("source_dir", nargs="+", help="A directory tree to be scanned.")

    a = parser.parse_args()

    logging.getLogger(__name__).setLevel(verbosity_levels[a.verbosity])
    logging.getLogger(__name__).handlers[:] = [logging.StreamHandler()]

    # Check for required parameters that aren't enforced by argparse.
    if a.sink_class is None or a.resolvers is None:
        parser.print_help()
        return 1

    # Load config to get base ignores.
    ignore_pattern_list = None
    ignore_file_list = None
    try:
        with open(a.config, "r") as config_file:
            config = json.load(config_file)

        ignore_pattern_list = [re.compile(x) for x in config.get("ignore_patterns", [])]
        ignore_file_list = config.get("ignore_names", [])
        logging.getLogger(__name__).info(
            "Loaded configuration file {} with {} ignore names and {} ignore patterns.".format(
                a.config, len(ignore_file_list), len(ignore_pattern_list)
            )
        )
    except:
        # Supply sensible defaults if we can't find or read a configuration file.
        ignore_pattern_list = [re.compile(x) for x in ["^\\._.+"]]
        ignore_file_list = [".DS_Store", ".git", ".hg"]
        logging.getLogger(__name__).debug(
            "Unable to load a configuration file; using default ignore configuration."
        )

    # Create and number sources.
    sources = []
    source_filter = ConfiguredSourceFilter(ignore_pattern_list, ignore_file_list)

    for i in range(len(a.source_dir)):
        sources.append(Source(a.source_dir[i], i + 1, source_filter))

    # Create sink, pulling out applicable parameters.
    params = {}
    for arg in sinks[a.sink_class]["args"]:
        this_arg_name = "sink-arguments-" + a.sink_class + "-" + arg["name"]
        if hasattr(a, this_arg_name):
            this_arg = getattr(a, this_arg_name)
            if this_arg is not None:
                params[arg["name"]] = this_arg[0]
            else:
                # Fail if an arg isn't provided and doesn't supply a default.
                if arg.get("default") is None:
                    logging.getLogger(__name__).error(
                        "The argument {} for the sink {} is required.".format(
                            "--sink-" + a.sink_class + "-" + arg["name"], a.sink_class
                        )
                    )
                    parser.print_help()
                    return 1

    sink = sinks[a.sink_class]["class"](**params)

    # Run the operation
    op = DeduplicateOperation(sources, a.resolvers, sink)

    op.run()

    return 0


if __name__ == "__main__":
    exit(main())
