# dedupe.py

[![CircleCI](https://circleci.com/gh/davidmreed/dedupe.py.svg?style=svg)](https://circleci.com/gh/davidmreed/dedupe.py)
[![codecov](https://codecov.io/gh/davidmreed/dedupe.py/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmreed/dedupe.py)

This tool provides functionality for scanning multiple file hierarchies for duplicate files
occurring at any depth and configurably managing these duplicates. `dedupe.py` is intended to support
merging large, structurally divergent hierarchies, such as overlapping photo collections archived in one
area by date and in another by album. It applies a user-specified sequence of resolvers to determine which
copy of a duplicated file to keep, using criteria like modification and creation date, file tree order,
path depth, and so on.

Duplicated items may be deleted, sequestered in a separate file tree, or labeled in a file.

## Usage

For details of additional arguments, do `dedupe.py -h`.

`dedupe.py` accepts command-line arguments defining (a) one or more *sources*, directory trees to be scanned, in order; (b) one or more *resolvers*, algorithms to be run on sets of duplicated files to determine the *original* or the preferred output file, in order; and (c) exactly one *sink*, an action to be run on files identified as non-originals. Resolvers can themselves take arguments.

A `dedupe.py` invocation looks like this:

   `dedupe.py --resolve-source-order --resolve-mod-date desc --sink-sequester --sink-sequester-path ~/sequester ~/source_1 ~/source_2`

Here's what `dedupe.py` will do:

  - Scan the directory trees `~/source_1` and `~/source_2` for duplicate files. `dedupe.py` makes two passes across each source, using file size in bytes to identify potential duplicates and the SHA-512 hash to confirm potentials.
  - For each group of duplicate files, evaluate the `source-order` and `mod-date` resolvers, in that order.
    - The `source-order` resolver will prefer duplicated files found in a source specified earlier on the command line.
    - The `mod-date` resolver, with the `desc` modifier, will prefer the most recent copy of duplicated files.
  - Resolvers will run on each group of duplicated files until either a single original file is identified, or all resolvers have been run without identifying a single original. Any original files or unresolvable duplicates are retained (in the latter case, a message is printed)
  - As files are identified as non-original duplicates, they are fed to the sink, in this case the `sequester` sink. `sequester` takes an additional argument, `sink-sequester-path`, which specifies a directory path. `sequester` rebuilds the file hierarchy for duplicate files within the sequestered tree, so no files are deleted.

For more details on the included resolvers and sinks, see the sections below. For details on the formatting of command-line arguments, see `dedupe.py -h`.

## Resolvers

`dedupe.py` comes with the following resolvers.

### `path-length`

This resolver sorts duplicated files by the number of path components in their paths, starting at their respective sources. It can be used to, for example, prefer files that have been sorted higher or lower in their hierarchies.

Given the sources `~/source_1` and `~/docs/source_2`, the path lengths for the following files would result:

  - `~/source_1/file.txt`: 1 (one path component inside the source)
  - `~/source_1/content/file2.txt`: 2
  - `~/docs/source_2/file3.txt`: 1 (only one component inside the source, even though the source's path is longer)
  - `~/docs/source_2/stuff/file.txt`: 2
  - `~/docs/source_2/things/and/other/stuff/file.txt`: 5

`path-length` by default prefers files higher in the hierarchy; to prefer deeper files, specify `desc`. Like all sort-based resolvers, `path-length` will prefer a single file with the highest/lowest attribute value; if more than one file have the same value, they will both be considered originals and sent to the next resolver in the chain.

### `source-order`

`source-order` prefers files based on the positions of their respective sources on the command line. By default, earlier sources are preferred to later ones; to prefer later ones, specify `desc`.

Within the same source, `source-order` has no effect and will pass all files to the next resolver.

### `mod-date`

`mod-date` chooses files based on their file system modification date. By default, earlier files are preferred; to choose later files, specify `desc`. Like all sort-based resolvers, files that are tied are passed on to the next resolver.

### `copy-pattern`

`copy-pattern` is not a sort-based resolver (`desc` has no meaning for this resolver). Instead, for each group of duplicate file candidates, `copy-pattern` marks as non-originals any files whose file names match common patterns used to indicate that a file has been duplicated.

The regular expressions used to do this matching are:
  - `^Copy of`
  - `.* copy [0-9]+\.[a-zA-Z0-9]{3}+$`
  - `^\._.+`
  - `^[0-9]_.+`
  - `\([0-9]\)\.[a-zA-Z0-9]{3}$`

`copy-pattern` is a useful resolver to specify early in the sequence. 

### `interactive`

The `interactive` resolver will stop the process for each group of duplicated files and request user input to resolve the duplicates. Other resolvers, like `copy-pattern`, may be specified before `interactive` to reduce the set of duplicates, but resolvers specified after `copy-pattern` will never be invoked.

## Sinks

`dedupe.py` comes with the following sinks.

### `delete`

Duplicated products are immediately deleted.

### `sequester`

A required command line option, `--sink-sequester-path PATH`, provides the location of another directory tree, which must be outside all of the sources.

The `sequester` sink will move all duplicate files within the sequester tree, replicating their original hierarchy position within their sources.

### `output-only`

The `output-only` sink will output the full paths of all files identified as non-original duplicates for later resolution. the `--sink-output-only-path` argument allows a file to be specified to receive this data; otherwise, it is written to standard output.

`dedupe.py` is available under the MIT License. (c) 2017 David Reed. This tool is in beta stage and is provided without warranty of any kind; use `dedupe.py` at your own risk and in the understanding that it is designed to make alterations, including deletions, to your data.

`dedupe.py` requires Python 3 and has been tested under Linux only. It is expected to work under Mac OS X, but has not been tested. While it is thought likely to work under Windows, it is completely untested and its behavior is not known.
