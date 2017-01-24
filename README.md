# dedupe.py

This tool provides functionality for scanning multiple file hierarchies for duplicate files
occurring at any depth and configurably managing these duplicates. `dedupe.py` is intended to support
merging large, structurally divergent hierarchies, such as overlapping photo collections archived in one
area by date and in another by album. It applies a user-specified sequence of resolvers to determine which
copy of a duplicated file to keep, using criteria like modification and creation date, file tree order,
path depth, and so on.

Duplicated items may be deleted, sequestered in a separate file tree, or labeled in a file.

For details of available resolvers and options, run `dedupe.py -h`

`dedupe.py` is available under the MIT License. (c) 2017 David Reed. This tool is pre-alpha stage.
