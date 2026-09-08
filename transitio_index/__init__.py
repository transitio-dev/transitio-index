"""Build stages for the place-based feed index.

Maintainer tooling, not part of the installed package: the library reads a
finished index, it never builds one. Stages communicate through files in a
build cache so each can be re-run on its own; see plans/place-index.md.
"""
