"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

from itertools import groupby
from typing import Generator, Iterable, Tuple


class ConceptExtractor:
    """A trie-based concept extractor"""

    def __init__(self, dictionary: Iterable[str]):
        self.trie = self.build_trie(i.split() for i in dictionary)

    def extract(self, tokens: Iterable[str]) -> Generator[Tuple[int, Tuple[str, ...]], None, None]:
        """Extract all concept that is in the dictionary, from the tokens"""

        trackers = []

        for start, token in enumerate(tokens):
            updated = []

            for (start, match), trie in trackers + [((start, []), self.trie)]:
                if token in trie:
                    matched, new_trie = trie[token]
                    updated.append(((start, match + [token]), new_trie))

                    if matched:
                        yield start, match + [token]

            trackers = updated

    @staticmethod
    def build_trie(dictionary: Iterable[Tuple[str, ...]]) -> dict:
        """Construct a trie from the dictionary"""

        def trie(head: str, tails: Iterable[Tuple[str, ...]]) -> Tuple[bool, dict]:
            match = [] in tails
            tails = sorted((i for i in tails if i), key=lambda x: x[0])

            return (match, {h: trie(h, [i[1:] for i in t]) for h, t
                            in groupby(tails, key=lambda x: x[0])})

        return trie(None, list(dictionary))[1]
