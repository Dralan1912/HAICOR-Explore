"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

import functools
import multiprocessing as mp
from typing import List, Tuple

import graph_tool as gt
import h5py
import numpy as np

Concept = int
Triplet = Tuple[Tuple[Concept, ...], Tuple[Concept, ...], Tuple[Concept, ...]]


class Evaluator:
    def __init__(self, filename: str):
        self.filename = filename

        with h5py.File(self.filename, "r") as file:
            self.available_concepts = {int(i) for i in file.keys()}

    def evaluate(self, path: List[Concept],
                 context: List[Concept],
                 triplet: List[Triplet]) -> Tuple[float, float, float]:
        return (self.simplicity(path),
                self.specificity(path, context),
                self.rationality(path, triplet))

    def simplicity(self, path: List[Concept]) -> float:
        return float(len(path))

    def specificity(self, path: List[Concept], context: List[Concept]) -> float:
        assert set(context).issubset(self.available_concepts)

        with mp.Pool() as pool:
            results = pool.map(functools.partial(self.path_specificity, path),
                               context)

        return np.sum(np.average(np.array(results), axis=0))

    def path_specificity(self, path: List[Concept], context: Concept) -> List[float]:
        with h5py.File(self.filename, "r") as file:
            distance = file.get(str(context))
            distance = distance[np.isin(distance[:, 0], path), :]
            distance = {key: 1.0 / (1 + value) for key, value in distance}

            return [distance.get(i, 0.0) for i in path]

    def rationality(self, path: List[Concept], triplet: List[Triplet]) -> float:
        pass  # TODO


if __name__ == "__main__":
    import sqlite3 as sqlite

    import tqdm

    from story_concepts_distances import DATABASE, STATEMENT, ConceptNetStore

    # setup conceptnet graph
    conceptnet = gt.Graph(directed=True)

    with sqlite.connect(DATABASE, factory=ConceptNetStore) as database:
        for source, target in database.execute(STATEMENT):
            conceptnet.add_edge(source - 1, target - 1)

    # setup evaluator
    evaluator = Evaluator("../ConceptNet/distances.hdf5")

    # experiment
    with open("data/concepts_id.txt") as file:
        context = [int(i) for i in file][:100]
