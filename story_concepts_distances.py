"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

import gzip
import multiprocessing as mp
import sqlite3 as sqlite

import graph_tool
from graph_tool import topology
from tqdm import tqdm

from haicor.knowledge.store import ConceptNetStore

INFINITY = 2147483647
DATABASE = "data/englishnet-assertions-5.7.0.sqlite"
STATEMENT = """
SELECT DISTINCT * FROM (
    SELECT source, target FROM assertions
    UNION
    SELECT target, source FROM assertions
    JOIN relations ON relations.id == assertions.type
    WHERE relations.direct == 0
);
"""

conceptnet = graph_tool.Graph(directed=True)


def shortest_distances(source):
    with gzip.open(f"data/distances/{source}.csv.gz", "wt") as file:
        distances = topology.shortest_distance(conceptnet, source)
        for target, distance in enumerate(distances.a):
            if distance < INFINITY:
                print(f"{target},{distance}", file=file)


if __name__ == "__main__":
    with sqlite.connect(DATABASE, factory=ConceptNetStore) as database:
        for source, target in database.execute(STATEMENT):
            conceptnet.add_edge(source - 1, target - 1)

    with open("data/concepts_id.txt", "r") as file:
        sources = [int(i) for i in file]

    with mp.Pool(8) as pool:
        list(tqdm(pool.imap(shortest_distances, sources),
                  desc="Path finding progress", total=len(sources)))
