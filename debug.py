"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

import sqlite3 as sqlite

from haicor.knowledge.store import ConceptNetStore

if __name__ == "__main__":
    with sqlite.connect("data/englishnet-assertions-5.7.0.sqlite",
                        factory=ConceptNetStore) as database:
        database: ConceptNetStore = database
        database.create(reset=True)
        database.populate("data/conceptnet-assertions-5.7.0.csv.gz",
                          "data/relations.csv")
