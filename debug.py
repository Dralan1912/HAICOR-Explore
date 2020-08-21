"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

import logging
import sqlite3 as sqlite

from haicor.knowledge.store import ConceptNetStore

if __name__ == "__main__":
    logger = logging.getLogger()

    formatter = logging.Formatter("%(asctime)s [%(levelname)8s] - %(msg)s")

    terminal_handler = logging.StreamHandler()
    terminal_handler.setFormatter(formatter)
    terminal_handler.setLevel(logging.DEBUG)

    logger.addHandler(terminal_handler)
    logger.setLevel(logging.DEBUG)

    with sqlite.connect("data/database.sqlite", factory=ConceptNetStore) as database:
        database: ConceptNetStore = database

        database.create(reset=True)
        database.populate("data/conceptnet-assertions-5.7.0.csv.gz",
                          "data/relations.csv")
