"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

import csv
import gzip
import json
import logging
import re
import sqlite3 as sqlite
from itertools import chain
from logging import Logger
from typing import Optional, Union

from .types import Assertion, Concept

CONCEPT = re.compile(r"^/c/en/([^/]+)(?:/(\w))?(?:/(.+))?/?$")

CREATE_SQL = """
CREATE TABLE concepts (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    text    TEXT    NOT NULL,
    speech  TEXT,
    suffix  TEXT,

    UNIQUE(text, speech, suffix)
);

CREATE TABLE relations (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    type    TEXT    UNIQUE NOT NULL,
    direct  BOOLEAN NOT NULL
);

CREATE TABLE assertions (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    type    INTEGER NOT NULL,
    source  INTEGER NOT NULL,
    target  INTEGER NOT NULL,
    weight  FLOAT   NOT NULL,

    UNIQUE(type, source, target),
    FOREIGN KEY(type) REFERENCES relations(id),
    FOREIGN KEY(source, target) REFERENCES concepts(id, id)
);
"""


class ConceptNetStore(sqlite.Connection):
    """Data store for English subset of ConceptNet"""

    def __init__(self, *args, **kwargs):
        super(ConceptNetStore, self).__init__(*args, **kwargs)

        self.logger: Logger = logging.getLogger()

    def create(self, reset: bool = False):
        """Create the necessary tables"""

        if reset:  # delete concepts and assertions tables if exist
            self.logger.info("Dropping old tables")
            self.execute("DROP TABLE IF EXISTS concepts;")
            self.execute("DROP TABLE IF EXISTS relations;")
            self.execute("DROP TABLE IF EXISTS assertions;")
            self.logger.info("Dropped old tables")

        self.logger.info("Creating necessary tables")
        self.executescript(CREATE_SQL)
        self.logger.info("Created necessary tables")
        self.commit()

    def populate(self, assertions: str, relations: str, processes: Optional[int] = None):
        """Populate the database with supplied ConceptNet file"""

        with gzip.open(assertions, "rt") as file:
            self.logger.info("Reading English assertions")
            assertions = {uri: (type, source, target, info)
                          for uri, type, source, target, info
                          in csv.reader(file, delimiter="\t")
                          if source[:6] == target[:6] == "/c/en/"}
            self.logger.info(f"Read {len(assertions)} English assertions")

        with open(relations, "r") as file:
            self.logger.info("Reading ConceptNet relations")
            relations = {relation: directed == "directed"
                         for relation, directed in csv.reader(file)}
            self.logger.info(f"Read {len(relations)} ConceptNet relations")

        self.logger.info("Populating concepts")
        concepts = chain.from_iterable(i[1:3] for i in assertions.values())
        concepts_lookup = {}

        for id, uri in enumerate(set(concepts), start=1):
            if not (match := re.match(CONCEPT, uri)):
                self.logger.error(f"Failed to parse {uri} into Concept")

            if str(concept := Concept(*match.group(1, 2, 3))) != uri:
                self.logger.warning(f"{concept} differs from {uri}")

            self.logger.debug(f"Parsed {uri} into Concept")
            concepts_lookup[uri] = (id, concept)
            self.execute(
                "INSERT INTO concepts (text, speech, suffix) VALUES (?, ?, ?);",
                (concept.text, concept.speech, concept.suffix)
            )

        self.logger.info(f"Populated {len(concepts_lookup)} concepts")
        self.logger.info("Populating relations")
        relations_lookup = {}

        for id, (type, directed) in enumerate(relations.items(), start=1):
            relations_lookup[type] = id
            self.execute(
                "INSERT INTO relations (type, direct) VALUES (?, ?);",
                (type, directed)
            )

        self.logger.info(f"Populated {len(relations_lookup)} relations")
        self.logger.info("Populating assertions")

        for uri, (type, source, target, info) in assertions.items():
            type_id, type = relations_lookup[type[3:]], type[3:]
            source_id, source = concepts_lookup[source]
            target_id, target = concepts_lookup[target]
            weight = json.loads(info)["weight"]

            if str(assertion := Assertion(type, source, target, weight)) != uri:
                self.logger.warning(f"{assertion} differs from {uri}")

            self.logger.debug(f"Parsed {uri} into Assertion")
            self.execute(
                ("INSERT INTO assertions (type, source, target, weight) "
                 "VALUES (?, ?, ?, ?);"),
                (type_id, source_id, target_id, weight)
            )

        self.logger.info(f"Populated {len(assertions)} assertions")
        self.commit()
