"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

import csv
import gzip
import json
import re
import sqlite3 as sqlite
from itertools import chain

from .types import Assertion, Concept

CONCEPT_URI = re.compile(r"^/c/en/([^/]+)(?:/(\w))?(?:/(.+?))?/?$")

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

    def create(self, reset: bool = False):
        """Create the necessary tables"""

        if reset:  # delete concepts and assertions tables if exist
            self.execute("DROP TABLE IF EXISTS concepts;")
            self.execute("DROP TABLE IF EXISTS relations;")
            self.execute("DROP TABLE IF EXISTS assertions;")

        self.executescript(CREATE_SQL)
        self.commit()

    def populate(self, assertions: str, relations: str, verify: bool = True):
        """Populate the database with supplied ConceptNet file"""

        with gzip.open(assertions, "rt") as file:
            assertions = {uri: (type, source, target, info)
                          for uri, type, source, target, info
                          in csv.reader(file, delimiter="\t")
                          if source[:6] == target[:6] == "/c/en/"}

        with open(relations, "r") as file:
            relations = {f"/r/{relation}": directed == "directed"
                         for relation, directed in csv.reader(file)}

        concept_mapping = {}
        relation_mapping = {}

        concepts = chain.from_iterable(i[1:3] for i in assertions.values())
        for id, uri in enumerate(set(concepts), start=1):
            if (match := re.match(CONCEPT_URI, uri)) is None:
                raise RuntimeError(f"{uri} cannot be parsed into Concept")

            concept = Concept(*match.group(1, 2, 3))
            concept_mapping[uri] = (id, concept)

            if verify and str(concept) != uri and str(concept) != uri[:-1]:
                raise RuntimeWarning(f"{uri} changed into {concept}")

        self.executemany("INSERT INTO concepts VALUES (?,?,?,?);",
                         ((i, c.text, c.speech, c.suffix)
                          for i, c in concept_mapping.values()))

        for id, (relation, directed) in enumerate(relations.items(), start=1):
            relation_mapping[relation] = (id, directed)

        self.executemany("INSERT INTO relations VALUES (?,?,?);",
                         ((i, r, d) for r, (i, d) in relation_mapping.items()))

        for id, (uri, fields) in enumerate(assertions.items(), start=1):
            relation, source, target, info = fields

            (type_id, _), type = relation_mapping[relation], relation[3:]
            source_id, source = concept_mapping[source]
            target_id, target = concept_mapping[target]
            weight = json.loads(info)["weight"]

            assertion = Assertion(type, source, target, weight)
            self.execute("INSERT INTO assertions VALUES (?,?,?,?,?);",
                         (id, type_id, source_id, target_id, weight))

            if verify and str(assertion) != uri:
                raise RuntimeWarning(f"{uri} changed into {assertion}")

        self.commit()
