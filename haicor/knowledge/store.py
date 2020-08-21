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
from typing import Any, Generator, Iterable, Optional, Tuple

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
            relations = {relation: directed == "directed"
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

            (type_id, _), type = relation_mapping[relation[3:]], relation[3:]
            source_id, source = concept_mapping[source]
            target_id, target = concept_mapping[target]
            weight = json.loads(info)["weight"]

            assertion = Assertion(type, source, target, weight)
            self.execute("INSERT INTO assertions VALUES (?,?,?,?,?);",
                         (id, type_id, source_id, target_id, weight))

            if verify and str(assertion) != uri:
                raise RuntimeWarning(f"{uri} changed into {assertion}")

        self.commit()

    def is_directed(self, type: str) -> bool:
        """Check if the given relation type is directed"""

        result = self.execute("SELECT * FROM relations WHERE type == ?",
                              (type,)).fetchone()

        if result is None:
            raise RuntimeError(f"unknown relation type {type}")

        return result[2] == 1

    def get_concepts(self, text: Optional[str] = None,
                     speech: Optional[str] = None,
                     suffix: Optional[str] = None) -> Generator[Concept]:
        """Get all concepts that matches the query"""

        statement, parameters = self.concept_clause(text, speech, suffix)

        return (Concept(*i[1:]) for i in self.execute(statement, parameters))

    def get_concepts_id(self, text: Optional[str] = None,
                        speech: Optional[str] = None,
                        suffix: Optional[str] = None) -> Generator[int]:
        """Get all concepts' id that matches the query"""

        statement, parameters = self.concept_clause(text, speech, suffix)

        return (i[0] for i in self.execute(statement, parameters))

    def get_assertions(self, type: Optional[str] = None,
                       source: Optional[Concept] = None,
                       target: Optional[Concept] = None) -> Generator[Assertion]:
        """Get all assertions that matches the query"""

        source = source and self.get_concepts_id(text=source.text,
                                                 speech=source.speech,
                                                 suffix=source.suffix)
        target = target and self.get_concepts_id(text=target.text,
                                                 speech=target.speech,
                                                 suffix=target.suffix)
        statement, parameters = self.assertion_clause(type, source, target)

        return (Assertion(i[1], Concept(*i[4:7]), Concept(*i[8:11]), i[2])
                for i in self.execute(statement, parameters))

    def get_assertions_id(self, type: Optional[str] = None,
                          source: Optional[Concept] = None,
                          target: Optional[Concept] = None) -> Generator[int]:
        """Get all assertions' id that matches the query"""

        source = source and self.get_concepts_id(text=source.text,
                                                 speech=source.speech,
                                                 suffix=source.suffix)
        target = target and self.get_concepts_id(text=target.text,
                                                 speech=target.speech,
                                                 suffix=target.suffix)
        statement, parameters = self.assertion_clause(type, source, target)

        return (i[0] for i in self.execute(statement, parameters))

    @staticmethod
    def in_clause(field: str, value: tuple) -> str:
        return f"{field} IN {value}"

    @staticmethod
    def equal_clause(field: str, value: Any) -> Tuple[str, Any]:
        return f"{field} == ?", value

    @staticmethod
    def where_clause(**kwargs) -> Tuple[str, tuple]:
        clauses, parameters = [], []
        for field, value in kwargs.items():
            if value is None:
                continue

            if isinstance(value, tuple):
                clauses.append(ConceptNetStore.in_clause(field, value))
            else:
                clause, parameter = ConceptNetStore.equal_clause(field, value)

                clauses.append(clause)
                parameters.append(parameter)

        return " AND ".join(clauses), tuple(parameters)

    @staticmethod
    def concept_clause(text: Optional[str] = None,
                       speech: Optional[str] = None,
                       suffix: Optional[str] = None) -> Tuple[str, tuple]:
        statement, parameters = ConceptNetStore.where_clause(text=text,
                                                             speech=speech,
                                                             suffix=suffix)
        statement = ("SELECT id, text, speech, suffix FROM concepts"
                     + (f" WHERE {statement}" if statement else ""))

        return statement, parameters

    @staticmethod
    def assertion_clause(type: Optional[str] = None,
                         source: Optional[Iterable[int]] = None,
                         target: Optional[Iterable[int]] = None) -> Tuple[str, tuple]:
        source, target = source and tuple(source), target and tuple(target)
        statement, parameters = ConceptNetStore.where_clause(relation=type,
                                                             source_id=source,
                                                             target_id=target)
        statement = ("SELECT assertions.id, relations.type AS relation, weight,"
                     + " cs.id AS source_id, cs.text, cs.speech, cs.suffix,"
                     + " ct.id AS target_id, ct.text, ct.speech, ct.suffix"
                     + " FROM assertions"
                     + " JOIN concepts AS cs ON cs.id == assertions.source"
                     + " JOIN concepts AS ct ON ct.id == assertions.target"
                     + " JOIN relations ON relations.id == assertions.type"
                     + (f" WHERE {statement}" if statement else ""))

        return statement, parameters
