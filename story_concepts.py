"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

import json
import multiprocessing as mp
import sqlite3 as sqlite
from itertools import chain

from nltk.tokenize import word_tokenize
from tqdm import tqdm

from haicor.knowledge.store import ConceptNetStore
from haicor.process.extractor import ConceptExtractor

DATABASE = "data/englishnet-assertions-5.7.0.sqlite"
DATASET = "data/story-commonsense/json_version/annotations.json"

extractor = None


def extract_concepts(tokens):
    return list('_'.join(i[1]) for i in extractor.extract(tokens))


if __name__ == "__main__":
    with sqlite.connect(DATABASE, factory=ConceptNetStore) as database:
        concepts = {i.text.replace('_', ' ') for i in database.get_concepts()}
        extractor = ConceptExtractor(i.lower() for i in concepts)

    with open(DATASET, "r") as file:
        dataset = json.load(file).values()
        sentences = [l["text"] for s in dataset for l in s["lines"].values()]

    with mp.Pool() as pool:
        sentences = pool.map(str.lower, sentences)
        sentences = pool.map(word_tokenize, sentences)
        concepts = list(tqdm(pool.imap(extract_concepts, sentences),
                             desc="Extraction process", total=len(sentences)))
        concepts = {i for i in chain.from_iterable(concepts)}

    with sqlite.connect(DATABASE, factory=ConceptNetStore) as database:
        concepts_id = [database.get_concepts_id(text=i) for i in concepts]
        concepts_id = {i for i in chain.from_iterable(concepts_id)}

    with open("data/concepts.txt", "w") as file:
        for concept in sorted(concepts):
            print(concept, file=file)

    with open("data/concepts_id.txt", "w") as file:
        for concept_id in sorted(concepts_id):
            print(concept_id, file=file)
