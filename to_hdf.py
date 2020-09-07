"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

import argparse
import gzip
import multiprocessing as mp
import os
from functools import partial
from typing import List, Optional, Tuple

import h5py
import numpy as np
from tqdm import tqdm

write_lock = mp.Lock()


def read_distances(filename: str) -> List[Tuple[int, int]]:
    with gzip.open(filename, "rt") as file:
        return [(int(target), int(distance)) for target, distance
                in (line.split(',') for line in file)]


def convert(filename: str, distances: str, directory: Optional[str] = None):
    data = np.array(read_distances(os.path.join(directory or "", distances)))

    with write_lock, h5py.File(filename, "a") as file:
        file.create_dataset(distances.split('.')[0],
                            data=data, shape=data.shape,
                            compression="gzip")


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument("source", type=str)
    parser.add_argument("target", type=str)

    args = parser.parse_args()

    target, source = args.target, args.source

    # conversion
    with mp.Pool() as pool:
        for _ in tqdm(pool.imap(partial(convert, target, directory=source),
                                os.listdir(source)),
                      total=len(os.listdir(source))):
            pass
