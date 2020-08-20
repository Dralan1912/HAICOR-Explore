"""
Copyright (c) 2020 Hecong Wang

This software is released under the MIT License.
https://opensource.org/licenses/MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Concept:
    """Dataclass used to represent English concepts from ConceptNet"""

    text: str
    speech: Optional[str] = None
    suffix: Optional[str] = None

    def __str__(self) -> str:
        """ConceptNet concept URI representation"""

        return (f"/c/en/{self.text}"
                + (f"/{self.speech}" if self.speech else "")
                + (f"/{self.suffix}" if self.suffix else ""))


@dataclass(frozen=True)
class Assertion:
    """Dataclass used to represent English assertions from ConceptNet"""

    type: str
    source: Concept
    target: Concept

    def __str__(self) -> str:
        """ConceptNet assertion URI representation"""

        return (f"/a/[{self.type}/,{self.source}/,{self.target}/]")
