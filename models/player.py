"""
Player data models for the TTBW system.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime


@dataclass
class PlayerRecord:
    """Database record for a player."""
    interne_lizenznr: str
    first_name: str
    last_name: str
    club: str
    gender: str
    district: str
    birth_year: int
    age_class: int
    region: int
    qttr: Optional[int] = None
    club_number: Optional[str] = None
    verband: str = "TTBW"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Player:
    """Player information and tournament results."""
    id: str
    first_name: str
    last_name: str
    club: str
    gender: str
    district: str
    birth_year: int
    age_class: int
    region: int
    qttr: Optional[int] = None
    points: float = 0.0
    tournaments: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def __post_init__(self):
        if self.tournaments is None:
            self.tournaments = {}
