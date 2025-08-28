"""
Tournament and district configuration models for the TTBW system.
"""

from dataclasses import dataclass


@dataclass
class TournamentConfig:
    """Configuration for a tournament including ID and point values."""
    tournament_id: int
    points: int


@dataclass
class DistrictConfig:
    """Configuration for a district including region and short name."""
    region: int
    short_name: str
