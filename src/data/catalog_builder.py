"""
Catalog raw data downloaded from API
"""

from dataclasses import dataclass, field

from dagster_project.shared.resources import BucketPath


@dataclass
class SessionFiles:
    """Files required for a sesssion"""

    results: bool = False
    laps: bool = False
    weather: bool = False
    session_status: bool = False
    track_status: bool = False
    messages: bool = False

    def has_all_critical_files(self):
        """
        Check if a session has all critical files

        Critical files:
            - results
            - laps
            - weather
        """

        return all((self.results, self.laps, self.weather))

    def completeness_score(self) -> float:
        """Calculate completeness percentage"""

        total_files = 6
        available = sum(
            [
                self.results,
                self.laps,
                self.weather,
                self.track_status,
                self.session_status,
                self.messages,
            ]
        )
        return (available / total_files) * 100


@dataclass
class SessionCatalogEntry:
    """
    Fields in a single catalog entry. One entry per F1 grand prix,
    eg - 2024|Italian Grand Prix|Race
    """

    round_number: int
    year: int
    grand_prix: str
    session_number: int
    session: str
    file_path: BucketPath

    # Metadata
    total_drivers: int
    total_laps: int = 0  # 0 for sessions other than race or sprint race
    event_format: str = "conventional"

    # Files availability
    files: SessionFiles = field(default_factory=SessionFiles)
    has_results: bool = False
    has_laps: bool = False
    has_session_status: bool = False
    has_track_status: bool = False
    has_weather: bool = False
    has_messages: bool = False
    completeness_score: float = 0.0

    def to_dict(self):
        """Convert the catalog entry to a dictionary"""

        return {
            "round_number": self.round_number,
            "year": self.year,
            "grand_prix": self.grand_prix,
            "session_number": self.session_number,
            "session": self.session,
            "file_path": self.file_path,
            # Metadata
            "total_drivers": self.total_drivers,
            "total_laps": self.total_laps,
            "event_format": self.event_format,
            # Files flags
            "has_results": self.files.results,
            "has_laps": self.files.laps,
            "has_session_status": self.files.session_status,
            "has_track_status": self.files.track_status,
            "has_weather": self.files.weather,
            "has_messages": self.files.messages,
            "completeness_score": self.completeness_score,
            "has_all_critical_files": self.files.has_all_critical_files(),
        }
