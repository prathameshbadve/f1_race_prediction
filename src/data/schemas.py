"""
Data validation schemas for F1 data using Pydantic.
"""

from datetime import timedelta

from pydantic import BaseModel, Field, field_validator, model_validator

from src.config.logging import get_logger

logger = get_logger("transformation.validator")


class RaceResultSchema(BaseModel):
    """Schema for race/sprint results"""

    # Driver & Team Identifiers
    DriverNumber: str = Field(description="Driver number as string")
    Abbreviation: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver code"
    )
    TeamId: str = Field(..., description="Lowercase name of team")
    FullName: str = Field(..., description="Driver full name")

    # Race results
    Position: float = Field(..., ge=1, le=22, description="Finishing position")
    ClassifiedPosition: str = Field(..., description="Classified position")
    GridPosition: float = Field(
        ..., ge=0, le=22, description="Starting grid position (0 for pit start)"
    )

    # Timing (Optional - may be null for DNF, R)
    Time: timedelta | None = Field(description="Race time or time behind winner")

    Status: str = Field(..., description="Finish status")
    Points: float = Field(..., ge=0, le=26, description="Championship points earned")
    Laps: float = Field(..., ge=0, le=100, description="Laps completed by the driver")

    @field_validator("Abbreviation")
    @classmethod
    def validate_abbreviation(cls, v: str) -> str:
        """Ensure abbreviation is uppercase"""
        return v.upper()

    @field_validator("DriverNumber")
    @classmethod
    def validate_driver_number(cls, v: str) -> str:
        """Ensure driver number is valid"""
        try:
            num = int(v)
            if not 1 <= num <= 99:
                raise ValueError(f"Driver number must be between 1 and 99, got {num}")
        except ValueError as e:
            logger.warning("Invalid driver number: %s", v)
            raise ValueError(f"Driver number must be numeric, got {v}") from e
        return v

    @model_validator(mode="after")
    def validate_time_vs_status(self):
        """Checks that valid timedelta is present for finished and lapped drivers"""

        if self.Status == "Finished" and self.Time is None:
            raise ValueError(f"Time is required when Status is '{self.Status}'")

        return self


class QualifyingResultSchema(BaseModel):
    """Schema for qualifying results"""

    # Driver & Team Identifiers
    DriverNumber: str = Field(description="Driver number as string")
    Abbreviation: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver code"
    )
    TeamId: str = Field(..., description="Lowercase name of team")
    FullName: str = Field(..., description="Driver full name")

    # Race results
    Position: float = Field(..., ge=1, le=22, description="Finishing position")

    # Qualifying session times
    Q1: timedelta | None = Field(description="Best Q1 Time")
    Q2: timedelta | None = Field(description="Best Q2 Time")
    Q3: timedelta | None = Field(description="Best Q3 Time")

    @field_validator("Abbreviation")
    @classmethod
    def validate_abbreviation(cls, v: str) -> str:
        """Ensure abbreviation is uppercase"""
        return v.upper()

    @field_validator("DriverNumber")
    @classmethod
    def validate_driver_number(cls, v: str) -> str:
        """Ensure driver number is valid"""
        try:
            num = int(v)
            if not 1 <= num <= 99:
                raise ValueError(f"Driver number must be between 1 and 99, got {num}")
        except ValueError as e:
            logger.warning("Invalid driver number: %s", v)
            raise ValueError(f"Driver number must be numeric, got {v}") from e
        return v


class WeatherSchema(BaseModel):
    """Validation schema for weather data"""

    Time: timedelta = Field(..., description="Time of recording the weather")
    AirTemp: float = Field(..., description="Air temperature")
    Humidity: float = Field(..., description="Humidity")
    Pressure: float = Field(..., description="Pressure")
    Rainfall: bool = Field(
        ..., description="Indicated if it was raining at the time of recording"
    )
    TrackTemp: float = Field(..., description="Track temperature")
    WindDirection: int = Field(..., ge=-360, le=360, description="Wind direction")
    WindSpeed: float = Field(..., description="Wind speed")
