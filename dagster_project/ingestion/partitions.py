"""
Asset partitions for seasons and F1 sessions
"""

from dagster import (
    DynamicPartitionsDefinition,
    StaticPartitionsDefinition,
)

# =============================================================================
# Static Partitions
# =============================================================================

# This is the partitions definition for F1 seasons
F1_SEASON_PARTITION = StaticPartitionsDefinition(["2023", "2024"])

# =============================================================================
# Dynamic Partitions (Populated by Asset Sensor)
# =============================================================================

# This is the main partition definition for F1 sessions
# It will be populated dynamically by your asset sensor
# Format: "year|grand_prix|session"
# Examples: "2024|Bahrain Grand Prix|Race", "2024|Monaco Grand Prix|Qualifying"
F1_SESSIONS_PARTITION = DynamicPartitionsDefinition(name="f1_sessions")
