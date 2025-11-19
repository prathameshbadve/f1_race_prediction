"""
Helper functions for the project
"""

from pathlib import Path


def get_project_root() -> Path:
    """
    Returns the root directory of the project.
    """

    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents):
        if (parent / "pyproject.toml").exists():
            return parent

    raise RuntimeError("Project root not found.")


def ensure_directory(path: Path | str):
    """
    Ensures that the directory at the given path exists.
    If it does not exist, it is created.
    """

    if isinstance(path, str):
        path = Path(path)

    Path(path).mkdir(parents=True, exist_ok=True)
