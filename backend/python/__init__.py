"""
ESA Backend — Flask web app + sync orchestrator daemon.

(Legacy APScheduler daemon was decommissioned; pipelines run on the
orchestrator at sync_service/.)
"""

from pathlib import Path

# Read version from VERSION file
_version_file = Path(__file__).parent / 'VERSION'
if _version_file.exists():
    __version__ = _version_file.read_text().strip()
else:
    __version__ = '1.0.0'


def get_version():
    """Return the current backend version."""
    return __version__


__all__ = ['__version__', 'get_version']
