import os
from ast import literal_eval
from typing import Any, Dict

from .errors import InvalidPluginError


def discover_filesystem_implementations() -> Dict[str, Any]:
    """Find custom DVC FileSystem implementations."""
    if os.environ.get("DVC_FS_PLUGINS") is None:
        return {}
    implementations = literal_eval(os.environ["DVC_FS_PLUGINS"])
    if not isinstance(implementations, dict):
        raise InvalidPluginError(
            "DVC_FS_PLUGINS environment variable did not"
            "specify valid filesystem plugin(s)."
        )
    return implementations


exposed_implementations = discover_filesystem_implementations()
