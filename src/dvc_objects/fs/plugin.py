import importlib
import inspect
import logging
import pkgutil
from types import ModuleType
from typing import Any, Dict, Optional

from .base import FileSystem
from .errors import SchemeCollisionError

logger = logging.getLogger(__name__)

EXPOSED_IMPLEMENTATIONS = "exposed_implementations"


def _import_fsplugin_module() -> Optional[ModuleType]:
    """Import the plugin module if it exists."""
    try:
        filesystem_plugins = importlib.import_module("dvc_fs_plugins")
        return filesystem_plugins
    except ImportError:
        return


def _is_fsplugin(obj: Any) -> bool:
    """Determine if the provided object is a FileSystem subclass."""
    return inspect.isclass(obj) and issubclass(obj, FileSystem)


def discover_filesystem_plugins() -> Dict[str, Any]:
    """Find custom DVC FileSystem plugins.

    Custom DVC FileSystem Plugins can be implemented as in the example...
    """
    plugin_module = _import_fsplugin_module()
    if plugin_module is None:
        return None
    all_exposed_implementations = {}
    for _, modname, _ in pkgutil.iter_modules(
        path=plugin_module.__path__, prefix=plugin_module.__name__ + "."
    ):
        plugin_module = importlib.import_module(modname)
        if EXPOSED_IMPLEMENTATIONS in plugin_module.__dict__:
            _exposed_implementations = getattr(plugin_module, EXPOSED_IMPLEMENTATIONS)
        for scheme, implementation in _exposed_implementations.items():
            # Check exposed implementation schema don't collide with each other.
            if scheme in all_exposed_implementations:
                raise SchemeCollisionError(
                    f"{implementation} tried to use {scheme=},"
                    f"already in use by {all_exposed_implementations[scheme]}."
                )
            all_exposed_implementations[scheme] = implementation
    return all_exposed_implementations


exposed_implementations = discover_filesystem_plugins()

if __name__ == "__main__":
    print(discover_filesystem_plugins())
