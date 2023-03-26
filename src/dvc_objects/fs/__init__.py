from collections.abc import Mapping
from typing import TYPE_CHECKING, Iterator, List, Type
from urllib.parse import urlparse

from importlib_metadata import entry_points

from . import generic  # noqa: F401
from .errors import SchemeCollisionError
from .local import LocalFileSystem, localfs  # noqa: F401
from .memory import MemoryFileSystem  # noqa: F401
from .scheme import Schemes

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

    from .base import FileSystem


known_implementations = {
    Schemes.LOCAL: {"class": "dvc_objects.fs.local.LocalFileSystem"},
    Schemes.MEMORY: {"class": "dvc_objects.fs.memory.MemoryFileSystem"},
    Schemes.AZURE: {
        "class": "dvc_azure.AzureFileSystem",
        "err": "azure is supported, but requires 'dvc-azure' to be installed",
    },
    Schemes.GDRIVE: {
        "class": "dvc_gdrive.GDriveFileSystem",
        "err": ("gdrive is supported, but requires 'dvc-gdrive' to be installed"),
    },
    Schemes.GS: {
        "class": "dvc_gs.GSFileSystem",
        "err": "gs is supported, but requires 'dvc-gs' to be installed",
    },
    Schemes.HDFS: {
        "class": "dvc_hdfs.HDFSFileSystem",
        "err": "hdfs is supported, but requires 'dvc-hdfs' to be installed",
    },
    Schemes.HTTP: {
        "class": "dvc_http.HTTPFileSystem",
        "err": "http is supported, but requires 'dvc-http' to be installed",
    },
    Schemes.HTTPS: {
        "class": "dvc_http.HTTPSFileSystem",
        "err": "https is supported, but requires 'dvc-http' to be installed",
    },
    Schemes.OSS: {
        "class": "dvc_oss.OSSFileSystem",
        "err": "oss is supported, but requires 'dvc-oss' to be installed",
    },
    Schemes.S3: {
        "class": "dvc_s3.S3FileSystem",
        "err": "s3 is supported, but requires 'dvc-s3' to be installed",
    },
    Schemes.SSH: {
        "class": "dvc_ssh.SSHFileSystem",
        "err": "ssh is supported, but requires 'dvc-ssh' to be installed",
    },
    Schemes.WEBDAV: {
        "class": "dvc_webdav.WebDAVFileSystem",
        "err": ("webdav is supported, but requires 'dvc-webdav' to be installed"),
    },
    Schemes.WEBDAVS: {
        "class": "dvc_webdav.WebDAVSFileSystem",
        "err": ("webdavs is supported, but requires 'dvc-webdav' to be installed"),
    },
    Schemes.WEBHDFS: {
        "class": "dvc_webhdfs.WebHDFSFileSystem",
        "err": ("webhdfs is supported, but requires 'dvc-webhdfs' to be installed"),
    },
}


def _import_class(cls: str):
    """Take a string FQP and return the imported class or identifier

    cls is of the form "package.module.klass".
    """
    import importlib

    mod, name = cls.rsplit(".", maxsplit=1)
    module = importlib.import_module(mod)
    return getattr(module, name)


class Registry(Mapping):
    def __init__(self, reg) -> None:
        self._registry = reg
        self._custom_schemes: List[str] = []

    @property
    def custom_schemes(self) -> List[str]:
        """Return the names of FileSystem schemes from plugins."""
        return self._custom_schemes

    def register_plugins(self):
        """Add custom FileSystem implementations to the registry.

        Custom FileSystem implementations are discovered using entrypoints
        from installed packages.

        An example `setup.py` for a package containing a custom FileSystem
        implementation looks like:
        ```
        from setuptools import setup

        setup(
            # ...,
            entry_points = {
                'dvc_objects.fs_plugins': [
                    'custom = my_package.MyCustomFileSystem'
                ]
            }
        )
        ```
        which would make the `custom` protocol available to DVC.
        e.g. `dvc remote add remote custom://url/for/custom/fs`
        """
        for entrypoint in entry_points(group="dvc_objects.fs_plugins"):
            if entrypoint.name in self._registry:
                raise SchemeCollisionError(
                    f"Plugin: {entrypoint.value} tried to use scheme={entrypoint.name},"
                    "\tbut this is already in use."
                )
            self._custom_schemes.append(entrypoint.name)
            self._registry[entrypoint.name] = {"class": entrypoint.value}

    def __getitem__(self, key: str) -> Type["FileSystem"]:
        entry = self._registry.get(key) or self._registry[Schemes.LOCAL]
        try:
            return _import_class(entry["class"])
        except ImportError as exc:
            raise ImportError(entry["err"]) from exc

    def __iter__(self) -> Iterator[str]:
        yield from self._registry

    def __contains__(self, key: object) -> bool:
        return key in self._registry

    def __len__(self) -> int:
        return len(self._registry)


registry = Registry(known_implementations)
registry.register_plugins()


def get_fs_cls(remote_conf, cls=None, scheme=None):
    if cls:
        return _import_class(cls)

    if not scheme:
        scheme = urlparse(remote_conf["url"]).scheme
    return registry.get(scheme)


def as_filesystem(
    fs: "AbstractFileSystem",
    checksum: str = "md5",
    object_based: bool = False,
    **fs_args,
) -> "FileSystem":
    """
    Provides a way to transform any fsspec-based filesystems into a
    dvc_objects.base.FileSystem compatible filesystem.

    This iterates through subclasses at first, and then creates an instance.
    If there's no existing subclass, it'll create a new subclass of
    the FileSystem and create a new instance out of it (the subclass will be
    reused, so please give attention to provided args, they should not change
    for the same filesystem).

    This is only intended for testing, please don't use it anywhere else.
    """
    from .base import FileSystem, ObjectFileSystem

    if isinstance(fs, FileSystem):
        return fs

    protos = (fs.protocol,) if isinstance(fs.protocol, str) else fs.protocol
    if "file" in protos:
        if klass := registry.get(Schemes.LOCAL):
            return klass()

    # if we have the class in our registry, instantiate with that.
    for proto in protos:
        if proto in registry:
            if klass := registry.get(proto):
                return klass(fs=fs, **fs_args)

    # fallback to unregistered subclasses
    for subclass in FileSystem.__subclasses__():
        for proto in protos:
            if proto == subclass.protocol:
                return subclass(fs=fs, **fs_args)

    # if that does not exist, create a new subclass and instantiate
    # from that (the subclass will be reused)
    fs_cls = ObjectFileSystem if object_based else FileSystem
    new_subclass = type(
        fs.__class__.__name__,
        (fs_cls,),
        {"PARAM_CHECKSUM": checksum, "protocol": protos[0]},
    )
    return new_subclass(fs=fs, **fs_args)
