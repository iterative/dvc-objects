from typing import TYPE_CHECKING
from urllib.parse import urlparse

from . import generic  # noqa: F401
from .implementations.azure import AzureFileSystem
from .implementations.gdrive import GDriveFileSystem
from .implementations.gs import GSFileSystem
from .implementations.hdfs import HDFSFileSystem
from .implementations.http import HTTPFileSystem
from .implementations.https import HTTPSFileSystem
from .implementations.local import LocalFileSystem
from .implementations.memory import MemoryFileSystem  # noqa: F401
from .implementations.oss import OSSFileSystem
from .implementations.s3 import S3FileSystem
from .implementations.ssh import SSHFileSystem
from .implementations.webdav import WebDAVFileSystem, WebDAVSFileSystem
from .implementations.webhdfs import WebHDFSFileSystem
from .scheme import Schemes

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

    from .base import FileSystem

FS_MAP = {
    Schemes.AZURE: AzureFileSystem,
    Schemes.GDRIVE: GDriveFileSystem,
    Schemes.GS: GSFileSystem,
    Schemes.HDFS: HDFSFileSystem,
    Schemes.WEBHDFS: WebHDFSFileSystem,
    Schemes.HTTP: HTTPFileSystem,
    Schemes.HTTPS: HTTPSFileSystem,
    Schemes.S3: S3FileSystem,
    Schemes.SSH: SSHFileSystem,
    Schemes.OSS: OSSFileSystem,
    Schemes.WEBDAV: WebDAVFileSystem,
    Schemes.WEBDAVS: WebDAVSFileSystem,
    # NOTE: LocalFileSystem is the default
}


def _import_class(cls: str):
    """Take a string FQP and return the imported class or identifier

    clas is of the form "package.module.klass".
    """
    import importlib

    mod, name = cls.rsplit(".", maxsplit=1)
    module = importlib.import_module(mod)
    return getattr(module, name)


def get_fs_cls(remote_conf, cls=None, scheme=None):
    if cls:
        return _import_class(cls)

    if not scheme:
        scheme = urlparse(remote_conf["url"]).scheme
    return FS_MAP.get(scheme, LocalFileSystem)


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
        protos = ("local", *protos)

    # if we have the class in our registry, instantiate with that.
    for proto in protos:
        if klass := FS_MAP.get(proto):
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
