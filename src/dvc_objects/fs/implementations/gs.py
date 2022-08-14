import threading

from funcy import cached_property, wrap_prop

# pylint:disable=abstract-method
from ..base import ObjectFileSystem


class GSFileSystem(ObjectFileSystem):
    protocol = "gs"
    REQUIRES = {"gcsfs": "gcsfs"}
    PARAM_CHECKSUM = "etag"

    def _prepare_credentials(self, **config):
        login_info = {"consistency": None}
        login_info["project"] = config.get("projectname")
        login_info["token"] = config.get("credentialpath")
        login_info["endpoint_url"] = config.get("endpointurl")
        login_info["session_kwargs"] = {"trust_env": True}
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from gcsfs import GCSFileSystem

        return GCSFileSystem(**self.fs_args)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        return infer_storage_options(path)["path"]

    def unstrip_protocol(self, path: str) -> str:
        return "gs://" + path.lstrip("/")
