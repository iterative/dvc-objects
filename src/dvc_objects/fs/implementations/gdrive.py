import logging
import os
import threading

from funcy import cached_property, wrap_prop

from ..base import FileSystem
from ..errors import ConfigError

logger = logging.getLogger(__name__)
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GDriveFileSystem(FileSystem):  # pylint:disable=abstract-method
    protocol = "gdrive"
    PARAM_CHECKSUM = "checksum"
    REQUIRES = {"pydrive2": "pydrive2"}
    # Always prefer traverse for GDrive since API usage quotas are a concern.
    TRAVERSE_WEIGHT_MULTIPLIER = 1

    GDRIVE_CREDENTIALS_DATA = "GDRIVE_CREDENTIALS_DATA"

    DEFAULT_GDRIVE_CLIENT_ID = "710796635688-iivsgbgsb6uv1fap6635dhvuei09o66c.apps.googleusercontent.com"  # noqa: E501
    DEFAULT_GDRIVE_CLIENT_SECRET = "a1Fz59uTpVNeG_VGuSKDLJXv"

    def __init__(self, **config):
        from fsspec.utils import infer_storage_options

        super().__init__(**config)

        self.url = config["url"]
        opts = infer_storage_options(self.url)

        if not opts["host"]:
            raise ConfigError("Empty GDrive URL '{}'.".format(config["url"]))

        self._path = opts["host"] + opts["path"]

        client_id = config.get(
            "gdrive_client_id", self.DEFAULT_GDRIVE_CLIENT_ID
        )
        client_secret = config.get(
            "gdrive_client_secret", self.DEFAULT_GDRIVE_CLIENT_SECRET
        )
        use_service_account = config.get("gdrive_use_service_account")
        client_user_email = config.get("gdrive_service_account_user_email")
        profile = config.get("profile")

        if use_service_account:
            client_json_file_path = config.get(
                "gdrive_service_account_json_file_path"
            )
        else:
            client_json_file_path = config.get("gdrive_user_credentials_file")

        env_creds = os.getenv(self.GDRIVE_CREDENTIALS_DATA)

        # Validate Service Account configuration
        if use_service_account and not (client_json_file_path or env_creds):
            raise ConfigError(
                "To use service account, set "
                "`gdrive_service_account_json_file_path`, and optionally "
                "`gdrive_service_account_user_email` in DVC config."
            )

        # Validate OAuth 2.0 Client ID configuration
        if not use_service_account:
            if bool(client_id) != bool(client_secret):
                raise ConfigError(
                    "Please specify GDrive's client ID and secret in "
                    "DVC config or omit both to use the defaults."
                )

        self._settings = {
            "trash_only": config.get("gdrive_trash_only"),
            "acknowledge_abuse": config.get("gdrive_acknowledge_abuse"),
            "use_service_account": use_service_account,
            "client_user_email": client_user_email,
            "client_json_file_path": client_json_file_path,
            "client_json": env_creds,
            "client_id": client_id,
            "client_secret": client_secret,
            "profile": profile,
        }

    @classmethod
    def _strip_protocol(cls, path):
        from fsspec.utils import infer_storage_options

        opts = infer_storage_options(path)

        return "{host}{path}".format(**opts)

    def unstrip_protocol(self, path):
        return f"gdrive://{path}"

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        return {"url": urlpath}

    @wrap_prop(threading.RLock())
    @cached_property
    def fs(self):
        from pydrive2.fs import GDriveFileSystem as _GDriveFileSystem

        return _GDriveFileSystem(self._path, **self._settings)

    def upload_fobj(self, fobj, to_info, **kwargs):
        self.makedirs(os.path.dirname(to_info))
        return self.fs.upload_fobj(fobj, to_info, **kwargs)
