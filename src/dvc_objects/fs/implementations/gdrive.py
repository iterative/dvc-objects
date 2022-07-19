import logging
import os
import tempfile
import threading

from funcy import cached_property, wrap_prop

from ..base import FileSystem
from ..errors import AuthError, ConfigError

logger = logging.getLogger(__name__)
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GDriveAuthError(AuthError):
    def __init__(self, cred_location):

        if cred_location:
            message = (
                "GDrive remote auth failed with credentials in '{}'.\n"
                "Backup first, remove or fix them, and run again.\n"
                "It should do auth again and refresh the credentials.\n\n"
                "Details:".format(cred_location)
            )
        else:
            message = "Failed to authenticate GDrive remote"

        super().__init__(message)


class GDriveFileSystem(FileSystem):  # pylint:disable=abstract-method
    protocol = "gdrive"
    PARAM_CHECKSUM = "checksum"
    REQUIRES = {"pydrive2": "pydrive2"}
    # Always prefer traverse for GDrive since API usage quotas are a concern.
    TRAVERSE_WEIGHT_MULTIPLIER = 1

    GDRIVE_CREDENTIALS_DATA = "GDRIVE_CREDENTIALS_DATA"
    DEFAULT_USER_CREDENTIALS_FILE = "gdrive-user-credentials.json"

    DEFAULT_GDRIVE_CLIENT_ID = "710796635688-iivsgbgsb6uv1fap6635dhvuei09o66c.apps.googleusercontent.com"  # noqa: E501
    DEFAULT_GDRIVE_CLIENT_SECRET = "a1Fz59uTpVNeG_VGuSKDLJXv"

    def __init__(self, **config):
        from fsspec.utils import infer_storage_options

        super().__init__(**config)

        self.url = config["url"]
        opts = infer_storage_options(self.url)

        if not opts["host"]:
            raise ConfigError("Empty GDrive URL '{}'.".format(config["url"]))

        self._bucket = opts["host"]
        self._path = opts["path"].lstrip("/")
        self._trash_only = config.get("gdrive_trash_only")
        self._use_service_account = config.get("gdrive_use_service_account")
        self._service_account_user_email = config.get(
            "gdrive_service_account_user_email"
        )
        self._service_account_json_file_path = config.get(
            "gdrive_service_account_json_file_path"
        )
        self._client_id = config.get("gdrive_client_id")
        self._client_secret = config.get("gdrive_client_secret")
        self._validate_config()

        tmp_dir = config["gdrive_credentials_tmp_dir"]
        if not tmp_dir:
            tmp_dir = tempfile.mkdtemp("dvc-gdrivefs")

        self._gdrive_user_credentials_path = config.get(
            "gdrive_user_credentials_file",
            os.path.join(tmp_dir, self.DEFAULT_USER_CREDENTIALS_FILE),
        )

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

    def _validate_config(self):
        # Validate Service Account configuration
        if (
            self._use_service_account
            and not self._service_account_json_file_path
            and GDriveFileSystem.GDRIVE_CREDENTIALS_DATA not in os.environ
        ):
            raise ConfigError(
                "To use service account, set "
                "`gdrive_service_account_json_file_path`, and optionally"
                "`gdrive_service_account_user_email` in DVC config."
            )

        # Validate OAuth 2.0 Client ID configuration
        if not self._use_service_account:
            if bool(self._client_id) != bool(self._client_secret):
                raise ConfigError(
                    "Please specify GDrive's client ID and secret in "
                    "DVC config or omit both to use the defaults."
                )

    @cached_property
    def credentials_location(self):
        """
        Helper to determine where will GDrive remote read credentials from.
        Useful for tests, exception messages, etc. Returns either env variable
        name if it's set or actual path to the credentials file.
        """
        if os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA):
            return GDriveFileSystem.GDRIVE_CREDENTIALS_DATA
        if os.path.exists(self._gdrive_user_credentials_path):
            return self._gdrive_user_credentials_path
        return None

    @staticmethod
    def _validate_credentials(auth, settings):
        """
        Detects discrepancy in DVC config and cached credentials file.
        Usually happens when a second remote is added and it is using
        the same credentials default file. Or when someones decides to change
        DVC config client id or secret but forgets to remove the cached
        credentials file.
        """
        if not os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA):
            if (
                settings["client_config"]["client_id"]
                != auth.credentials.client_id
                or settings["client_config"]["client_secret"]
                != auth.credentials.client_secret
            ):
                logger.warning(
                    "Client ID and secret configured do not match the "
                    "actual ones used\nto access the remote. Do you "
                    "use multiple GDrive remotes and forgot to\nset "
                    "`gdrive_user_credentials_file` for one or more of them?"
                )

    @wrap_prop(threading.RLock())
    @cached_property
    def fs(self):
        from pydrive2.auth import GoogleAuth
        from pydrive2.fs import GDriveFileSystem as _GDriveFileSystem

        auth_settings = {
            "save_credentials": True,
            "get_refresh_token": True,
            "oauth_scope": [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.appdata",
            ],
        }

        env_creds = os.getenv(self.GDRIVE_CREDENTIALS_DATA)
        if env_creds:
            auth_settings["save_credentials_backend"] = "dictionary"
            auth_settings["save_credentials_dict"] = {"creds": env_creds}
            auth_settings["save_credentials_key"] = "creds"
        else:
            auth_settings["save_credentials_backend"] = "file"
            auth_settings[
                "save_credentials_file"
            ] = self._gdrive_user_credentials_path

        if self._use_service_account:
            service_config = {
                "client_user_email": self._service_account_user_email,
            }

            if env_creds:
                service_config["client_json"] = env_creds
            else:
                service_config[
                    "client_json_file_path"
                ] = self._service_account_json_file_path

            auth_settings["client_config_backend"] = "service"
            auth_settings["service_config"] = service_config
        else:
            auth_settings["client_config_backend"] = "settings"
            auth_settings["client_config"] = {
                "client_id": self._client_id or self.DEFAULT_GDRIVE_CLIENT_ID,
                "client_secret": self._client_secret
                or self.DEFAULT_GDRIVE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "revoke_uri": "https://oauth2.googleapis.com/revoke",
                "redirect_uri": "",
            }

        gauth = GoogleAuth(settings=auth_settings)

        try:
            logger.debug("GDrive remote auth with config '%s'.", auth_settings)
            if self._use_service_account:
                gauth.ServiceAuth()
            else:
                gauth.LocalWebserverAuth()
                GDriveFileSystem._validate_credentials(gauth, auth_settings)

        # Handle AuthenticationError, RefreshError and other auth failures
        # It's hard to come up with a narrow exception, since PyDrive throws
        # a lot of different errors - broken credentials file, refresh token
        # expired, flow failed, etc.
        except Exception as exc:
            raise GDriveAuthError(self.credentials_location) from exc

        return _GDriveFileSystem(
            f"{self._bucket}/{self._path}",
            gauth,
            trash_only=self._trash_only,
        )

    def upload_fobj(self, fobj, to_info, **kwargs):
        self.makedirs(os.path.dirname(to_info))
        return self.fs.upload_fobj(fobj, to_info, **kwargs)
