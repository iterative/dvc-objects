import dvc_azure
import dvc_gs
import dvc_hdfs
import dvc_http
import dvc_oss
import dvc_s3
import dvc_ssh
import dvc_webhdfs
import pytest

from dvc_objects.fs.base import FileSystem


@pytest.fixture(autouse=True)
def mock_check_requires(mocker):
    yield mocker.patch.object(FileSystem, "_check_requires")


@pytest.mark.parametrize(
    "fs_cls, urlpath, stripped",
    [
        (dvc_azure.AzureFileSystem, "azure://container", "container"),
        (dvc_gs.GSFileSystem, "gs://container", "container"),
        (dvc_s3.S3FileSystem, "s3://container", "container"),
        (dvc_oss.OSSFileSystem, "oss://container", "container"),
        (dvc_hdfs.HDFSFileSystem, "hdfs://example.com", ""),
        (dvc_hdfs.HDFSFileSystem, "hdfs://example.com:8020", ""),
        (
            dvc_http.HTTPFileSystem,
            "http://example.com/path/to/file",
            "http://example.com/path/to/file",
        ),
        (
            dvc_http.HTTPSFileSystem,
            "https://example.com/path/to/file",
            "https://example.com/path/to/file",
        ),
        (dvc_ssh.SSHFileSystem, "ssh://example.com:/dir/path", "/dir/path"),
        (dvc_webhdfs.WebHDFSFileSystem, "webhdfs://example.com", ""),
        (dvc_webhdfs.WebHDFSFileSystem, "webhdfs://example.com:8020", ""),
    ],
)
@pytest.mark.parametrize("path", ["", "/path"])
def test_strip_prptocol(fs_cls, urlpath, stripped, path):
    assert fs_cls._strip_protocol(urlpath + path) == stripped + path


@pytest.mark.parametrize(
    "fs_args, expected_url",
    [
        ({"host": "example.com"}, "hdfs://example.com"),
        ({"host": "example.com", "port": None}, "hdfs://example.com"),
        ({"host": "example.com", "port": 8020}, "hdfs://example.com:8020"),
    ],
)
def test_hdfs_unstrip_protocol(fs_args, expected_url):
    fs = dvc_hdfs.HDFSFileSystem(**fs_args)
    assert fs.unstrip_protocol("/path") == expected_url + "/path"


@pytest.mark.parametrize(
    "fs_cls, path, expected_url",
    [
        (dvc_azure.AzureFileSystem, "container", "azure://container"),
        (
            dvc_azure.AzureFileSystem,
            "container/path",
            "azure://container/path",
        ),
        (dvc_gs.GSFileSystem, "container", "gs://container"),
        (dvc_gs.GSFileSystem, "container/path", "gs://container/path"),
        (dvc_s3.S3FileSystem, "container", "s3://container"),
        (dvc_s3.S3FileSystem, "container/path", "s3://container/path"),
        (dvc_oss.OSSFileSystem, "container", "oss://container"),
        (dvc_oss.OSSFileSystem, "container/path", "oss://container/path"),
    ],
)
def test_unstrip_protocol(mocker, fs_cls, path, expected_url):
    assert fs_cls.unstrip_protocol(mocker.MagicMock(), path) == expected_url
