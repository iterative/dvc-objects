import pytest

from dvc_objects.fs.base import FileSystem
from dvc_objects.fs.implementations import (
    azure,
    gs,
    hdfs,
    http,
    https,
    oss,
    s3,
    ssh,
    webhdfs,
)


@pytest.fixture(autouse=True)
def mock_check_requires(mocker):
    yield mocker.patch.object(FileSystem, "_check_requires")


@pytest.mark.parametrize(
    "fs_cls, urlpath, stripped",
    [
        (azure.AzureFileSystem, "azure://container", "container"),
        (gs.GSFileSystem, "gs://container", "container"),
        (s3.S3FileSystem, "s3://container", "container"),
        (oss.OSSFileSystem, "oss://container", "container"),
        (hdfs.HDFSFileSystem, "hdfs://example.com", ""),
        (hdfs.HDFSFileSystem, "hdfs://example.com:8020", ""),
        (
            http.HTTPFileSystem,
            "http://example.com/path/to/file",
            "http://example.com/path/to/file",
        ),
        (
            https.HTTPSFileSystem,
            "https://example.com/path/to/file",
            "https://example.com/path/to/file",
        ),
        (ssh.SSHFileSystem, "ssh://example.com:/dir/path", "/dir/path"),
        (webhdfs.WebHDFSFileSystem, "webhdfs://example.com", ""),
        (webhdfs.WebHDFSFileSystem, "webhdfs://example.com:8020", ""),
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
    fs = hdfs.HDFSFileSystem(**fs_args)
    assert fs.unstrip_protocol("/path") == expected_url + "/path"


@pytest.mark.parametrize(
    "fs_cls, path, expected_url",
    [
        (azure.AzureFileSystem, "container", "azure://container"),
        (azure.AzureFileSystem, "container/path", "azure://container/path"),
        (gs.GSFileSystem, "container", "gs://container"),
        (gs.GSFileSystem, "container/path", "gs://container/path"),
        (s3.S3FileSystem, "container", "s3://container"),
        (s3.S3FileSystem, "container/path", "s3://container/path"),
        (oss.OSSFileSystem, "container", "oss://container"),
        (oss.OSSFileSystem, "container/path", "oss://container/path"),
    ],
)
def test_unstrip_protocol(mocker, fs_cls, path, expected_url):
    assert fs_cls.unstrip_protocol(mocker.MagicMock(), path) == expected_url
