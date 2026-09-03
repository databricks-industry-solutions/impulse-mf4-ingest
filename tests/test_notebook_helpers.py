import pytest

from utils.notebook_helpers import is_uc_volume_path, resolve_volume_path


@pytest.mark.parametrize(
    "volume_or_path,expected",
    [
        ("mdf4", "/Volumes/mda_demo/default/mdf4"),
        ("/Volumes/cat/sch/vol", "/Volumes/cat/sch/vol"),
        ("/Volumes/cat/sch/vol/", "/Volumes/cat/sch/vol"),
        ("dbfs:/Volumes/cat/sch/vol", "/Volumes/cat/sch/vol"),
        ("/dbfs/Volumes/cat/sch/vol", "/Volumes/cat/sch/vol"),
    ],
)
def test_resolve_volume_path(volume_or_path, expected):
    assert resolve_volume_path("mda_demo", "default", volume_or_path) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("mdf4", False),
        ("/Volumes/cat/sch/vol", True),
        ("dbfs:/Volumes/cat/sch/vol", True),
    ],
)
def test_is_uc_volume_path(value, expected):
    assert is_uc_volume_path(value) is expected
