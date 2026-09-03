from datetime import datetime

import pandas as pd
import pytest

from utils.demo_conversion import (
    _copy_to_local,
    _get_obd_signals,
    _local_csv_path,
    _volume_fuse_path,
    resolve_demo_data_source,
)


def test_get_obd_signals_relative_timestamps(tmp_path):
    csv_path = tmp_path / "segment.csv"
    pd.DataFrame(
        {
            "Time": ["00:00:00.000", "00:00:01.000", "00:00:02.000"],
            "Engine RPM": [800.0, 1200.0, 1500.0],
        }
    ).to_csv(csv_path, index=False)

    relative_master, signals, start_time = _get_obd_signals(str(csv_path), "2017-06-15")

    assert relative_master == [0.0, 1.0, 2.0]
    assert signals["Engine RPM"] == [800.0, 1200.0, 1500.0]
    assert start_time == datetime(2017, 6, 15, 0, 0, 0)


def test_resolve_demo_data_source_prefers_existing_file(tmp_path):
    dataset_dir = tmp_path / "obd_dataset"
    dataset_dir.mkdir()
    resolved = resolve_demo_data_source(str(dataset_dir))
    assert resolved == str(dataset_dir)


def test_resolve_demo_data_source_zip_sibling(tmp_path):
    zip_path = tmp_path / "obd_dataset.zip"
    zip_path.write_text("placeholder")
    resolved = resolve_demo_data_source(str(tmp_path / "obd_dataset"))
    assert resolved == str(zip_path)


def test_resolve_demo_data_source_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="OBD demo data not found"):
        resolve_demo_data_source(str(tmp_path / "missing"))


def test_volume_fuse_path_normalizes_dbfs_uri():
    assert _volume_fuse_path("dbfs:/Volumes/cat/sch/vol") == "/Volumes/cat/sch/vol"
    assert _volume_fuse_path("/dbfs/Volumes/cat/sch/vol") == "/Volumes/cat/sch/vol"


def test_local_csv_path_reads_local_file_without_copy(tmp_path):
    csv_path = tmp_path / "segment.csv"
    csv_path.write_text("Time,Engine RPM\n00:00:00.000,800\n")
    assert _local_csv_path(str(csv_path), str(tmp_path / "cache")) == str(csv_path)


def test_copy_to_local(tmp_path):
    src = tmp_path / "src.csv"
    src.write_text("a,b\n1,2\n")
    dest = tmp_path / "cache" / "dest.csv"
    _copy_to_local(str(src), str(dest))
    assert dest.read_text() == "a,b\n1,2\n"
