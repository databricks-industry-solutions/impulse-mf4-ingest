"""OBD CSV to MDF4 demo conversion (requires asammdf at runtime)."""

import os
import re
import shutil
import zipfile
from datetime import datetime
from typing import Dict, List, Tuple

DEMO_DATASET_BASENAME = "obd_dataset"


def _volume_fuse_path(path: str) -> str:
    """Normalize UC Volume paths to ``/Volumes/...`` for driver FUSE I/O."""
    if path.startswith("dbfs:/Volumes/"):
        return path[len("dbfs:"):]
    if path.startswith("/dbfs/Volumes/"):
        return path[len("/dbfs"):]
    return path


def _is_volume_path(path: str) -> bool:
    return path.startswith(("/Volumes/", "dbfs:/Volumes/", "/dbfs/Volumes/"))


def _copy_to_local(src: str, local_path: str) -> str:
    """Copy a file to local disk without ``dbutils.fs`` ``file:`` URIs (serverless-safe)."""
    src = _volume_fuse_path(src)
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    shutil.copyfile(src, local_path)
    return local_path


def _unzip_archive(zip_path, extract_to):
    """Extract all contents of a zip archive to a target directory."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)


def _resolve_demo_zip_to_local(input_path: str, local_tmp_dir: str, dbutils=None) -> str:
    """Resolve a workspace or UC Volume demo zip path to a local-readable file path."""
    fuse_path = _volume_fuse_path(input_path)
    if os.path.isfile(fuse_path):
        return fuse_path
    raise FileNotFoundError(f"Demo data not found at {input_path}")


def _list_dir_names(path: str, dbutils=None) -> list:
    """List entry names via the UC Volume FUSE mount (``/Volumes/...``)."""
    fuse_path = _volume_fuse_path(path)
    if not os.path.isdir(fuse_path):
        return []
    return os.listdir(fuse_path)


def _path_is_dir(path: str, dbutils=None) -> bool:
    return os.path.isdir(_volume_fuse_path(path))


def _experiment_dirs(dataset_root: str, dbutils=None) -> list:
    """Return immediate child directories under a staged OBD dataset root."""
    fuse_root = _volume_fuse_path(dataset_root)
    if not os.path.isdir(fuse_root):
        return []
    return [
        os.path.join(fuse_root, name)
        for name in os.listdir(fuse_root)
        if not name.startswith(".") and os.path.isdir(os.path.join(fuse_root, name))
    ]


def _resolve_dataset_root(input_path: str, dbutils=None) -> str:
    """Find the directory that contains experiment_* folders."""
    fuse_input = _volume_fuse_path(input_path)
    candidates = [fuse_input.rstrip("/")]
    parent = os.path.dirname(fuse_input.rstrip("/"))
    if parent and parent not in candidates:
        candidates.append(parent)
    for candidate in candidates:
        if _experiment_dirs(candidate):
            return candidate
    return fuse_input


def _experiment_has_mf4(output_experiment_path: str, dbutils=None) -> bool:
    return any(
        name.endswith(".mf4")
        for name in _list_dir_names(output_experiment_path)
    )


def _local_csv_path(csv_path: str, local_tmp_dir: str, dbutils=None) -> str:
    """Return a path pandas can read (FUSE-first; avoids dbutils ``file:`` copies)."""
    fuse_path = _volume_fuse_path(csv_path)
    if not fuse_path.startswith("/Volumes/"):
        return fuse_path
    if os.path.isfile(fuse_path):
        return fuse_path
    os.makedirs(local_tmp_dir, exist_ok=True)
    local_path = os.path.join(local_tmp_dir, os.path.basename(fuse_path))
    return _copy_to_local(fuse_path, local_path)


def _clear_mdf_outputs(output_root: str, dbutils=None) -> None:
    """Remove prior MF4 experiment folders from the volume root (keeps ``_seed``)."""
    fuse_root = _volume_fuse_path(output_root)
    for name in _list_dir_names(fuse_root):
        if name in ("_seed", ".", ".."):
            continue
        path = f"{fuse_root.rstrip('/')}/{name}"
        if not _path_is_dir(path):
            continue
        shutil.rmtree(path, ignore_errors=True)


def resolve_demo_data_source(source_path: str, notebook_dir: str | None = None) -> str:
    """Resolve the OBD demo source to an existing workspace file or directory.

    The bundle ships ``examples/obd_dataset.zip``; workspace import may also
    auto-extract a sibling ``obd_dataset/`` directory. Relative widget defaults
    are resolved against the notebook's workspace location.
    """
    candidates = []

    def add(path: str) -> None:
        if path and path not in candidates:
            candidates.append(path)

    add(source_path)
    if source_path.endswith(".zip"):
        add(source_path[:-4])
    else:
        add(f"{source_path}.zip")
        parent = os.path.dirname(source_path.rstrip("/"))
        add(os.path.join(parent, f"{DEMO_DATASET_BASENAME}.zip"))
        add(os.path.join(parent, DEMO_DATASET_BASENAME))

    if notebook_dir:
        examples_root = os.path.normpath(f"/Workspace{notebook_dir}/../../examples")
        add(os.path.join(examples_root, DEMO_DATASET_BASENAME))
        add(os.path.join(examples_root, f"{DEMO_DATASET_BASENAME}.zip"))

    for path in candidates:
        if os.path.isfile(path) or os.path.isdir(path):
            return path

    raise FileNotFoundError(
        "OBD demo data not found. Tried: " + ", ".join(candidates)
    )


def _create_builtin_demo_zip(local_tmp_dir: str) -> str:
    """Create a tiny built-in demo dataset zip as a fallback."""
    os.makedirs(local_tmp_dir, exist_ok=True)
    local_zip_path = os.path.join(local_tmp_dir, "obd_dataset.zip")
    csv_rel_path = "obd_dataset/experiment_1/2024-01-01_demo_vehicle_route_dry_segment.csv"
    csv_data = (
        "Time,vehicle_speed [km/h],engine_rpm [rpm],coolant_temp [C]\n"
        "00:00:00.000,0,800,25\n"
        "00:00:01.000,8,1200,26\n"
        "00:00:02.000,16,1500,27\n"
        "00:00:03.000,22,1800,28\n"
        "00:00:04.000,30,2100,29\n"
    )
    with zipfile.ZipFile(local_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_rel_path, csv_data)
    return local_zip_path


def prepare_demo_data(
    source_path: str,
    target_root: str,
    dbutils=None,
    notebook_dir: str | None = None,
) -> str:
    """Stage the OBD demo dataset onto a UC Volume as an extracted directory tree.

    Always materializes ``<target_root>/obd_dataset/experiment_*/`` so downstream
    conversion can list experiment folders. Reuses an existing staged copy only
    when it already contains experiment subdirectories.

    Args:
        source_path (str): Workspace path of the demo source (``.zip`` or directory).
        target_root (str): Destination on a UC Volume, e.g.
            ``/Volumes/<catalog>/<schema>/<volume>/_seed``.
        dbutils: Retained for API compatibility; volume I/O uses ``/Volumes/...`` FUSE only.
        notebook_dir (str | None): Notebook workspace directory for resolving
            relative paths such as ``../../examples/obd_dataset``.

    Returns:
        str: Volume path to the staged ``obd_dataset`` directory.

    Raises:
        FileNotFoundError: When the source cannot be resolved or staging is empty.
    """
    resolved = resolve_demo_data_source(source_path, notebook_dir)
    target_root = _volume_fuse_path(target_root)
    target = os.path.join(target_root, DEMO_DATASET_BASENAME)

    if _experiment_dirs(target):
        return target

    if os.path.exists(target):
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            os.remove(target)

    os.makedirs(target_root, exist_ok=True)
    local_tmp = os.path.join("/tmp", "obd_demo_seed")

    try:
        if os.path.isfile(resolved) and resolved.endswith(".zip"):
            if os.path.isdir(local_tmp):
                shutil.rmtree(local_tmp)
            os.makedirs(local_tmp, exist_ok=True)
            zip_local = resolved
            if _is_volume_path(resolved):
                zip_local = os.path.join(local_tmp, f"{DEMO_DATASET_BASENAME}.zip")
                _copy_to_local(resolved, zip_local)
            _unzip_archive(zip_local, local_tmp)
            extracted = os.path.join(local_tmp, DEMO_DATASET_BASENAME)
            src_dir = extracted if os.path.isdir(extracted) else local_tmp
            shutil.copytree(src_dir, target)
        elif os.path.isdir(resolved):
            shutil.copytree(resolved, target)
        else:
            raise FileNotFoundError(f"Demo source not found: {source_path}")
    finally:
        shutil.rmtree(local_tmp, ignore_errors=True)

    if not _experiment_dirs(target):
        raise FileNotFoundError(
            f"Staged demo data at {target} has no experiment folders. "
            f"Source was: {resolved}"
        )
    return target


def _get_obd_signals(file_path, date) -> Tuple[List[float], Dict[str, List[float]], datetime]:
    """Read an OBD CSV file and construct signal arrays compatible with ASAM MDF.

    The function expects a CSV file with a ``Time`` column and multiple signal columns.
    It builds a relative master time axis (seconds from the first sample) and the
    measurement start time for the MDF header.

    Args:
        file_path (str): Path to the CSV file.
        date (str): Date string (YYYY-MM-DD) used when building the timestamp.

    Returns:
        Tuple of relative master timestamps (seconds from t0), signal values, and
        the absolute measurement start time for ``mdf.header.start_time``.
    """
    import pandas as pd

    pdf = pd.read_csv(file_path)
    pdf['date'] = date
    master_ts = pd.to_datetime(pdf['date'].astype(str) + ' ' + pdf['Time'].astype(str))
    pdf = pdf.dropna(subset=['Time'])
    start_time = master_ts.iloc[0].to_pydatetime()
    relative_master = (
        (master_ts - master_ts.iloc[0]) / pd.Timedelta(seconds=1)
    ).astype(float).tolist()
    signals = {
        col: list(pdf[col].astype(float))
        for col in pdf.drop(["Time", "date"], axis=1).columns
    }
    return relative_master, signals, start_time


def convert_obd_data(input_path: str, local_tmp_dir: str, output_dir: str,
                          dbutils, force_convert: bool = False):
    """Convert and package OBD CSV demo data into MDF (ASAM MDF v4) files.

    Serverless-safe: MDF files are written to ``local_tmp_dir`` (regular Python
    I/O, permitted on serverless) and then published to the UC Volume at
    ``output_dir`` via ``shutil.copytree``. UC Volume reads use the driver FUSE
    mount (``/Volumes/...``) only — never ``dbfs:/Volumes/...`` or ``dbutils.fs``.

    Args:
        input_path (str): Path to the input OBD dataset or zip archive. May be a
            workspace file (``/Workspace/...``), a UC Volume file, or a local path.
        local_tmp_dir (str): Ephemeral local directory used for zip extraction
            and MDF staging.
        output_dir (str): UC Volume root for MDF outputs
            (e.g. ``/Volumes/<catalog>/<schema>/<volume>``).
        dbutils: Retained for API compatibility; unused for volume I/O.
        force_convert (bool): When True, remove existing MF4 experiment folders
            under ``output_dir`` before converting.

    Returns:
        None

    Raises:
        RuntimeError: When no MF4 files are produced and nothing was skipped.
    """
    from asammdf import MDF, Signal

    input_path = _volume_fuse_path(input_path)
    output_root = _volume_fuse_path(output_dir).rstrip("/")
    if not output_root.startswith("/Volumes/"):
        raise ValueError(f"output_dir must be a UC Volume path under /Volumes/, got: {output_dir}")
    os.makedirs(output_root, exist_ok=True)

    if force_convert:
        _clear_mdf_outputs(output_root)

    os.makedirs(local_tmp_dir, exist_ok=True)
    csv_local_dir = os.path.join(local_tmp_dir, "csv_inputs")
    os.makedirs(csv_local_dir, exist_ok=True)
    staging_root = os.path.join(local_tmp_dir, "obd_dataset_stage")
    if os.path.isdir(staging_root):
        shutil.rmtree(staging_root)
    os.makedirs(staging_root, exist_ok=True)

    # Databricks workspace import auto-extracts .zip files on upload. If the path
    # points at an auto-extracted zip (file gone, sibling directory present), use the
    # directory directly instead of failing with FileNotFoundError.
    if input_path.endswith(".zip") and not os.path.isfile(input_path):
        extracted_dir = input_path[:-4]
        if os.path.isdir(extracted_dir):
            print(f"Note: {input_path} not found; using auto-extracted directory {extracted_dir}.")
            input_path = extracted_dir

    if os.path.isdir(input_path) or _path_is_dir(input_path):
        dataset_root = _resolve_dataset_root(input_path)
    else:
        try:
            local_zip_path = _resolve_demo_zip_to_local(input_path, local_tmp_dir)
        except FileNotFoundError:
            print(f"Warning: demo_data_path not found ({input_path}). Using built-in fallback demo dataset.")
            local_zip_path = _create_builtin_demo_zip(local_tmp_dir)
        _unzip_archive(local_zip_path, staging_root)
        candidate = os.path.join(staging_root, "obd_dataset")
        dataset_root = candidate if os.path.isdir(candidate) else staging_root
        dataset_root = _resolve_dataset_root(dataset_root)

    listing = _experiment_dirs(dataset_root)
    if not listing:
        raise FileNotFoundError(f"No experiment folders found under demo dataset root: {dataset_root}")

    unit_pattern = r'\[([^\]]+)\]'
    converted_files = 0
    failed_files = 0
    skipped_experiments = 0

    for sim_data_path in listing:
        experiment_id = os.path.basename(sim_data_path.rstrip("/"))
        output_experiment_path = f"{output_root}/{experiment_id}"
        if _experiment_has_mf4(output_experiment_path):
            print(f"Skipping {experiment_id}: MF4 files already exist at {output_experiment_path}")
            skipped_experiments += 1
            continue

        # Stage MDF writes locally; asammdf does seek/back-patch which is unreliable
        # over the UC Volume FUSE mount. Publish atomically via shutil.copytree.
        local_experiment_path = os.path.join(local_tmp_dir, experiment_id)
        if os.path.isdir(local_experiment_path):
            shutil.rmtree(local_experiment_path)
        os.makedirs(local_experiment_path, exist_ok=True)

        mdf_file_written = False
        csv_files = [
            name for name in _list_dir_names(sim_data_path)
            if name.endswith(".csv")
        ]
        print(f"Converting {len(csv_files)} CSV file(s) for {experiment_id} from {sim_data_path}")
        for csv_file_path in csv_files:
            try:
                file_path = f"{sim_data_path.rstrip('/')}/{csv_file_path}"
                file_name = csv_file_path.replace(".csv", "")
                tmp_mdf_path = os.path.join(local_experiment_path, f"{file_name}.mf4")
                (date, brand, model, from_city, to_city, condition) = file_name.split("_")[:6]
                local_csv = _local_csv_path(file_path, csv_local_dir)
                master, signals, start_time = _get_obd_signals(local_csv, date)
                data_groups = _create_data_groups(signals)
                comment = f"{{'brand': '{brand}', 'model': '{model}', 'vehicle_key': '{brand}_{model}', 'from_city': '{from_city}', 'to_city': '{to_city}', 'condition': '{condition}', 'experiment_id': '{experiment_id}'}}"
                mdf = MDF(version='4.10')
                mdf.header.start_time = start_time
                mdf.header.comment = comment
                for data_group in data_groups:
                    current_mdf_signals = []
                    for k, v in data_group.items():
                        name = k[:k.find(' [')]
                        unit = re.findall(unit_pattern, k)[-1]
                        if unit in ['Â°C', '°C']:
                            unit = 'C'
                        sig = Signal(samples=v,
                                        timestamps=master,
                                        name=name,
                                        unit=unit,
                                        comment=comment
                                        )
                        current_mdf_signals.append(sig)
                    mdf.append(current_mdf_signals)
                mdf.save(tmp_mdf_path, overwrite=True)
                mdf_file_written = True
                converted_files += 1
            except Exception as e:
                failed_files += 1
                print(f"Failed to convert {csv_file_path} in {experiment_id}: {e}")
                continue
        if mdf_file_written:
            shutil.copytree(local_experiment_path, output_experiment_path, dirs_exist_ok=True)
            print(f"Published MF4 files to {output_experiment_path}")
        else:
            print(f"No MF4 files produced for {experiment_id}")
        shutil.rmtree(local_experiment_path, ignore_errors=True)

    shutil.rmtree(staging_root, ignore_errors=True)
    shutil.rmtree(csv_local_dir, ignore_errors=True)

    print(
        f"OBD→MF4 conversion complete: {converted_files} file(s) written, "
        f"{failed_files} failure(s), {skipped_experiments} experiment(s) skipped"
    )
    if converted_files == 0 and skipped_experiments < len(listing):
        raise RuntimeError(
            "No MF4 files were produced. Check cluster logs for per-file errors "
            "(common causes: missing asammdf on the cluster, or unreadable CSV paths on UC Volumes)."
        )


def _create_data_groups(signals: dict) -> list:
    """Split signals into a deterministic number of MDF channel groups."""
    group_count = max(1, min(8, len(signals) // 10 + 1))
    data_groups = [{} for _ in range(group_count)]
    for i, key in enumerate(signals):
        data_groups[i % group_count][key] = signals[key]
    return data_groups

