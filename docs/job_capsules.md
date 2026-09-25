# Job capsules

A job capsule is one run of one pipeline over one asset with one parameter set. It is a directory uploaded to the job capsules Dandiset (`001697`), and it holds everything needed to run, inspect and account for that run.

## Where a capsule lives

```text
derivatives/
├── jobs.tsv                           # one row per capsule (see Data model)
├── jobs.json                          # BIDS-style sidecar describing the jobs.tsv columns
├── paths.tsv                          # one row per asset path of each capsule
├── paths.json                         # BIDS-style sidecar describing the paths.tsv columns
├── issues_dump.json
├── issues_summary.json
└── dandisets-{first 3 digits}/
    └── dandiset-{dandiset_id}/
        └── {path of the asset, without .nwb}/
            └── pipeline-{pipeline}/
                └── job-{YYMMDD}{hash}/        # the capsule
                    ├── dataset_description.json
                    ├── code/
                    │   ├── submit.sh
                    │   ├── {parameters}.json
                    │   ├── submitted_date-YYYY+MM+DD_time-HH+MM+SS   # added when claimed
                    │   └── ...                                       # pipeline specific
                    ├── logs/
                    └── derivatives/                                  # added on success
```

For example, the capsule for `sub-mouse01/sub-mouse01_ecephys.nwb` in Dandiset `000409` might be:

```text
derivatives/dandisets-000/dandiset-000409/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/job-260916a1b2c3/
```

The `dandisets-{first 3 digits}` level keeps any one directory from holding thousands of Dandisets.

### What each pipeline puts in a capsule

| Path | `aind+ephys` | `lfp` |
|---|---|---|
| `code/submit.sh` | Nextflow driver script | `datalad containers-run` script |
| `code/{parameters}.json` | The registered AIND parameter file | The registered LFP parameter file |
| `code/{config}.config` | The registered Nextflow config | Not used |
| `code/main_multi_backend.nf` | Copy of the pipeline entry point | Not used |
| `code/capsule_versions.env` | Copy of the pipeline's pinned capsule versions | Not used |
| `logs/` | `nextflow.log`, `job-{id}_slurm.log`, Nextflow reports such as `timeline.html` | `duct_*` resource usage, `job-{id}_slurm.log` |
| outputs | `derivatives/nwb/`, `derivatives/visualization/`, `derivatives/postprocessed/` | `derivatives/nwb/{asset}_desc-lfp` |

## The job ID

The directory name is the job ID, `job-{YYMMDD}{hash}`.

- `YYMMDD` is the UTC date the capsule was prepared. It makes the name readable and separates re-attempts of one job across days.
- `hash` is the first six hex characters of an MD5 over the fields that identify the job. Those are the Dandiset ID, the asset's path and content ID, the pipeline and its version, and the parameters and config IDs.

For example:

| Job ID | What it is |
|---|---|
| `job-260916a1b2c3` | A job first prepared on 16 September 2026 |
| `job-260920a1b2c3` | The same job prepared again on 20 September, after the first capsule was archived |
| `job-260916a1b2c3-2` | The same job prepared a second time on 16 September |
| `job-2609167f04d9` | A different job prepared the same day, for example the same asset with other parameters |

The codebase version is left out on purpose. A job is the same logical job whichever release of this package formed it. That is how the queue decides a capsule already exists and must not be formed again.

Parameters and configs enter the hash by the MD5 of their contents rather than by their registry key. Two keys pointing at the same file therefore form the same job, and editing a file (and its registered checksum) forms a new one.

Two capsules can still share an ID when the same job is formed twice on the same day, for example by `jobs create --latest`. The second carries a `-2` suffix, the third `-3`, and so on. The queue reads through the suffix, so every spelling is recognised as the same job.

This job ID is the only capsule layout the package understands. Capsules prepared before it existed carry older names, and the queue does not see them until they are migrated.

## Provenance

The job ID alone does not say what was run. That is recorded in the capsule's `dataset_description.json`, under a `DandiCompute` key next to the standard BIDS fields.

```json
{
  "Name": "DANDI Compute: AIND Ephys pipeline output for Dandiset 000409",
  "BIDSVersion": "1.10",
  "DatasetType": "study",
  "GeneratedBy": [
    {"Name": "AIND Ephys Pipeline", "Version": "v1.2.4+<pipeline commit>", "CodeURL": "..."},
    {"Name": "DANDI Compute: Code", "Version": "v0.8.2+<codebase commit>", "CodeURL": "..."}
  ],
  "SourceDatasets": [{"URL": "https://dandiarchive.org/dandiset/000409/"}],
  "DandiCompute": {
    "job_id": "job-260916a1b2c3",
    "dandiset_id": "000409",
    "within_dandiset_path": "sub-mouse01/sub-mouse01_ecephys.nwb",
    "content_id": "048d1ee9-83b7-491f-8f02-1ca615b1d455",
    "pipeline": "aind+ephys",
    "version": "v1.2.4",
    "codebase": "v0.8.2",
    "params": "4e89ec7",
    "config": "7940dfd",
    "params_key": "default",
    "config_key": "default"
  }
}
```

`jobs refresh` reads this block back for every capsule to build `jobs.tsv`. Pipeline versions are written "BIDS-safe", with `-` replaced by `+`, so `v1.0.0-fixes` is recorded as `v1.0.0+fixes`.

## Lifecycle

A capsule's status is not stored anywhere. It is derived each time the queue is read, from which of its files exist on the archive. The checks run from the furthest point in the lifecycle backwards, so every capsule lands on exactly one status.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> pending: prepared and uploaded<br/>(code/ exists)
    pending --> stalled: array task claims it<br/>(code/submitted_date-* uploaded)
    stalled --> failed: a log appears<br/>(logs/ holds a file)
    failed --> successful: outputs appear<br/>(derivatives/ exists)
    pending --> [*]: clean --unsubmitted<br/>(deleted)
    pending --> archived: archive --status pending
    stalled --> archived: archive --status stalled
    failed --> archived: archive --status failed
    archived --> [*]
    successful --> [*]
```

| Status | Observed on the archive | Meaning |
|---|---|---|
| `pending` | `code/` but no submitted marker | Formed and waiting for a dispatcher. |
| `stalled` | A `code/submitted*` marker, but no logs | Claimed by an array task, with no logs uploaded yet. Logs are uploaded when a run finishes, so a capsule sits here for its whole run. It also stays here if its task died before uploading anything. An AIND run cut short by its time limit, preemption or `scancel` uploads its logs first, so it moves on to `failed`. |
| `failed` | Logs, but no `derivatives/` | Logs were uploaded without outputs. A run that uploads its logs part way through reads the same, and nothing on the archive tells the two apart, so they share one status. |
| `successful` | `derivatives/` | Outputs were uploaded. |
| `unknown` | None of the above | Also the fallback for a status cell in `jobs.tsv` that is empty or unrecognised. |

`archived` is not a status. An archived capsule has moved to `001873`, where it keeps whichever status it had.

### Timestamps

`jobs.tsv` records three timestamps, each read from the `dateModified` of an asset on the archive.

| Column | Taken from |
|---|---|
| `created_at` | `code/submit.sh` |
| `job_submission_time` | The earliest `code/submitted*` marker. A resubmitted capsule can carry several. |
| `job_completion_time` | The latest file under `logs/` |

`queue_wait_seconds` and `run_duration_seconds` are the differences between consecutive pairs. They are derived when the table is written and ignored when it is read back.

`process_wall_time_seconds` sums the duration of every process in the capsule's Nextflow `logs/timeline.html`. `jobs refresh` downloads each report to fill it in, and leaves it empty for a capsule with no readable report.

## From preparation to results

```mermaid
sequenceDiagram
    autonumber
    participant Op as jobs create / prepare
    participant Proc as processing/prepare-job-*/
    participant DANDI as 001697
    participant Task as array task
    participant Run as submit.sh

    Op->>DANDI: list existing capsules under pipeline-{name}/
    alt a capsule with this hash exists
        Op-->>Op: skip (unless forced)
    else new job
        Op->>Proc: download 001697's dandiset.yaml only
        Op->>Proc: render code/submit.sh, copy params and config, write dataset_description.json
        Op->>DANDI: dandi upload the capsule directory
    end

    Task->>DANDI: dandi download --preserve-tree {capsule}/code/
    Task->>DANDI: upload code/submitted_date-* (claim)
    Task->>Run: bash code/submit.sh, tee to the capsule's SLURM log path
    Run->>Proc: run in and write to the preparation tree
    Run->>DANDI: dandi upload
    Run->>Proc: append the tree's name to processing/done.txt
```

The submission script refers to the preparation tree by absolute path. That tree, `processing/prepare-job-*/001697/{capsule}/`, is where the pipeline writes its intermediate results, logs and outputs, and where its closing `dandi upload` uploads them from. The copy an array task downloads is used only to read `submit.sh` and to claim the capsule. Preparation trees therefore have to outlive the capsule's run, and nothing removes them automatically. `processing/done.txt` lists the ones whose script ran to completion.

## Curating a successful run

A successful `aind+ephys` capsule holds one SortingAnalyzer per recorded stream, under `derivatives/postprocessed/`. [SpikeInterface GUI](https://github.com/SpikeInterface/spikeinterface-gui) can open these straight from the archive's S3 bucket. Nothing is downloaded up front, and no AWS credentials are needed.

The steps below curate one stream, export the curated units to a new NWB file and upload that file to DANDI. They are written for `job-260612eac7ed`. Enter any other successful capsule to fill in its values. A job ID, a capsule path or a DANDI link to the capsule all work.

<div id="curation-widget"></div>

The code blocks are Bash. On Windows, run them in WSL or Git Bash.

### 1. Set up an environment

```bash
conda create --yes --name dandi-curation python=3.12
conda activate dandi-curation
pip install "spikeinterface-gui[web]" s3fs neuroconv remfile dandi
```

### 2. Curate in the web GUI

```bash
cat > curate_job-260612eac7ed.py << 'EOF'
import json
import pathlib

import spikeinterface
import spikeinterface_gui

ANALYZER_URL = "s3://dandiarchive/zarr/cc0f0a1e-f69e-489c-8501-255c20f83068/"
CURATION_FILE = pathlib.Path("job-260612eac7ed_block0_acquisition-ElectricalSeriesRaw_recording1_curation.json")


def save_curation(curation_data: dict) -> None:
    CURATION_FILE.write_text(json.dumps(curation_data, indent=4, default=str))
    print(f"Saved curation to {CURATION_FILE.absolute()}")


analyzer = spikeinterface.load_sorting_analyzer(ANALYZER_URL, load_extensions=False)
if CURATION_FILE.exists():
    curation_dict = json.loads(CURATION_FILE.read_text())
else:
    curation_dict = {"format_version": "2", "unit_ids": analyzer.unit_ids.tolist()}

spikeinterface_gui.run_mainwindow(
    analyzer,
    mode="web",
    curation=True,
    curation_dict=curation_dict,
    curation_callback=save_curation,
    skip_extensions=["waveforms", "principal_components"],
)
EOF
python curate_job-260612eac7ed.py
```

Open the `http://localhost:…` link it prints. Loading takes about half a minute. Press **Save curation** in the curation view to write the JSON file, then stop the script with `Ctrl+C`. Running the same command again picks up where the saved file left off.

The analyzer on the archive is read-only, so the curation is kept in that local JSON file rather than inside it. The `waveforms` and `principal_components` extensions are the largest, so they are skipped to keep loading fast. That hides the waveform heatmap and the PC scatter view. The trace views are hidden too, because the analyzer has no recording attached.

### 3. Export the curated units to NWB

```bash
cat > export_job-260612eac7ed.py << 'EOF'
import json
import pathlib
import uuid

import h5py
import hdmf.utils
import neuroconv.tools.spikeinterface
import numpy
import pynwb
import remfile
import spikeinterface
import spikeinterface.curation

ANALYZER_URL = "s3://dandiarchive/zarr/cc0f0a1e-f69e-489c-8501-255c20f83068/"
SOURCE_NWB_URL = "https://dandiarchive.s3.amazonaws.com/blobs/a05/ac4/a05ac4e6-030f-49b7-ac62-e1aa74e54dcb"
CURATION_FILE = pathlib.Path("job-260612eac7ed_block0_acquisition-ElectricalSeriesRaw_recording1_curation.json")
OUTPUT_FILE = pathlib.Path("000397/sub-Pt03/sub-Pt03_desc-curated_ecephys.nwb")
UNITS_DESCRIPTION = (
    "Units sorted by the AIND ephys pipeline in DANDI Compute job-260612eac7ed "
    "(block0_acquisition-ElectricalSeriesRaw_recording1), then curated in SpikeInterface GUI."
)

analyzer = spikeinterface.load_sorting_analyzer(ANALYZER_URL, load_extensions=False)
curation = json.loads(CURATION_FILE.read_text())
curated_sorting = spikeinterface.curation.apply_curation(analyzer.sorting, curation)

# Merged and split units are new, so they get no quality metrics.
quality_metrics = analyzer.load_extension("quality_metrics").get_data()
for metric in quality_metrics.columns:
    values = [quality_metrics[metric].get(unit_id, numpy.nan) for unit_id in curated_sorting.unit_ids]
    curated_sorting.set_property(metric, numpy.asarray(values, dtype=float))

with h5py.File(remfile.File(SOURCE_NWB_URL), "r") as file, pynwb.NWBHDF5IO(file=file, load_namespaces=True) as io:
    source = io.read()
    subject = None
    if source.subject is not None:
        # Some sources use a Subject extension, so only the base Subject's fields are copied.
        subject_fields = {argument["name"] for argument in hdmf.utils.get_docval(pynwb.file.Subject.__init__)}
        subject = pynwb.file.Subject(**{k: v for k, v in source.subject.fields.items() if k in subject_fields})
    nwbfile = pynwb.NWBFile(
        session_description=source.session_description,
        identifier=str(uuid.uuid4()),
        session_start_time=source.session_start_time,
        session_id=source.session_id,
        subject=subject,
    )

neuroconv.tools.spikeinterface.add_sorting_to_nwbfile(
    sorting=curated_sorting, nwbfile=nwbfile, units_description=UNITS_DESCRIPTION
)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
with pynwb.NWBHDF5IO(OUTPUT_FILE, "w") as io:
    io.write(nwbfile)
print(f"Wrote {len(curated_sorting.unit_ids)} curated units to {OUTPUT_FILE.absolute()}")
EOF
python export_job-260612eac7ed.py
```

The new file holds only the curated units. Removed units are dropped, merges and splits are applied, and each label becomes a column such as `quality`. The quality metrics of the original sort are carried over as columns. Session and subject metadata are copied from the source NWB file, which is streamed rather than downloaded.

### 4. Upload to DANDI

```bash
dandi download --download dandiset.yaml --existing refresh DANDI:000397
cd 000397
dandi upload sub-Pt03/sub-Pt03_desc-curated_ecephys.nwb
cd ..
```

This uploads next to the source asset, in the Dandiset it came from. Change **Upload to Dandiset** above to send it to another Dandiset you own. `dandi upload` asks for your API key from [dandiarchive.org](https://dandiarchive.org) unless `DANDI_API_KEY` is set. It validates the file before uploading it. Problems it reports in the session or subject metadata, such as a missing species, come from the source NWB file.

## Formation rules

- A capsule is never formed twice. Before preparing, the code lists existing capsules under the asset's `pipeline-{name}/` directory in `001697` and stops if one has the same hash, whatever its date or status.
- `jobs create` additionally skips any asset whose `(pipeline, params, config, content_id)` already has a capsule of any pipeline version. New pipeline versions are only rolled out with `--latest`.
- New capsules always target the latest version available on the machine. For `aind+ephys` that is the highest `vX.Y.Z` tag in the local pipeline checkout. For `lfp` it is this package's own version, which also picks the container tag.
- An AIND parameter file declares the pipeline version it was written for. Preparation refuses a requested version in a different major series, or one older than the file's.
- An asset whose content ID is missing from the `content-id-to-usage-dandiset-path` cache is skipped with a warning. When a content ID is used at several paths, the first one listed is used.
