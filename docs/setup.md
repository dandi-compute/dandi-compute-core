# Setup

DANDI Compute runs passively. Once it is installed and running, it finds qualifying assets, forms and dispatches job capsules, and reports on them without anyone driving it. This page covers installing it, and the commands for stepping in by hand when something needs attention.

## Installation

The orchestration side is a light install. It needs Python 3.10 or newer.

```bash
pip install git+https://github.com/dandi-compute/dandi-compute-core
```

On the cluster the package is installed from the `code/` checkout inside the base directory (`{base}` below, see [Infrastructure](infrastructure.md)), so that the commit recorded in each capsule's provenance matches what is on disk:

```bash
cd {base}/code
git pull
pip install -e .
```

The LFP pipeline's scientific stack (SpikeInterface, neuroconv, pynwb) is not part of this install. It lives in the LFP container. To reproduce that environment locally, install both projects:

```bash
pip install . ./src/dandi_compute_code/lfp_pipeline/envs
```

For development, the `dev`, `docs` and `schemas` dependency groups are available, and `all` pulls in every one of them:

```bash
pip install -e . --group all
```

## The command line

Everything is driven by the `dandicompute` command. Each command is a thin wrapper around a function in the public API, so see the [API reference](api/index.rst) for what each one does, and run any command with `--help` for its options.

## Stepping in by hand

Normal operation needs none of these. These commands are for debugging a single asset, rolling out a change, or cleaning up after failures.

### Process one asset by hand

Useful when debugging a single asset. Assets are addressed by content ID, or by Dandiset and path.

```bash
export DANDI_API_KEY=...

# Form the capsule and print the command that would submit it
dandicompute prepare aind --id 048d1ee9-83b7-491f-8f02-1ca615b1d455 --version v1.2.4

# Or address the asset by where it lives
dandicompute prepare aind --dandiset 000409 --dandipath sub-mouse01/sub-mouse01_ecephys.nwb --version v1.2.4

# Pick a registered parameter set and cluster config, and submit straight away
dandicompute prepare aind --id <content id> --version v1.2.4 --params no-motion --config default --submit
```

If a capsule already exists for that asset, parameter set, config and pipeline version, nothing is formed and the command says so. A capsule is never formed twice. Forming test capsules for the example asset, across every configured pipeline and parameter set, is one flag:

```bash
dandicompute prepare aind --test
```

### Keep the queue full

`jobs create` crosses every pipeline and parameter set in `pipeline_configs.json` with that pipeline's qualifying assets. Assets that already have a capsule for the combination are skipped.

```bash
dandicompute jobs create                     # every configured pipeline
dandicompute jobs create --pipeline lfp      # one pipeline
dandicompute jobs create --limit 20          # at most 20 new capsules
```

`--limit` samples round-robin across Dandisets, so a small limit spreads work over many datasets rather than exhausting the first one.

`--latest` deliberately ignores existing capsules and forms a new one for every qualifying asset against the newest local pipeline version. It is how a new pipeline release is rolled out over assets that were already processed.

### Run the queue

```bash
dandicompute jobs pending --silent && dandicompute jobs dispatch
```

`jobs dispatch` is safe to run repeatedly. A pipeline whose dispatcher is still on the cluster is skipped, so overlapping invocations never stack arrays. [Array dispatch](dispatch.md) explains the mechanism in full, including how array resources are sized.

```bash
dandicompute jobs dispatch --pipeline lfp --max 4   # one pipeline, overriding its limit
dandicompute jobs dispatch --jitter 0               # no random start delay
```

### Report on the queue

```bash
dandicompute jobs refresh       # jobs.tsv + paths.tsv in 001697 and 001873
dandicompute issues summarize   # issues_dump.json + issues_summary.json
```

These are uploaded into the Dandiset's `derivatives/` directory, so the state of the queue can be browsed from the archive without cluster access. Their formats are in [Data model](data_model.md). Aggregate figures, such as the number of capsules, the bytes processed or the total compute time, are sums over the columns of `jobs.tsv`.

### Clear out failures

```bash
dandicompute archive --status failed      # every capsule with logs but no output
dandicompute archive --status stalled     # claimed, but nothing was ever logged
dandicompute archive --job derivatives/dandisets-000/dandiset-000409/sub-mouse01/pipeline-aind+ephys/job-260916a1b2c3
```

Archiving preserves each capsule's path exactly and deletes it from the source only after the upload to the archive succeeds. Once a failed capsule is archived, the next `jobs create` sees no capsule for that asset and forms a fresh one.

To throw away capsules that were never claimed, for example after changing a parameter set, delete them instead of archiving:

```bash
dandicompute clean --unsubmitted
```

### Tidy the cluster

```bash
dandicompute clean --work              # empty work/ except its apptainer_cache/
dandicompute clean --dispatch          # remove dispatch directories with no live array
dandicompute clean --dispatch --age 6  # only those at least 6 hours old
```

## Using the Python API

Every command is a thin wrapper around the public API, so the same operations are available from Python.

```python
import dandi_compute_code.queue as queue

state = queue.PipelineQueue.from_dandi()      # the live queue, rebuilt from 001697
print(len(state), "capsules")
for capsule in state.failed:
    print(capsule.job.job_id, capsule.job.within_dandiset_path)

queue.PipelineQueue.has_pending_jobs()       # same check as `jobs pending`
```

See the [API reference](api/index.rst) for every public function and class.
