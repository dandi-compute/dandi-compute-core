# Array dispatch

Every pipeline is run on the cluster by exactly one SLURM array job, its dispatcher. This note describes how that works, why it is built the way it is, and how to operate it.

## How a dispatch works

`dandicompute jobs dispatch` reads the job capsules awaiting submission from the DANDI assets metadata, takes the ones belonging to each configured pipeline, writes them to a manifest, and submits an array covering them.

Each array task then:

1. reads its capsule out of the manifest by `SLURM_ARRAY_TASK_ID`, which is why array indices are one-based
2. downloads that capsule's `code/` tree with `dandi download --preserve-tree`
3. claims it by writing a `submitted_date-*` marker and uploading that marker back
4. runs the capsule's `submit.sh`
5. removes its working tree, unless `--test` was passed

The capsule is claimed before it runs, so a dispatch that overlaps a live array sees it as submitted and does not place it in a second one.

## The concurrency limit

SLURM holds the queue and enforces the limit. The array's `%n` throttle is the pipeline's `max_concurrent` setting, so tasks beyond it stay queued in SLURM rather than being resubmitted by a later invocation.

This replaced a design that submitted capsules one `sbatch` at a time and held concurrency down by counting running jobs before each submission. That made every cron invocation a scheduler of its own, racing the others for the same pending capsules.

Because the throttle caps *simultaneously running* tasks and not submitted ones, a 200-capsule array at `%2` puts 198 elements in `PENDING` where they consume nothing, and the real work enters the cluster two at a time.

## Not resubmitting a live dispatcher

A pipeline whose dispatcher is still on the cluster is skipped. That array already holds the pipeline's pending tasks, so a second one would only duplicate them.

Liveness is decided by job name. Each dispatcher carries `dandicompute-dispatch-{pipeline}`, with characters a pipeline name may hold but a job name should not folded to hyphens, so `aind+ephys` becomes `dandicompute-dispatch-aind-ephys`. A `squeue --me --name <job name>` returning anything in an active state means the dispatcher still owns its array.

Capsules formed after that array went out wait for it to be exhausted and go out with the next dispatch. Repeated invocations from a crontab are therefore safe: they add nothing while an array is working.

## Where an array's resources come from

A capsule runs inside its array task rather than being submitted as a job of its own, so the task's allocation is the one the capsule actually gets.

This matters because `#SBATCH` directives are parsed by the `sbatch` command when it reads a script at submission time. The header inside a capsule run with `bash` is a block of inert comments, so an array sized wrongly would silently truncate the capsule it runs.

Rather than restate those requests in the pipeline configuration, the dispatcher reads each pending capsule's own `code/submit.sh` back out of the archive and groups capsules by what they ask for. Each group gets an array sized for it.

Pipelines differ here for real reasons:

| pipeline | its `submit.sh` | resulting array |
|---|---|---|
| `aind+ephys` | a Nextflow driver that dispatches the heavy work to its own jobs, so it needs very little itself | 1GB / 1 CPU / `mit_normal` / 12h |
| `lfp` | does its work in process via `datalad containers-run` | 16GB / 1 CPU / `mit_preemptable` / 48h |

Capsules of one pipeline normally agree, since one template renders them all, so this is one array per pipeline in practice. They can diverge when a template changed between the releases that prepared them, and a capsule needing more than its neighbours would otherwise be truncated by an array sized for them.

The configured concurrency limit is what the pipeline may run at once *in total*, so it is shared out across the arrays rather than applied to each. A pipeline limited to 4 that splits into two groups gets `%2` on each.

A capsule whose script cannot be read falls back to its pipeline's packaged submission template, and a pipeline with no packaged template falls back to deliberately generous requests. Under-provisioning a task means the capsule inside it is killed mid-run, so both fallbacks err high.

### Why the capsule is not submitted separately

Having the array task `sbatch` its capsule and wait on it would honour the capsule's own header natively, but it was rejected:

- it spends two job slots per unit of work, both counting against per-user job limits
- the waiting task has to outlive the capsule it watches, covering the capsule's queue time as well as its run time
- anything that kills that waiting task first (its own wall time, preemption, a controller restart mid-poll) frees the array slot while the capsule keeps running, which silently over-admits past the limit

Running the capsule in the task keeps the throttle honest with no coordination of our own. A task finishing *is* its capsule finishing, so SLURM admits the next one at exactly the right moment.

### The one directive reproduced by hand

`#SBATCH --output` points into the capsule's own `logs/` directory, which is where the capsule's closing `dandi upload` uploads its SLURM log from and where `dandicompute issues dump` globs it back as `logs/*slurm.log`.

The array task therefore parses that path out of the capsule script, expands the SLURM filename patterns, and tees the run into it. `pipefail` keeps a failing capsule a failing array task through that pipe.

## Configuration

Each pipeline's dispatcher is configured in `src/dandi_compute_code/queue/pipeline_configs.json` under its `dispatch` key, and validated by the `Dispatch` class in `src/dandi_compute_code/schemas/pipeline_config.linkml.yaml`:

```json
"dispatch": {
    "max_concurrent": 2,
    "max_array_tasks": 500
}
```

`max_concurrent` is the per-pipeline concurrency limit. Pipelines may differ, and setting one does not affect another.

`max_array_tasks` caps how many capsules one array may hold, with the rest staying pending for the next dispatch. Set it to `null` for no upper bound, which places every pending capsule in one array and relies on the cluster's own `MaxArraySize` being large enough. An omitted key is not the same as `null`: it takes the default of 500.

Those two limits are the whole of what is configurable, per the section above. A pipeline that declares no `dispatch` block still dispatches, on the built-in defaults.

## Running a dispatch

```bash
dandicompute jobs dispatch
```

To skip the dispatch entirely when there is no queued work, gate it on `jobs pending`, which exits 0 when at least one job awaits submission and 1 when nothing does:

```bash
dandicompute jobs pending --silent && dandicompute jobs dispatch
```

The output reports what each pipeline dispatched, with one line per array naming that group's size, its requests and its share of the limit:

```
aind+ephys: dispatched 15 capsules as 2 array jobs, one per distinct set of requested resources.
  array 900: 12 capsules requesting 1GB / 1 CPU / mit_normal / 12:00:00, at most 2 at a time
  array 901: 3 capsules requesting 16GB / 1 CPU / mit_preemptable / 48:00:00, at most 2 at a time

  logs, manifests and scripts: processing/derivatives/logs/dandicompute-dispatch-aind-ephys

lfp: dispatched 1 capsule as array job 902.
  array 902: 1 capsule requesting 16GB / 1 CPU / mit_preemptable / 48:00:00, at most 4 at a time
  logs, manifests and scripts: processing/derivatives/logs/dandicompute-dispatch-lfp
```

To dispatch a single pipeline, optionally overriding its configured limit for that invocation:

```bash
dandicompute jobs dispatch --pipeline lfp --max 4
```

The concurrency limit is a per-pipeline setting, so `--max` requires `--pipeline` and overrides that one pipeline's limit. It is rejected on its own rather than applied to every pipeline at once.

`--jitter` applies a random delay before processing, which spreads concurrent invocations so they do not read the cluster state at the same moment. `--test` leaves each array task's working tree on disk for debugging.

## The log directory

Dispatch works inside the `processing/` directory of the base directory given by `--base` (see [Infrastructure](infrastructure.md) for its layout). Every dispatcher keeps its record in one central place under it, `derivatives/logs/{job name}/`. So `aind+ephys` records into `derivatives/logs/dandicompute-dispatch-aind-ephys/`. This mirrors the Dandiset layout, where `derivatives/` already holds dispatch-level records such as `jobs.tsv`.

Each dispatch names its files after the moment it was formed, `{YYYYMMDD-HHMMSS}`. For resource group `n` it writes:

- `{timestamp}-manifest-{n}.txt`, the capsules that group covers, one per line
- `{timestamp}-dispatch-{n}.sh`, the array script generated for that group and handed to `sbatch`
- `{timestamp}-dispatch-{n}-{array job id}_{task id}.log`, each array task's own output

These files are the lasting record of what each dispatch covered and how it was submitted. Nothing removes them, `clean` included.

The array tasks read their manifest from here as they start, so the log directory has to stay readable from the compute nodes.

A capsule's own SLURM log does not live here. It goes to the capsule's `logs/` directory, where the capsule uploads it from.

## The dispatch directory

Each dispatch also creates a working directory under `processing/`, named `dandicompute-dispatch-{pipeline}-{YYYYMMDD-HHMMSS}` with the same timestamp as its records. It holds only `task-{array job id}-{task id}/`, the working tree a task downloads its capsule into. That tree is removed when the task finishes unless `--test` is passed.

It has to stay reachable from the compute nodes for as long as the arrays live, so nothing in it is cleaned up at submission time.

## Cleaning up

Finished dispatch directories are swept up by `clean`:

```bash
dandicompute clean --dispatch
```

A dispatch directory is removed only once both guards pass: its pipeline has no dispatcher left on the cluster, and it is at least `--age` hours old (24 by default). The age floor covers the window between submitting an array and SLURM reporting it.

Both matter, since removing the directory under a live array would pull the working tree out from under its running tasks. Anything in the processing directory that is not a dispatch directory is left alone. That includes the central `derivatives/logs/` directory, so the manifests, scripts and array output of a cleaned dispatch are kept.

`clean --work` empties the base directory's `work/` instead, and the two flags can be given together.
