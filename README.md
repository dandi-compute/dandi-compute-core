# DANDI Compute: Orchestration code

Contains essential code for orchestrating computation submission and queue management for processing pipelines acting on DANDI assets.



## Job capsules

Each run of a pipeline over one asset lives in its own job capsule directory:

```
derivatives/dandisets-{first 3 digits}/dandiset-{dandiset_id}/{dandi path}/pipeline-{pipeline}/job-{YYMMDD}{hash}
```

The job ID is the whole name. `YYMMDD` is the date the capsule was prepared, which keeps the name readable and separates re-attempts of the same job across days. The hash is the first six characters of the MD5 checksum of the fields that identify the job, so capsules for different parameters, configs, versions or assets stay apart.

The codebase version is deliberately left out of the hash. A job is the same logical job no matter which release of this package formed it, which is how the queue decides that a capsule already exists and must not be formed a second time.

Everything the name used to spell out is recorded in two places instead:

- the `DandiCompute` provenance block in the capsule's `dataset_description.json`
- the `derivatives/state.tsv` summary table, which reads that provenance back

Two capsules can still land on one job ID when they are the same logical job prepared on the same day: re-attempts, or runs differing only in codebase version, which the hash ignores. Those carry a `-2`, `-3` counter, and the queue reads through it, so every spelling of a name reads back as the same job. Preparation never produces one, since it does not form a capsule for a job that already has one, but capsules migrated onto this layout do.

The job ID is the only capsule layout this package understands. Capsules prepared before it existed carry older names and are invisible to the queue until they are migrated.

## Manual dispatch commands on MIT Engaging

To run manually with confirmation to trigger (for debugging):

```bash
dandicompute aind prepare --id [full content ID]
```

To run automatically:

```bash
dandicompute aind prepare --id [full content ID] --submit
```

To test automatically on the example asset:

```bash
dandicompute prepare aind --test
```

To clean unsubmitted job capsules:

```bash
dandicompute queue clean --dandiset ./dandi/001697/
```

To archive a failed job capsule by moving it from `001697` to the permanent archive `001873`:

```bash
dandicompute archive --job derivatives/dandisets-000/dandiset-000409/sub-mouse01/pipeline-aind+ephys/job-260916a1b2c3
```


To check whether there is any queued work before dispatching, use `queue pending`. It exits with code 0 when at least one job is awaiting submission, and code 1 when there is nothing to process. This lets a crontab skip the dispatch entirely when the queue is empty:

```bash
dandicompute queue pending --silent && dandicompute queue process --processing ./processing/
```



## Array dispatch

Every pipeline is run on the cluster by exactly one SLURM array job, its dispatcher. `dandicompute queue process` collects the pipeline's pending job capsules, writes them to a manifest, and submits a single array covering them. Each array task reads its capsule out of the manifest by task index, downloads the capsule's `code/` tree, claims it with a submitted marker, and runs its `submit.sh`.

SLURM holds the queue and enforces the limit. The array's `%n` throttle is the pipeline's `dispatch.max_concurrent` setting, so the remaining tasks stay queued in SLURM rather than being resubmitted by a later invocation. Capsules are never submitted one `sbatch` at a time, and there is no polling of running job counts.

A pipeline whose dispatcher is still on the cluster is skipped. The live array already holds that pipeline's pending tasks, so a second one would only duplicate them. Capsules formed after it was submitted wait for it to be exhausted and go out with the next dispatch. This makes repeated invocations from a crontab safe: they add nothing while an array is working.

Each pipeline's dispatcher is configured in `src/dandi_compute_code/queue/pipeline_configs.json` under its `dispatch` key:

```json
"dispatch": {
    "max_concurrent": 2,
    "max_array_tasks": 500
}
```

`max_concurrent` is the per-pipeline concurrency limit. `max_array_tasks` caps how many capsules one array may hold, and capsules beyond it stay pending for the next dispatch. Set `max_array_tasks` to `null` for no upper bound, which places every pending capsule in one array and relies on the cluster's own `MaxArraySize` being large enough. An omitted key is not the same as `null`: it takes the default of 500.

Those two limits are the whole of what is configurable, and deliberately so.

A capsule runs inside its array task rather than being submitted as a job of its own, which means the task's allocation is the one the capsule actually gets. `#SBATCH` directives are read by the `sbatch` command when it parses a script at submission time, so the header inside a capsule run with `bash` is a block of inert comments.

Rather than restate those requests in the queue configuration, the dispatcher reads each pending capsule's own `code/submit.sh` back out of the archive and groups capsules by what they ask for. Each group gets an array sized for it.

Pipelines differ here for real reasons. The AIND submission script is a Nextflow driver that dispatches the heavy work to its own jobs and needs very little itself, so its array comes out at 1GB / 1 CPU / `mit_normal` / 12h. The LFP script does its work in process and needs a lot, so its array comes out at 16GB / 1 CPU / `mit_preemptable` / 48h.

Capsules of one pipeline normally agree, since one template renders them all, so this is one array per pipeline in practice. They can diverge when a template changed between the releases that prepared them, and a capsule needing more than its neighbours would otherwise be truncated by an array sized for them. The configured concurrency limit is what the pipeline may run at once in total, so it is shared out across the arrays rather than applied to each.

A capsule whose script cannot be read falls back to its pipeline's packaged submission template, and a pipeline with no packaged template falls back to deliberately generous requests. Under-provisioning a task means the capsule inside it is killed mid-run, so both fallbacks err high.

Running the capsule in the task is what keeps the throttle honest with no coordination of our own: a task finishing *is* its capsule finishing, so SLURM admits the next one at exactly the right moment. The alternative of submitting the capsule separately and waiting on it would spend two job slots per unit of work and would free the slot early whenever the waiting task was killed first.

The one directive that still has to be reproduced by hand is `#SBATCH --output`. It points into the capsule's own `logs/` directory, which is where the capsule uploads its SLURM log from and where `issues dump` reads it back, so the array task parses that path out of the capsule script and tees the run into it.

The dispatch directory created under `--processing` holds the manifest, the generated array script, and the array's logs. It has to stay readable from the compute nodes for as long as the array lives, so it is not cleaned up at submission time.

To dispatch a single pipeline, or to override its configured concurrency limit for one invocation:

```bash
dandicompute queue process --processing ./processing/ --pipeline lfp --max 4
```



## Contributing Non-Code Files

Non-code files for the AIND ephys pipeline are organized under the following subdirectories of `src/dandi_compute_code/aind_ephys_pipeline/`:

- **`templates/`** — Jinja2 submission script templates (e.g., `submission_template.txt`).
  Add a new `.txt` template here and reference it via `_globals.py` or a new globals module.

- **`params/`** — JSON parameter files passed to the pipeline (e.g., `default.json`, `no_motion.json`).
  To add a new parameters file:
  1. Add the `[id].json` file to this directory.
  2. Register it in `registries/registered_params.json` by adding an entry with the short name as the key, and its relative `path` and full MD5 `md5` as values, e.g.:
     ```json
     "my+params": {
       "path": "my_params.json",
       "md5": "<md5 hash of the file>"
     }
     ```
  The short name can then be passed via the `parameters_key` argument in `_prepare_job.py` or via `--params` on the CLI.

- **`registries/`** — JSON registry files mapping short names to resource paths and checksums (e.g., `registered_params.json`).

- **`configs/`** — Nextflow configuration files for a specific compute environment (e.g., `mit_engaging.config`).
  To add a new config file:
  1. Add the `[environment].config` file to this directory.
  2. Register it in `registries/registered_configs.json` by adding an entry with the short name as the key, and its relative `path` and full MD5 `md5` as values.
  Use `--config` / `config_key` to select a registered config (default: `default`).

Non-code files for the LFP pipeline are organized under the following subdirectories of `src/dandi_compute_code/lfp_pipeline/`:

- **`params/`** — JSON parameter files (e.g., `name-default.json`) plus `parameter_schema.json`, the JSON Schema that defines and constrains the exposed LFP parameters.
  To add a new parameters file:
  1. Add the `name-[id].json` file to this directory.
  2. Register it in `registries/registered_params.json` by adding an entry with the short name as the key, and its relative `path` and full MD5 `md5` as values.
  The short name can then be passed via the `parameters_key` argument of `load_lfp_parameters`.

- **`registries/`** — JSON registry files mapping short names to resource paths and checksums (e.g., `registered_params.json`).

The LFP pipeline depends on heavy scientific packages (SpikeInterface, neuroconv, pynwb). These are deliberately kept out of the base install and are declared only in `src/dandi_compute_code/lfp_pipeline/envs/pyproject.toml`. To run the LFP pipeline, use the runtime container built from `src/dandi_compute_code/lfp_pipeline/containers/lfp.Dockerfile`, or reproduce it locally with `pip install . ./src/dandi_compute_code/lfp_pipeline/envs`. The container image is built and pushed to the GitHub Container Registry by the manually dispatched `Build and upload LFP container image` workflow.
