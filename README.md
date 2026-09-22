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



## Schemas

Every structure this package defines and passes around is described by a [LinkML](https://linkml.io) schema under `src/dandi_compute_code/schemas/`. The schemas are the source of truth for what these structures may contain. The Python classes carry the behaviour.

| Schema | Describes |
| --- | --- |
| `pipeline_config.linkml.yaml` | `queue/pipeline_configs.json`: which parameter sets each pipeline forms capsules for, its per-asset overrides and its dispatcher limits. |
| `registry.linkml.yaml` | Every `registries/*.json` file, which maps a short key onto a packaged file and the MD5 it must still have. |
| `lfp_parameters.linkml.yaml` | The LFP extraction parameters in `lfp_pipeline/params/`. |
| `job_capsule.linkml.yaml` | A job capsule's identity and lifecycle status, which is one row of `state.tsv`. |
| `dispatch.linkml.yaml` | One pipeline's array dispatcher settings, and what one dispatch attempt produced. |
| `assets_metadata.linkml.yaml` | The slice of a Dandiset's `assets.jsonld` this package indexes. |

Validation happens twice, because the two validators are good at different things.

At runtime, `dandi_compute_code.schemas.validate_against_schema` validates against these schemas using `linkml-runtime`, which the base install already carries. Loading the pipeline configuration, a registry or a set of LFP parameters goes through it, so a malformed file is rejected where it is read rather than misread.

In CI, the `Validate LinkML schemas` workflow installs the full `linkml` distribution (`pip install --group schemas`), compiles every schema to JSON Schema and validates every packaged data file against the schema that describes it. That validator rejects mismatched scalar types where the runtime one quietly normalizes them, and compiling the schemas catches one that loads but does not express what it appears to. The workflow sets `DANDI_COMPUTE_REQUIRE_STRICT_SCHEMA_VALIDATION=1`, so those checks fail rather than skip if the toolchain ever goes missing there.

The same test module also asserts that each schema class carries exactly the fields of the dataclass it describes, so a schema cannot drift once its model changes.

### Published JSON Schemas

A pipeline's parameter schema is also published as JSON Schema, because that is the form the documentation website renders. Those files are generated from their LinkML source rather than maintained alongside it, so the two cannot disagree:

```bash
python -m dandi_compute_code.schemas
```

This writes `src/dandi_compute_code/lfp_pipeline/params/parameter_schema.json`. Do not edit that file by hand. CI regenerates it and fails if the committed copy differs, and also checks that the published schema rejects exactly what the runtime LinkML path rejects.

The generated output is flat and self-contained, with every enumeration inlined rather than referenced through `$defs`, so a renderer can show a field's allowed values without resolving anything. A numeric set of allowed values is declared in the LinkML schema as an enumeration (its values being text) and pointed at from the slot it governs by a `numeric_enum` annotation. The generator and the loader both read that one declaration.

To add a schema, put it in `src/dandi_compute_code/schemas/` named `[name].linkml.yaml` and add it to `SCHEMA_PATHS` and `SCHEMA_TREE_ROOTS` in `schemas/_globals.py`. Both validation layers enumerate those, so it is covered by CI from then on.

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

- **`params/`** — JSON parameter files (e.g., `name-default.json`) plus `parameter_schema.json`, the JSON Schema that defines and constrains the exposed LFP parameters and is what the website renders.
  `parameter_schema.json` is generated from `src/dandi_compute_code/schemas/lfp_parameters.linkml.yaml`, which is the source of truth. Do not edit it by hand. After changing the LinkML schema, regenerate it with `python -m dandi_compute_code.schemas`. CI fails if the committed file is out of date.
  To add a new parameters file:
  1. Add the `name-[id].json` file to this directory.
  2. Register it in `registries/registered_params.json` by adding an entry with the short name as the key, and its relative `path` and full MD5 `md5` as values.
  The short name can then be passed via the `parameters_key` argument of `load_lfp_parameters`.

- **`registries/`** — JSON registry files mapping short names to resource paths and checksums (e.g., `registered_params.json`).

The LFP pipeline depends on heavy scientific packages (SpikeInterface, neuroconv, pynwb). These are deliberately kept out of the base install and are declared only in `src/dandi_compute_code/lfp_pipeline/envs/pyproject.toml`. To run the LFP pipeline, use the runtime container built from `src/dandi_compute_code/lfp_pipeline/containers/lfp.Dockerfile`, or reproduce it locally with `pip install . ./src/dandi_compute_code/lfp_pipeline/envs`. The container image is built and pushed to the GitHub Container Registry by the manually dispatched `Build and upload LFP container image` workflow.
