# Schemas

The internal structures this package defines and passes around are described by [LinkML](https://linkml.io) schemas under `src/dandi_compute_code/schemas/`. The schemas are the source of truth for what these structures may contain. The Python classes carry the behaviour.

| Schema | Describes |
| --- | --- |
| `pipeline_config.linkml.yaml` | `queue/pipeline_configs.json`: which parameter sets each pipeline forms capsules for, its per-asset overrides and its dispatcher limits. |
| `registry.linkml.yaml` | Every `registries/*.json` file, which maps a short key onto a packaged file and the MD5 it must still have. |
| `job_capsule.linkml.yaml` | A job capsule's identity and lifecycle status, which is one row of `jobs.tsv` plus its rows of `paths.tsv`. |
| `dispatch.linkml.yaml` | One pipeline's array dispatcher settings, and what one dispatch attempt produced. |
| `assets_metadata.linkml.yaml` | The slice of a Dandiset's `assets.jsonld` this package indexes. |

## Parameter schemas are not among them

A pipeline's parameter schema stays plain JSON Schema, written by hand. `src/dandi_compute_code/lfp_pipeline/params/parameter_schema.json` is the one this repository ships.

That is the form the documentation website renders, and the form `validate_lfp_parameters` checks against with `jsonschema` directly. Describing those parameters in LinkML as well would mean either two schemas for one thing or a generator between them, and neither is worth it for a schema this small and this stable.

## Validation

Validation happens twice, because the two validators are good at different things.

At runtime, `dandi_compute_code.schemas.validate_against_schema` validates against these schemas using `linkml-runtime`, which the base install already carries. Loading the pipeline configuration or a registry goes through it, so a malformed file is rejected where it is read rather than misread.

In CI, the `Validate LinkML schemas` workflow installs the full `linkml` distribution (`pip install --group schemas`), compiles every schema to JSON Schema and validates every packaged data file against the schema that describes it. That validator rejects mismatched scalar types where the runtime one quietly normalizes them, and compiling the schemas catches one that loads but does not express what it appears to.

The workflow sets `DANDI_COMPUTE_REQUIRE_STRICT_SCHEMA_VALIDATION=1`, so those checks fail rather than skip if the toolchain ever goes missing there. It runs on pull requests, on the merge queue, and as part of the daily tests.

The same test module also asserts that each schema class carries exactly the fields of the dataclass it describes, so a schema cannot drift once its model changes.

## Adding a schema

Put it in `src/dandi_compute_code/schemas/` named `[name].linkml.yaml`, then add it to `SCHEMA_PATHS` and `SCHEMA_TREE_ROOTS` in `schemas/_globals.py`. Both validation layers enumerate those, so it is covered by CI from then on.
