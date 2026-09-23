# Example queue state

`jobs.tsv` is a single static example of a queue `jobs.tsv` file.
It is the shared ground truth for the queue test suite.

Each row is one job capsule, matching the format produced by `PipelineQueue.to_tsv` and consumed by `PipelineQueue.from_tsv`.
`paths.tsv` beside it holds the asset paths of those capsules, one per row, keyed by `job_id`.
Every capsule has a dataset description. The failed and successful ones also have a log, and the successful ones an output.
Which mapping a path belongs to is read from where it sits beneath the capsule directory.

`jobs.json` and `paths.json` are the exact BIDS-style sidecars written beside those two tables.
They are generated from the job capsule LinkML schema, and a test fails whenever the generated sidecars stop matching them.
After an intended change to a column description, regenerate them with `PipelineQueue.to_tsv_sidecar_string()` and `PipelineQueue.to_paths_tsv_sidecar_string()`.

The failed and successful capsules carry mock submission and completion times, so their `queue_wait_seconds` and `run_duration_seconds` are filled in. The stalled ones have a submission time only.
Tests load the file through the fixtures in `../conftest.py` and select the entry they need by its `within_dandiset_path`, which is named to describe the scenario it covers.
Rows that share a `within_dandiset_path` are told apart by their `config`.

`job_id` is the `job-{YYMMDD}{hash}` directory name of the capsule, and is what names the capsule directory on disk.

The empty-queue case is written inline by the few tests that need it rather than kept as a file.

The point in the job lifecycle is the `status` column:

- pending: code prepared but never submitted
- stalled: submitted, but no logs or output ever appeared
- failed: logs present, no output
- successful: output present
- unknown: nothing observed about the capsule at all

| `within_dandiset_path` | Scenario |
| --- | --- |
| `sub-pending` | Prepared but never submitted. |
| `sub-successful` | Output present, with a known source-asset size (120 bytes). |
| `sub-failed/ses-one` and `sub-failed/ses-two` | Two capsules with logs but no output (the failed state), reaching `max_fail_per_dandiset` for Dandiset 000001 (both mapped to `asset-aaa`). |
| `sub-fresh` (Dandiset 000002) | A queued asset in another Dandiset with no failures (mapped to `asset-bbb`). |
| `sub-sole/ses-capsule` (Dandiset 001371) | The sole capsule in its pipeline tree, so empty parents are pruned on removal. |
| `sub-two/ses-capsules` (configs `cfgtwoa` and `cfgtwob`) | The `cfgtwoa` capsule is queued and the `cfgtwob` one is completed, so the shared parent is kept after the queued capsule is removed. |
| `sub-already/ses-submitted` | Queued in state, but a submitted marker exists on disk, so it is left alone. |
| `sourcedata/sub-test+bids` (Dandiset 001849) | A queued capsule whose recorded `within_dandiset_path` differs from the on-disk layout, exercising fallback capsule-directory resolution. |
| `sub-stalled/ses-one` and `sub-stalled/ses-two` | Two capsules submitted to the scheduler with no logs or output ever appearing (the stalled state), both mapped to `asset-ccc`. |
