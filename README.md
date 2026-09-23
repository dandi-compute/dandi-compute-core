# DANDI Compute: Orchestration code

Contains essential code for orchestrating computation submission and queue management for processing pipelines acting on DANDI assets.

## Documentation

The documentation is at [dandi-compute-code.readthedocs.io](https://dandi-compute-code.readthedocs.io). It covers how the system fits together, setting it up, job capsules, the data model, the infrastructure it runs on, and the API reference. The sources are in [`docs/`](docs/).

## Installation

```bash
pip install git+https://github.com/dandi-compute/dandi-compute-core
```

This installs the `dandicompute` command. Run `dandicompute --help` to see what it can do.

## Contributing

See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for development setup, and for how to add parameter sets, configs and pipelines.
