# syntax=docker/dockerfile:1

FROM neurodebian:trixie

LABEL org.opencontainers.image.source="https://github.com/dandi-compute/dandi-compute-core"
LABEL org.opencontainers.image.description="Runtime environment for the DANDI Compute LFP extraction pipeline (dandi_compute_code.lfp_pipeline)."

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python3 -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

COPY pyproject.toml README.md LICENSE.txt /tmp/build/
COPY src /tmp/build/src
RUN pip install /tmp/build /tmp/build/src/dandi_compute_code/lfp_pipeline/envs \
    && rm -rf /tmp/build

CMD ["python", "-c", "import dandi_compute_code.lfp_pipeline; print('DANDI Compute LFP pipeline runtime ready')"]
