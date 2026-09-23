DANDI Compute (Core)
====================

Orchestration code for running processing pipelines over assets on the
`DANDI Archive <https://dandiarchive.org>`_. It forms one self-describing *job capsule*
per asset and pipeline, uploads it to a Dandiset, runs it on a SLURM cluster, and
tracks every capsule through its lifecycle using nothing but the archive's own metadata.

New here? Start with the :doc:`overview`, then read :doc:`setup` to get it running.

.. toctree::
   :maxdepth: 2
   :caption: Getting started

   overview
   setup

.. toctree::
   :maxdepth: 2
   :caption: Concepts

   job_capsules
   data_model

.. toctree::
   :maxdepth: 2
   :caption: Operations

   infrastructure
   dispatch

.. toctree::
   :maxdepth: 2
   :caption: Development

   contributing
   schemas

.. toctree::
   :maxdepth: 1
   :caption: Reference

   api/index
