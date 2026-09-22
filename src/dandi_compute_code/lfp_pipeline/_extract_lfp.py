import logging

import numpy
import spikeinterface.full

from ._load_parameters import load_lfp_parameters
from ._resolve import resolve_filter_kwargs, resolve_reference_spec

_log = logging.getLogger(__name__)


def _shank_groups(recording, /) -> list[list]:
    """Group channel ids by their SpikeInterface channel group (one group per shank)."""
    channel_groups = recording.get_channel_groups()
    channel_ids = recording.channel_ids
    grouped: dict = {}
    for channel_id, channel_group in zip(channel_ids, channel_groups):
        grouped.setdefault(channel_group, []).append(channel_id)
    ordered_groups = [grouped[key] for key in sorted(grouped)]
    return ordered_groups


def _decimate_channel_ids(recording, *, factor: int) -> list:
    """Select every ``factor``-th channel id ordered by increasing depth."""
    locations = recording.get_channel_locations()
    channel_ids = recording.channel_ids
    depth_order = numpy.argsort(locations[:, 1])
    kept_channel_ids = channel_ids[depth_order][::factor].tolist()
    return kept_channel_ids


def extract_lfp(*, recording, parameters: dict | None = None):
    """
    Extract an LFP recording from a raw SpikeInterface recording.

    Adapted from the vandermeerlab neuropixels hackathon LFP extraction script.
    The steps are applied in order: bad channel detection and removal, bandpass
    filtering, re-referencing, resampling, and spatial decimation. Any step
    whose parameter selects "none" or an identity value is skipped.

    Parameters
    ----------
    recording
        The raw SpikeInterface recording to process.
    parameters : dict, optional
        The validated LFP parameters. Defaults to the registered ``default``
        parameters loaded via :func:`.load_lfp_parameters`.

    Returns
    -------
    The processed LFP SpikeInterface recording.
    """
    parameters = parameters if parameters is not None else load_lfp_parameters()

    processed_recording = recording
    if parameters["bad_channel_method"] != "none":
        bad_channel_ids, _ = spikeinterface.full.detect_bad_channels(
            processed_recording, method=parameters["bad_channel_method"]
        )
        _log.info(f"Removing {len(bad_channel_ids)} bad channels detected via '{parameters['bad_channel_method']}'.")
        processed_recording = processed_recording.remove_channels(bad_channel_ids)

    filter_kwargs = resolve_filter_kwargs(parameters)
    _log.info(f"Applying {parameters['filter_family']} bandpass filter over {tuple(parameters['filter_band'])} Hz.")
    processed_recording = spikeinterface.full.bandpass_filter(processed_recording, **filter_kwargs)

    reference_spec = resolve_reference_spec(parameters)
    if reference_spec["apply"]:
        groups = _shank_groups(processed_recording) if reference_spec["per_shank"] else None
        _log.info(f"Applying '{parameters['reference_scheme']}' reference.")
        processed_recording = spikeinterface.full.common_reference(
            processed_recording, reference="global", operator=reference_spec["operator"], groups=groups
        )

    _log.info(f"Resampling to {parameters['resample_target_fs']} Hz.")
    processed_recording = spikeinterface.full.resample(
        processed_recording, resample_rate=parameters["resample_target_fs"]
    )

    if parameters["spatial_factor"] != 1:
        kept_channel_ids = _decimate_channel_ids(processed_recording, factor=parameters["spatial_factor"])
        _log.info(f"Spatially decimating by {parameters['spatial_factor']} to {len(kept_channel_ids)} channels.")
        processed_recording = processed_recording.channel_slice(channel_ids=kept_channel_ids)

    return processed_recording
