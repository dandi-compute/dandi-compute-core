import logging
import pathlib

import beartype
import neuroconv.tools.spikeinterface
import pynwb

from ._extract_lfp import extract_lfp

_log = logging.getLogger(__name__)


@beartype.beartype
def add_lfp_to_nwbfile(*, recording, nwbfile):
    """
    Add an LFP recording to an NWBFile as an ``ElectricalSeries`` under the processing group.

    The series is placed inside an ``LFP`` container within the ``ecephys``
    processing module, following the NWB best practice for processed LFP.

    Parameters
    ----------
    recording
        The LFP SpikeInterface recording to add.
    nwbfile : pynwb.NWBFile
        The in-memory NWBFile to augment.

    Returns
    -------
    pynwb.NWBFile
        The same NWBFile, with the LFP ``ElectricalSeries`` added.
    """
    neuroconv.tools.spikeinterface.add_recording_to_nwbfile(
        recording=recording, nwbfile=nwbfile, parent_container="processing/LFP"
    )
    return nwbfile


@beartype.beartype
def run_lfp_pipeline(
    *,
    recording,
    nwbfile,
    parameters: dict | None = None,
    nwbfile_path: str | pathlib.Path | None = None,
):
    """
    Run the LFP pipeline and produce an NWBFile containing the LFP under the processing group.

    Extracts LFP from the raw recording and adds the resulting
    ``ElectricalSeries`` to ``nwbfile`` inside the ``ecephys`` processing
    module. When ``nwbfile_path`` is provided the NWBFile is written to disk.

    Parameters
    ----------
    recording
        The raw SpikeInterface recording to process.
    nwbfile : pynwb.NWBFile
        The in-memory NWBFile to augment with the extracted LFP.
    parameters : dict, optional
        The validated LFP parameters. Defaults to the registered ``default``
        parameters.
    nwbfile_path : str or pathlib.Path, optional
        Where to write the resulting NWB file. When ``None`` the NWBFile is only
        augmented in memory and not written to disk.

    Returns
    -------
    pynwb.NWBFile
        The augmented NWBFile.
    """
    lfp_recording = extract_lfp(recording=recording, parameters=parameters)
    add_lfp_to_nwbfile(recording=lfp_recording, nwbfile=nwbfile)
    if nwbfile_path is not None:
        _log.info(f"Writing LFP NWB file to {nwbfile_path}.")
        with pynwb.NWBHDF5IO(path=str(nwbfile_path), mode="w") as io:
            io.write(nwbfile)
    return nwbfile
