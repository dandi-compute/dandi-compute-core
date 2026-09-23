import beartype

_DIRECTION_TO_SPIKEINTERFACE = {"causal": "forward", "zero-phase": "forward-backward"}


@beartype.beartype
def resolve_filter_kwargs(parameters: dict, /) -> dict:
    """
    Translate the filter parameters into keyword arguments for ``spikeinterface.bandpass_filter``.

    Parameters
    ----------
    parameters : dict
        The validated LFP parameters.

    Returns
    -------
    dict
        Keyword arguments for ``spikeinterface.bandpass_filter``.
    """
    freq_min, freq_max = parameters["filter_band"]
    filter_kwargs = {
        "freq_min": freq_min,
        "freq_max": freq_max,
        "filter_order": parameters["filter_order"],
        "ftype": parameters["filter_family"],
        "direction": _DIRECTION_TO_SPIKEINTERFACE[parameters["filter_direction"]],
    }
    return filter_kwargs


@beartype.beartype
def resolve_reference_spec(parameters: dict, /) -> dict:
    """
    Translate the reference scheme into a specification for ``spikeinterface.common_reference``.

    The ``apply`` flag reports whether re-referencing should happen at all. When
    it is ``True``, ``operator`` is the median operator and ``per_shank`` selects
    between a global reference and one computed within each shank group.

    Parameters
    ----------
    parameters : dict
        The validated LFP parameters.

    Returns
    -------
    dict
        A specification with ``apply``, ``operator``, and ``per_shank`` keys.
    """
    reference_scheme = parameters["reference_scheme"]
    if reference_scheme == "none":
        reference_spec = {"apply": False, "operator": None, "per_shank": False}
    elif reference_scheme == "CMR":
        reference_spec = {"apply": True, "operator": "median", "per_shank": False}
    else:
        reference_spec = {"apply": True, "operator": "median", "per_shank": True}
    return reference_spec
