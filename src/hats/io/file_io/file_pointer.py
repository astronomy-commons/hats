from __future__ import annotations

from importlib.metadata import version
from pathlib import Path
from urllib.parse import urlparse

from upath import UPath

BLOCK_SIZE = 32 * 1024
_VIZCAT_HOST = "vizcat.cds.unistra.fr"


def get_upath(path: str | Path | UPath) -> UPath:
    """Returns a UPath file pointer from a path string or other path-like type.

    Parameters
    ----------
    path: str | Path | UPath
        base file path to be normalized to UPath

    Returns
    -------
    UPath
        Instance of UPath.
    """
    if not path:
        return None
    if isinstance(path, UPath):
        return path
    return get_upath_for_protocol(path)


def get_upath_for_protocol(path: str | Path) -> UPath:
    """Create UPath with protocol-specific configurations.

    If we access pointers on S3 and credentials are not found we assume
    an anonymous access, i.e., that the bucket is public.

    Parameters
    ----------
    path: str | Path | UPath
        base file path to be normalized to UPath

    Returns
    -------
    UPath
        Instance of UPath.
    """
    upath = UPath(path)
    if upath.protocol == "s3":
        upath = UPath(path, anon=True, default_block_size=BLOCK_SIZE)
    if upath.protocol in ("http", "https"):
        kwargs = {
            "block_size": BLOCK_SIZE,
            "client_kwargs": {"headers": {"User-Agent": f"hats/{version('hats')}"}},
        }

        parts = urlparse(path)
        if parts.netloc == _VIZCAT_HOST:
            kwargs["cache_options"] = {"parquet_precache_all_bytes": True}
        upath = UPath(path, **kwargs)
    return upath


def filter_query_params_for_url(url: str, query_params: dict) -> dict:
    """Return a copy of ``query_params`` with any keys unsupported by the target
    host removed.

    Currently the only special case is VizCaT (``vizcat.cds.unistra.fr``), which
    does not support server-side ``filters``.

    Parameters
    ----------
    url : str
        Full URL of the catalog resource (used only to inspect the host).
    query_params : dict
        Query parameters dict as produced by
        :func:`~hats.io.paths.dict_to_query_urlparams`.

    Returns
    -------
    dict
        Filtered copy of ``query_params``.
    """
    if urlparse(url).netloc == _VIZCAT_HOST:
        return {k: v for k, v in query_params.items() if k != "filters"}
    return query_params


def directory_has_contents(pointer: str | Path | UPath) -> bool:
    """Checks if a directory already has some contents (any files or subdirectories)

    Parameters
    ----------
    pointer : str | Path | UPath
        File Pointer to check for existing contents

    Returns
    -------
    bool
        True if there are any files or subdirectories below this directory.
    """
    pointer = get_upath(pointer)

    return next(pointer.rglob("*"), None) is not None
