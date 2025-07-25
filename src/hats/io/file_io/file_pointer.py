from __future__ import annotations

from pathlib import Path

from upath import UPath

BLOCK_SIZE = 32 * 1024


def get_upath(path: str | Path | UPath) -> UPath:
    """Returns a file pointer from a path string"""
    if not path:
        return None
    if isinstance(path, UPath):
        return path
    return get_upath_for_protocol(path)


def get_upath_for_protocol(path: str | Path) -> UPath:
    """Create UPath with protocol-specific configurations.

    If we access pointers on S3 and credentials are not found we assume
    an anonymous access, i.e., that the bucket is public.
    """
    upath = UPath(path)
    if upath.protocol == "s3":
        upath = UPath(path, anon=True, default_block_size=BLOCK_SIZE)
    if upath.protocol in ("http", "https"):
        upath = UPath(path, block_size=BLOCK_SIZE)
    return upath


def append_paths_to_pointer(pointer: str | Path | UPath, *paths: str) -> UPath:
    """Append directories and/or a file name to a specified file pointer.

    Args:
        pointer: `FilePointer` object to add path to
        paths: any number of directory names optionally followed by a file name to append to the
            pointer

    Returns:
        New file pointer to path given by joining given pointer and path names
    """
    pointer = get_upath(pointer)
    return pointer.joinpath(*paths)


def does_file_or_directory_exist(pointer: str | Path | UPath) -> bool:
    """Checks if a file or directory exists for a given file pointer

    Args:
        pointer: File Pointer to check if file or directory exists at

    Returns:
        True if file or directory at `pointer` exists, False if not
    """
    pointer = get_upath(pointer)
    return pointer.exists()


def is_regular_file(pointer: str | Path | UPath) -> bool:
    """Checks if a regular file (NOT a directory) exists for a given file pointer.

    Args:
        pointer: File Pointer to check if a regular file

    Returns:
        True if regular file at `pointer` exists, False if not or is a directory
    """
    pointer = get_upath(pointer)
    return pointer.is_file()


def find_files_matching_path(pointer: str | Path | UPath, *paths: str) -> list[UPath]:
    """Find files or directories matching the provided path parts.

    Args:
        pointer: base File Pointer in which to find contents
        paths: any number of directory names optionally followed by a file name.
            directory or file names may be replaced with `*` as a matcher.
    Returns:
        New file pointers to files found matching the path
    """
    pointer = get_upath(pointer)

    if len(paths) == 0:
        return [pointer]

    matcher = pointer.fs.sep.join(paths)
    contents = []
    for child in pointer.rglob(matcher):
        contents.append(child)

    if len(contents) == 0:
        return []

    contents.sort()
    return contents


def directory_has_contents(pointer: str | Path | UPath) -> bool:
    """Checks if a directory already has some contents (any files or subdirectories)

    Args:
        pointer: File Pointer to check for existing contents

    Returns:
        True if there are any files or subdirectories below this directory.
    """
    pointer = get_upath(pointer)
    return len(find_files_matching_path(pointer, "*")) > 0
