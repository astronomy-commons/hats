import importlib
import os
import platform
import struct
import subprocess
import sys


def _get_sys_info() -> dict[str, str]:
    uname_result = platform.uname()
    return {
        "python": platform.python_version(),
        "python-bits": str(struct.calcsize("P") * 8),
        "OS": uname_result.system,
        "OS-release": uname_result.release,
        "Version": uname_result.version,
        "machine": uname_result.machine,
        "processor": uname_result.processor,
        "byteorder": sys.byteorder,
        "LC_ALL": os.environ.get("LC_ALL") or "",
        "LANG": os.environ.get("LANG") or "",
    }


def _get_dependency_info(deps) -> dict[str, str]:
    if deps is None or len(deps) == 0:
        deps = [
            "hats",
            "nested-pandas",
            "pandas",
            "numpy",
            "pyarrow",
            "fsspec",
        ]

    result: dict[str, str] = {}
    for modname in deps:
        try:
            result[modname] = importlib.metadata.version(modname)
        except Exception:  # pylint: disable=broad-exception-caught # pragma: no cover
            result[modname] = "N/A"
    return result


def _get_package_info(package_name):
    """For the hats project (or other primary dependency),
    try to find if we're currently in a branch, and name it."""
    installed_version = None
    try:
        installed_version = importlib.metadata.version(package_name)
    except Exception:  # pylint: disable=broad-exception-caught # pragma: no cover
        pass
    try:
        # Runs the git command to return the current branch name
        branch = (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.STDOUT)
            .decode("utf-8")
            .strip()
        )
        if installed_version is None:
            return branch
        return f"{installed_version} ({branch})"
    except subprocess.CalledProcessError:
        return "N/A"


def print_versions(package_name, extra_deps):
    """Print runtime versions and system info, useful for bug reports."""
    sys_info = _get_sys_info()
    deps = _get_dependency_info(extra_deps)

    maxlen = max(len(x) for x in deps) + 1
    print("\n--------      SYSTEM INFO      --------")
    for k, v in sys_info.items():
        print(f"{k:<{maxlen}}: {v}")
    print("--------   INSTALLED VERSIONS   --------")
    print(f"{package_name:<{maxlen}}: {_get_package_info(package_name=package_name)}")
    for k, v in deps.items():
        print(f"{k:<{maxlen}}: {v}")


def show_versions():
    """Print runtime versions and system info, useful for bug reports."""
    print_versions("hats",
        [
            "nested-pandas",
            "pandas",
            "numpy",
            "pyarrow",
            "fsspec",
        ]
    )
