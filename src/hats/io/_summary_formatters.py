import numpy as np


def _fmt_count_percent(n: int, total: int) -> str:
    if n == 0:
        return "0"
    percent = round(n / total * 100, 2)
    if percent < 0.01:
        return f"{n:,} (<0.01%)"
    return f"{n:,} ({percent}%)"


def _hard_truncate(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    return s[: limit - 1] + "…"


def _format_example_value(
    value, *, float_precision: int = 4, soft_limit: int = 50, hard_limit: int = 70
) -> str:
    """Format an example value for display in a summary table.

    Floats are rounded to a limited number of significant figures.
    Lists are shown with as many items as fit within ``soft_limit``
    characters (always at least one), with a ``(N total)`` suffix when
    truncated. Any resulting string longer than ``hard_limit`` is
    truncated with ``…``.
    """
    if value is None:
        return "*NULL*"

    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return "*NaN*"
        if np.isinf(value):
            return "-∞" if value < 0 else "∞"
        return f"{value:.{float_precision}g}"

    if isinstance(value, (list, tuple, np.ndarray)):
        items = list(value)
        if len(items) == 0:
            return "[]"
        fmt_kwargs = {"float_precision": float_precision, "soft_limit": soft_limit, "hard_limit": hard_limit}
        suffix = f", … ({len(items)} total)]"
        parts = [_format_example_value(items[0], **fmt_kwargs)]
        for item in items[1:]:
            candidate = _format_example_value(item, **fmt_kwargs)
            preview = "[" + ", ".join(parts + [candidate]) + suffix
            if len(preview) > soft_limit:
                break
            parts.append(candidate)
        if len(parts) < len(items):
            result = "[" + ", ".join(parts) + suffix
        else:
            result = "[" + ", ".join(parts) + "]"
    else:
        result = str(value)

    return _hard_truncate(result, hard_limit)
