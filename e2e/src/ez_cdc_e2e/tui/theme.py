"""EZ-CDC theme for rich — colors sourced from dev-workspace/brand/colors.json.

Primary palette is Tailwind Blue. The theme is dark-terminal friendly: the
"brand" color is primary.400 (#60A5FA) which reads well on both dark and light
backgrounds. The banner gradient anchors on primary.600 (#2563EB) and
primary.400 (#60A5FA), matching the logo gradient.
"""

from rich.theme import Theme

# ── Brand palette (from brand/colors.json) ────────────────────────────────────

# primary (Tailwind Blue)
PRIMARY_50  = "#EFF6FF"
PRIMARY_100 = "#DBEAFE"
PRIMARY_200 = "#BFDBFE"
PRIMARY_300 = "#93C5FD"
PRIMARY_400 = "#60A5FA"  # brand (dark-friendly)
PRIMARY_500 = "#3B82F6"  # info
PRIMARY_600 = "#2563EB"  # brand (light-friendly), gradient anchor
PRIMARY_700 = "#1D4ED8"  # gradient end
PRIMARY_800 = "#1E40AF"
PRIMARY_900 = "#1E3A8A"

# grays (Tailwind Slate)
GRAY_50  = "#F8FAFC"
GRAY_100 = "#F1F5F9"
GRAY_200 = "#E2E8F0"
GRAY_300 = "#CBD5E1"
GRAY_400 = "#94A3B8"  # tagline, muted
GRAY_500 = "#64748B"  # dim text
GRAY_600 = "#475569"  # very dim
GRAY_700 = "#334155"
GRAY_800 = "#1E293B"
GRAY_900 = "#0F172A"

# semantic
SUCCESS = "#10B981"
WARNING = "#F59E0B"
ERROR   = "#EF4444"
INFO    = "#3B82F6"

# ── Rich theme ────────────────────────────────────────────────────────────────

EZ_CDC_THEME = Theme({
    # brand
    "brand":           f"bold {PRIMARY_400}",
    "brand.gradient":  PRIMARY_600,
    "tagline":         GRAY_400,

    # semantic
    "success":         f"bold {SUCCESS}",
    "error":           f"bold {ERROR}",
    "warning":         f"bold {WARNING}",
    "info":            INFO,

    # text hierarchy
    "muted":           GRAY_500,
    "dim":             GRAY_600,

    # dashboard panels
    "panel.header":    f"bold {PRIMARY_400}",
    "panel.border":    GRAY_600,
    "metric.label":    GRAY_400,
    "metric.number":   "bold white",

    # verify output
    "step":            INFO,
    "pass":            f"bold {SUCCESS}",
    "fail":            f"bold {ERROR}",
    "skip":            f"bold {WARNING}",
    "check.id":        PRIMARY_400,
    "check.detail":    GRAY_400,

    # progress spinners
    "spinner":         PRIMARY_400,
    "spinner.done":    SUCCESS,
    "spinner.fail":    ERROR,

    # separators, borders
    "rule":            GRAY_600,
})


def interpolate_hex(start: str, end: str, steps: int) -> list[str]:
    """Linear interpolation between two hex colors, returning `steps` hex strings.

    Used for banner gradients. `start` and `end` are "#RRGGBB" strings.
    Returns a list of length `steps` where the first is `start` and the last is `end`.
    """
    if steps < 2:
        return [start]

    def hex_to_rgb(h: str) -> tuple[int, int, int]:
        h = h.lstrip("#")
        return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

    def rgb_to_hex(r: int, g: int, b: int) -> str:
        return f"#{r:02X}{g:02X}{b:02X}"

    r1, g1, b1 = hex_to_rgb(start)
    r2, g2, b2 = hex_to_rgb(end)
    out = []
    for i in range(steps):
        t = i / (steps - 1)
        r = round(r1 + (r2 - r1) * t)
        g = round(g1 + (g2 - g1) * t)
        b = round(b1 + (b2 - b1) * t)
        out.append(rgb_to_hex(r, g, b))
    return out
