"""Banner rendering for the EZ-CDC CLI.

Two banner variants:

  - Banner A (ANSI Shadow): large block-style ASCII art with a vertical
    gradient. Used in the main menu (`ez-cdc` with no subcommand) and in
    `ez-cdc quickstart`. Needs at least 56 columns; falls back to banner D
    when the terminal is narrower.

  - Banner D (gradient bar): compact two-line gradient bar with centered
    text. Used in all other subcommands (`verify`, `load`, `up`, `down`,
    `logs`, `status`). Fits in any terminal >= 40 columns.

Gradients are interpolated between primary.600 (#2563EB) and primary.400
(#60A5FA), matching the brand logo gradient.
"""

from __future__ import annotations

import shutil

from rich.console import Group, RenderableType
from rich.text import Text

from .theme import PRIMARY_400, PRIMARY_600, GRAY_400, interpolate_hex


# ── Banner A: ANSI Shadow block art ──────────────────────────────────────────

# 6 lines, 44 columns wide. "EZ-CDC" rendered in ANSI Shadow figlet font.
_BANNER_A_LINES = [
    "███████╗███████╗     ██████╗██████╗  ██████╗",
    "██╔════╝╚══███╔╝    ██╔════╝██╔══██╗██╔════╝",
    "█████╗    ███╔╝     ██║     ██║  ██║██║     ",
    "██╔══╝   ███╔╝      ██║     ██║  ██║██║     ",
    "███████╗███████╗    ╚██████╗██████╔╝╚██████╗",
    "╚══════╝╚══════╝     ╚═════╝╚═════╝  ╚═════╝",
]

_BANNER_A_MIN_WIDTH = 56  # enough padding to look balanced
_BANNER_A_TAGLINE = "Real-time CDC, radically simplified"


def render_banner_a() -> RenderableType:
    """Render the ANSI Shadow banner with a vertical gradient.

    Returns a rich Renderable (Group of Text lines). Each line of the ASCII
    art gets its own gradient color, from primary.600 at the top to
    primary.400 at the bottom.

    If the terminal is narrower than 56 columns, falls back to banner D.
    """
    width = shutil.get_terminal_size((80, 24)).columns
    if width < _BANNER_A_MIN_WIDTH:
        return render_banner_d()

    colors = interpolate_hex(PRIMARY_600, PRIMARY_400, len(_BANNER_A_LINES))

    lines: list[RenderableType] = [Text("")]  # top padding
    for line, color in zip(_BANNER_A_LINES, colors):
        t = Text(line, style=f"bold {color}")
        t.pad_left(4)
        lines.append(t)

    lines.append(Text(""))  # space before tagline
    tagline = Text(_BANNER_A_TAGLINE, style=GRAY_400, justify="center")
    # Center tagline under the ASCII art (the art is ~44 cols, padded left 4,
    # so centering around col 4 + 22 = 26). Use pad_left calc instead of
    # justify="center" so we center *under the art*, not the whole terminal.
    tagline_padding = 4 + (44 - len(_BANNER_A_TAGLINE)) // 2
    if tagline_padding > 0:
        tagline = Text(" " * tagline_padding + _BANNER_A_TAGLINE, style=GRAY_400)
    lines.append(tagline)

    lines.append(Text(""))  # bottom padding
    return Group(*lines)


# ── Banner D: gradient bar ───────────────────────────────────────────────────

_BANNER_D_BAR_WIDTH = 52
_BANNER_D_TEXT = "EZ-CDC  •  Real-time CDC, radically simplified"
_BANNER_D_PAD_LEFT = 4


def _make_gradient_bar(char: str, width: int) -> Text:
    """Build a horizontal gradient bar of `width` chars using the given char.

    Each character is styled with an interpolated color between primary.600
    and primary.400, producing a smooth left-to-right gradient.
    """
    colors = interpolate_hex(PRIMARY_600, PRIMARY_400, width)
    t = Text()
    t.append(" " * _BANNER_D_PAD_LEFT)
    for color in colors:
        t.append(char, style=color)
    return t


def render_banner_d() -> RenderableType:
    """Render the compact gradient bar banner.

    Layout:
        (blank)
        ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
                EZ-CDC  •  Real-time CDC, radically simplified
        ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
        (blank)

    The bars use ▄ (upper half block) above the text and ▀ (lower half block)
    below, creating the visual effect of a solid bar hugging the text.
    """
    width = shutil.get_terminal_size((80, 24)).columns
    bar_width = min(_BANNER_D_BAR_WIDTH, max(40, width - _BANNER_D_PAD_LEFT - 2))

    top_bar = _make_gradient_bar("▄", bar_width)
    bot_bar = _make_gradient_bar("▀", bar_width)

    # Build text line: "EZ-CDC" in brand + "•" in dim + tagline in tagline
    text_line = Text()
    text_padding = _BANNER_D_PAD_LEFT + max(0, (bar_width - len(_BANNER_D_TEXT)) // 2)
    text_line.append(" " * text_padding)
    text_line.append("EZ-CDC", style=f"bold {PRIMARY_400}")
    text_line.append("  •  ", style=GRAY_400)
    text_line.append("Real-time CDC, radically simplified", style=GRAY_400)

    return Group(
        Text(""),
        top_bar,
        Text(""),
        text_line,
        Text(""),
        bot_bar,
        Text(""),
    )
