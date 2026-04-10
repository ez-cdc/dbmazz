"""Interactive prompts wrapping questionary with EZ-CDC brand colors.

All prompts exit cleanly on Ctrl+C (returning None) instead of raising
KeyboardInterrupt, so the caller can handle cancellation uniformly.
"""

from __future__ import annotations

from typing import Optional

import questionary
from questionary import Style

from .theme import PRIMARY_400, GRAY_400, GRAY_500


# Questionary style matching the EZ-CDC brand. Colors are taken directly from
# the rich theme so the prompts visually match the rest of the CLI output.
#
# All "chosen" styles use the brand blue (PRIMARY_400) — using green (SUCCESS)
# is reserved for pass results in the verify output. Mixing them confuses the
# user into thinking the default choice is already "correct".
#
# IMPORTANT — `noreverse` and `bg:default` on `highlighted`:
#   prompt_toolkit applies a reverse-video (inverted background) effect by
#   default to the currently-hovered choice in a select. That produces a
#   solid blue block under the first option, which looks like the option
#   is "pre-selected". We don't want that — we just want the text in brand
#   color and the `❯` pointer to indicate position.
#   `noreverse` cancels the reverse effect and `bg:default` keeps the
#   terminal's normal background (no fill).
EZ_CDC_PROMPT_STYLE = Style([
    ("qmark",       f"fg:{PRIMARY_400} bold"),   # the ? marker
    ("question",    "bold"),                      # question text
    ("answer",      f"fg:{PRIMARY_400} bold"),    # user's answer (after confirm)
    ("pointer",     f"fg:{PRIMARY_400} bold"),    # ❯ indicator
    ("highlighted", f"fg:{PRIMARY_400} noreverse bg:default"),
    ("selected",    f"fg:{PRIMARY_400} noreverse bg:default"),  # multiselect "checked"
    ("separator",   f"fg:{GRAY_500}"),            # separator lines
    ("instruction", f"fg:{GRAY_400}"),            # "(use ↑↓ to navigate)"
    ("text",        ""),                          # plain text
    ("disabled",    f"fg:{GRAY_500} italic"),     # disabled items
])


def select(
    message: str,
    choices: list[dict],
    default: Optional[str] = None,
) -> Optional[str]:
    """Show a select menu and return the user's choice value.

    Args:
        message: the question to show.
        choices: list of {"name": str, "value": str, "description": str (optional)}.
                 `name` is what's displayed, `value` is what's returned.
        default: the value of the default choice.

    Returns:
        The `value` of the selected choice, or None if the user cancelled.
    """
    # Compute the column width for the name based on the longest name in the
    # current set, so descriptions always line up no matter how long the
    # names are. Min width 14 to keep short lists from looking cramped.
    max_name = max((len(c["name"]) for c in choices), default=0)
    name_width = max(max_name, 14)

    questionary_choices = [
        questionary.Choice(
            title=_format_choice(c, name_width),
            value=c["value"],
        )
        for c in choices
    ]

    try:
        answer = questionary.select(
            message,
            choices=questionary_choices,
            default=default,
            style=EZ_CDC_PROMPT_STYLE,
            qmark="?",
            pointer="❯",          # brand pointer (overrides questionary's default »)
            use_indicator=False,  # hide the radio-button ●/○ — pointer alone is enough
            use_shortcuts=False,
        ).ask()
    except KeyboardInterrupt:
        return None

    return answer


def confirm(message: str, default: bool = True) -> Optional[bool]:
    """Show a yes/no prompt. Returns None if cancelled."""
    try:
        return questionary.confirm(
            message,
            default=default,
            style=EZ_CDC_PROMPT_STYLE,
            qmark="?",
        ).ask()
    except KeyboardInterrupt:
        return None


def text(message: str, default: str = "") -> Optional[str]:
    """Show a text input prompt. Returns None if cancelled."""
    try:
        return questionary.text(
            message,
            default=default,
            style=EZ_CDC_PROMPT_STYLE,
            qmark="?",
        ).ask()
    except KeyboardInterrupt:
        return None


def password(message: str) -> Optional[str]:
    """Show a password input prompt (characters hidden). Returns None if cancelled."""
    try:
        return questionary.password(
            message,
            style=EZ_CDC_PROMPT_STYLE,
            qmark="?",
        ).ask()
    except KeyboardInterrupt:
        return None


def _format_choice(choice: dict, name_width: int = 14) -> str:
    """Format a choice for display: 'name   description'.

    The name is padded to `name_width` (computed by select() based on the
    longest name in the choice set) and a fixed 3-space gutter is added
    before the description so the two columns are visually distinct even
    when the name happens to fill the column exactly.
    """
    name = choice["name"]
    description = choice.get("description", "")
    if description:
        return f"{name:<{name_width}}   {description}"
    return name
