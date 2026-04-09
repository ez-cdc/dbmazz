"""Interactive prompts wrapping questionary with EZ-CDC brand colors.

All prompts exit cleanly on Ctrl+C (returning None) instead of raising
KeyboardInterrupt, so the caller can handle cancellation uniformly.
"""

from __future__ import annotations

from typing import Optional

import questionary
from questionary import Style

from .theme import PRIMARY_400, GRAY_400, GRAY_500, SUCCESS


# Questionary style matching the EZ-CDC brand. Colors are taken directly from
# the rich theme so the prompts visually match the rest of the CLI output.
EZ_CDC_PROMPT_STYLE = Style([
    ("qmark",       f"fg:{PRIMARY_400} bold"),   # the ? marker
    ("question",    "bold"),                      # question text
    ("answer",      f"fg:{SUCCESS} bold"),        # user's answer
    ("pointer",     f"fg:{PRIMARY_400} bold"),    # ❯ indicator
    ("highlighted", f"fg:{PRIMARY_400} bold"),    # currently-hovered choice
    ("selected",    f"fg:{SUCCESS}"),             # selected item
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
    questionary_choices = [
        questionary.Choice(
            title=_format_choice(c),
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
            use_indicator=True,
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


def _format_choice(choice: dict) -> str:
    """Format a choice for display: 'name  description'."""
    name = choice["name"]
    description = choice.get("description", "")
    if description:
        # Pad name to align descriptions in a column.
        return f"{name:<14}{description}"
    return name
