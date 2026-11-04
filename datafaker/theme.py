"""Entrypoint for the datafaker package."""
from dataclasses import dataclass
from enum import Enum

import colorama

colorama.just_fix_windows_console()


@dataclass
class Theme:  # pylint: disable=too-many-instance-attributes
    """A colour theme for DataFaker terminal output."""

    prompt: str
    column: str
    data: str
    function: str
    query: str
    line: str
    reset: str
    # 'propose' table/recommendation highlighting: `recommend` marks the
    # single recommended row, `near_miss` the other Pareto front 1 rows
    # (non-dominated alternatives worth a second look) - kept distinct from
    # each other and from the colours above so recommendation status reads
    # at a glance instead of blending into the rest of the table.
    recommend: str
    near_miss: str


class ThemeEntry(str, Enum):
    """Themes available in the ``--theme`` option."""

    NONE = "none"
    DARK = "dark"
    LIGHT = "light"


THEME: dict[str, Theme] = {
    ThemeEntry.NONE: Theme("", "", "", "", "", "", "", "", ""),
    ThemeEntry.DARK: Theme(
        prompt=colorama.Fore.CYAN + colorama.Style.NORMAL,  # type: ignore
        column=colorama.Fore.GREEN + colorama.Style.NORMAL,  # type: ignore
        data=colorama.Fore.YELLOW + colorama.Style.NORMAL,  # type: ignore
        function=colorama.Fore.MAGENTA + colorama.Style.NORMAL,  # type: ignore
        query=colorama.Fore.GREEN + colorama.Style.NORMAL,  # type: ignore
        line=colorama.Fore.WHITE + colorama.Style.DIM,  # type: ignore
        reset=colorama.Style.RESET_ALL,  # type: ignore
        recommend=colorama.Fore.GREEN + colorama.Style.BRIGHT,  # type: ignore
        near_miss=colorama.Fore.BLUE + colorama.Style.NORMAL,  # type: ignore
    ),
    ThemeEntry.LIGHT: Theme(
        prompt=colorama.Fore.BLUE,  # type: ignore
        column=colorama.Fore.GREEN,  # type: ignore
        data=colorama.Fore.BLACK,  # type: ignore
        function=colorama.Fore.MAGENTA,  # type: ignore
        query=colorama.Fore.MAGENTA,  # type: ignore
        line=colorama.Fore.LIGHTBLACK_EX,  # type: ignore
        reset=colorama.Style.RESET_ALL,  # type: ignore
        recommend=colorama.Fore.GREEN + colorama.Style.BRIGHT,  # type: ignore
        near_miss=colorama.Fore.CYAN,  # type: ignore
    ),
}


theme_active = THEME[ThemeEntry.NONE]


def set_active_theme(te: ThemeEntry):
    """Set the active theme by key."""
    global theme_active  # pylint: disable=global-statement
    theme_active = THEME[te]


def get_active_theme() -> Theme:
    """Get the active theme."""
    return theme_active
