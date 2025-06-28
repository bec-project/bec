from dataclasses import dataclass


@dataclass(frozen=True)
class _ANSWER_KEYS:
    VERSION: str = "_commit"
    WIDGETS: str = "widget_plugins_input"


ANSWER_KEYS = _ANSWER_KEYS()
