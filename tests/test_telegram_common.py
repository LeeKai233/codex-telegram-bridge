from __future__ import annotations

import pytest
from telegram import InlineKeyboardButton

from codex_telegram_bridge.telegram_common import balanced_button_rows


@pytest.mark.parametrize(
    ("count", "row_lengths"),
    (
        (1, [1]),
        (3, [3]),
        (4, [2, 2]),
        (5, [3, 2]),
        (7, [3, 2, 2]),
        (10, [3, 3, 2, 2]),
    ),
)
def test_balanced_button_rows_avoids_singleton_trailing_rows(
    count: int, row_lengths: list[int]
) -> None:
    buttons = [InlineKeyboardButton(str(index), callback_data=str(index)) for index in range(count)]

    rows = balanced_button_rows(buttons)

    assert [len(row) for row in rows] == row_lengths
    assert [button.text for row in rows for button in row] == [str(index) for index in range(count)]


def test_balanced_button_rows_preserves_single_column_rows() -> None:
    buttons = [InlineKeyboardButton(str(index), callback_data=str(index)) for index in range(3)]

    rows = balanced_button_rows(buttons, columns=1)

    assert [len(row) for row in rows] == [1, 1, 1]
