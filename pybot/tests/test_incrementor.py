from __future__ import annotations

import os

import pytest

from dickgrowerbot.incrementor import _base_increment


@pytest.mark.parametrize("min_value,max_value", [(-5, 10), (1, 10), (-10, -1)])
def test_base_increment_in_range(min_value: int, max_value: int) -> None:
    for _ in range(200):
        v = _base_increment(min_value, max_value, sign_ratio=0.5)
        assert min_value <= v <= max_value


def test_base_increment_positive_only_is_positive() -> None:
    for _ in range(200):
        assert _base_increment(1, 10, sign_ratio=0.0) > 0


def test_base_increment_respects_sign_ratio_edges() -> None:
    # With ratio ~100%, negative should never appear (for mixed range).
    for _ in range(200):
        assert _base_increment(-5, 10, sign_ratio=1.0) > 0
    # With ratio ~0%, positive should never appear (for mixed range).
    for _ in range(200):
        assert _base_increment(-5, 10, sign_ratio=0.0) < 0

