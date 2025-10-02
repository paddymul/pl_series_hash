from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from polars.plugins import register_plugin_function

from pl_series_hash._internal import __version__ as __version__

if TYPE_CHECKING:
    from pl_series_hash.typing import IntoExprColumn

LIB = Path(__file__).parent


def hash_xx(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="hash_series",
        is_elementwise=True,
    )
