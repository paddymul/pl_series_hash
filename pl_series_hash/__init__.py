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
        is_elementwise=False,
        returns_scalar=True
    )

@pl.api.register_expr_namespace("pl_series_hash")
class SeriesHash:
    def __init__(self, expr: pl.Expr):
        self._expr = expr


    def hash_xx(self) -> pl.Expr:
        return register_plugin_function(
    args=[self._expr],
    plugin_path=LIB,
    function_name="hash_series",
    is_elementwise=False,
    returns_scalar=True
    )

    def crash(self) -> pl.Expr:
        return register_plugin_function(
    args=[self._expr],
    plugin_path=LIB,
    function_name="crash_period",
    is_elementwise=False,
    returns_scalar=True
    )


def crash(expr: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="crash_period",
        is_elementwise=False,
        returns_scalar=True
    )

    




# @pl.api.register_expr_namespace("dist_arr")
# class DistancePairWiseArray:
#     def __init__(self, expr: pl.Expr):
#         self._expr = expr

    # def euclidean(self, other: IntoExpr) -> pl.Expr:
    #     """Returns euclidean distance between two vectors"""
    #     return register_plugin_function(
    #         plugin_path=Path(__file__).parent,
    #         args=[self._expr, other],
    #         function_name="euclidean_arr",
    #         is_elementwise=True,
    #     )


