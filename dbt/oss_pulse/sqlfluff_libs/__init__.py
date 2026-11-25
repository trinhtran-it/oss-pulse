"""dbt context for sqlfluff's Jinja templater (lint only, never imported by dbt).

sqlfluff renders the models without a dbt connection. Its dbt builtins cover ref/source/config,
but `var` returns a placeholder and `modules` / `run_query` / `execute` are missing. These stubs
render every model the way a first (full) build would, using the defaults in dbt_project.yml.
"""

import datetime as _datetime
import types as _types

_VARS = {
    "history_start_date": "2026-10-01",
    "run_date": None,
    "lookback_days": 1,
    "date_spine_end": "2028-01-01",
}

modules = _types.SimpleNamespace(datetime=_datetime)
execute = False  # parse-time: skips run_query branches, like `dbt parse`


def var(name, default=None):
    return _VARS.get(name, default)


def run_query(sql):  # never called while execute is False
    return None


def _return(value):  # dbt's return(): the stub renders the value instead of returning it
    return ""


globals()["return"] = _return  # `return` is a Python keyword, so it can't be a def name


def _date_spine(datepart, start_date, end_date):
    return f"select cast({start_date} as datetime) as date_day"


def _generate_surrogate_key(field_list):
    return "to_hex(md5(" + " || '-' || ".join(f"cast({f} as string)" for f in field_list) + "))"


dbt_utils = _types.SimpleNamespace(
    date_spine=_date_spine, generate_surrogate_key=_generate_surrogate_key
)
