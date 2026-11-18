"""Render docs/bq_cost_before_after.png from the measured job stats in docs/bq_cost_results.json.

    python warehouse/plot_cost.py [results.json] [output.png]

The numbers come from BigQuery job statistics written by warehouse/cost_demo.sh; nothing here
is typed in by hand.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import PathPatch  # noqa: E402
from matplotlib.path import Path as MplPath  # noqa: E402

# Reference palette (dataviz skill): light surface, ink, categorical slot 1, hairline grid.
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e8e7e3"
BAR = "#2a78d6"

DPI = 200
SCALE = DPI / 100  # spec values are CSS px at 1x; the PNG is rendered at 2x
BAR_PX = 24 * SCALE
RADIUS_PX = 4 * SCALE

LABELS = {
    "A": "A  Unpartitioned table, no filter",
    "B": "B  Unpartitioned table, same SQL as C",
    "C": "C  Partitioned + clustered (raw.events)",
}


def _rounded_bar(ax, y: float, width: float, height: float, rx: float, ry: float) -> None:
    """Horizontal bar: square at the baseline (x=0), rounded data-end."""
    rx = min(rx, width / 2)
    y0, y1 = y - height / 2, y + height / 2
    verts = [
        (0, y0),
        (width - rx, y0),
        (width, y0),
        (width, y0 + ry),
        (width, y1 - ry),
        (width, y1),
        (width - rx, y1),
        (0, y1),
        (0, y0),
    ]
    codes = [
        MplPath.MOVETO,
        MplPath.LINETO,
        MplPath.CURVE3,
        MplPath.CURVE3,
        MplPath.LINETO,
        MplPath.CURVE3,
        MplPath.CURVE3,
        MplPath.LINETO,
        MplPath.CLOSEPOLY,
    ]
    ax.add_patch(PathPatch(MplPath(verts, codes), facecolor=BAR, edgecolor="none", zorder=3))


def render(results_path: Path, out_path: Path) -> None:
    data = json.loads(results_path.read_text())
    queries = {q["key"]: q for q in data["queries"]}
    order = ["A", "B", "C"]
    mb = {k: queries[k]["bytes_processed"] / 1e6 for k in order}
    billed_c = queries["C"]["bytes_billed"] / 1e6
    saving = 1 - mb["C"] / mb["B"]

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, ax = plt.subplots(figsize=(9, 3.6), dpi=DPI)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    xmax = max(mb.values()) * 1.32  # room for the tip labels
    ax.set_xlim(0, xmax)
    ax.set_ylim(len(order) - 0.5, -0.5)  # A on top
    fig.subplots_adjust(left=0.33, right=0.97, top=0.78, bottom=0.2)

    # Convert pixel sizes to data units once the axes geometry is fixed.
    fig.canvas.draw()
    bbox = ax.get_window_extent()
    y_per_px = len(order) / bbox.height
    x_per_px = xmax / bbox.width
    height, rx, ry = BAR_PX * y_per_px, RADIUS_PX * x_per_px, RADIUS_PX * y_per_px

    for i, key in enumerate(order):
        _rounded_bar(ax, i, mb[key], height, rx, ry)
        label = f"{mb[key]:,.1f} MB"
        if key == "C":
            label += f" processed  ·  {billed_c:,.1f} MB billed (10 MB minimum)"
        ax.text(
            mb[key] + xmax * 0.012,
            i,
            label,
            va="center",
            ha="left",
            color=TEXT_PRIMARY,
            fontsize=10,
            fontweight="bold" if key == "C" else "normal",
        )

    ax.set_yticks(range(len(order)), [LABELS[k] for k in order], color=TEXT_PRIMARY)
    ax.tick_params(axis="y", length=0, pad=8)
    ax.tick_params(axis="x", colors=TEXT_SECONDARY, length=0)
    ax.xaxis.set_major_formatter(lambda v, _: f"{v:,.0f} MB")
    ax.grid(axis="x", color=GRID, linewidth=1, zorder=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)

    days = data["days_loaded"]
    fig.text(
        0.02,
        0.93,
        f"Same SQL, {saving:.1%} fewer bytes processed with partitioning + clustering",
        fontsize=13,
        fontweight="bold",
        color=TEXT_PRIMARY,
    )
    fig.text(
        0.02,
        0.855,
        f"Query: top 10 starred repos on {data['cost_day']}  ·  table holds {len(days)} days "
        f"({days[0]} to {days[-1]})  ·  cache disabled",
        fontsize=9.5,
        color=TEXT_SECONDARY,
    )
    fig.text(
        0.02,
        0.04,
        "Source: BigQuery job statistics (totalBytesProcessed, totalBytesBilled); "
        "job IDs in docs/bq_cost_results.json",
        fontsize=7.5,
        color=TEXT_SECONDARY,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=SURFACE)
    plt.close(fig)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    args = sys.argv[1:]
    results = Path(args[0]) if args else Path("docs/bq_cost_results.json")
    output = Path(args[1]) if len(args) > 1 else Path("docs/bq_cost_before_after.png")
    render(results, output)
