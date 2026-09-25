"""Compact static figures (matplotlib, Agg).  Every figure has a CSV twin in tables/.

Style follows the repo data-viz rules: fixed categorical order (blue, orange, aqua), 2px
lines, >= 8px markers with a surface ring, hairline solid grid, text in ink colors, one
y-axis per panel, <= 3 series per panel with distinct marker shapes as secondary encoding.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
MARKERS = ["o", "s", "^"]


def _style(ax):
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8, linestyle="-")
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(AXIS)
    ax.tick_params(colors=MUTED, labelcolor=INK2, labelsize=8)
    ax.xaxis.label.set_color(INK2)
    ax.yaxis.label.set_color(INK2)
    ax.title.set_color(INK)


def _fig(w, h, ncols=1, nrows=1, **kw):
    fig, axes = plt.subplots(nrows, ncols, figsize=(w, h), facecolor=SURFACE, **kw)
    for ax in np.atleast_1d(axes).ravel():
        _style(ax)
    return fig, axes


def _save(fig, path: Path):
    fig.tight_layout()
    fig.savefig(path, dpi=90, facecolor=SURFACE)
    plt.close(fig)


def reliability_panels(rel: pd.DataFrame, models: list[str], labels: list[str], taus=(60, 30, 10, 2), path: Path | None = None):
    """Observed frequency vs mean forecast by decision time (z-grid, test period)."""
    fig, axes = _fig(10, 3.0, ncols=len(taus), sharey=True)
    for ax, tau in zip(np.atleast_1d(axes), taus):
        ax.plot([0, 1], [0, 1], color=AXIS, linewidth=1)
        for i, (m, lab) in enumerate(zip(models, labels)):
            d = rel[(rel.model == m) & (rel.tau_min == tau)]
            ax.plot(d.p_mean, d.y_mean, color=SERIES[i], linewidth=2, marker=MARKERS[i], markersize=5,
                    markeredgecolor=SURFACE, markeredgewidth=1, label=lab)
        ax.set_title(f"{tau} min before T", fontsize=9)
        ax.set_xlabel("mean forecast P(YES)", fontsize=8)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
    np.atleast_1d(axes)[0].set_ylabel("observed YES frequency", fontsize=8)
    np.atleast_1d(axes)[0].legend(fontsize=7, frameon=False, labelcolor=INK2, loc="upper left")
    _save(fig, path)


def tail_ratio(tail: pd.DataFrame, models: list[str], labels: list[str], split: str = "all", path: Path | None = None):
    """Realized / predicted tail-event frequency by model tail-probability bucket (log2 axis)."""
    fig, ax = _fig(6.5, 3.2)
    order = ["<0.5%", "0.5-1%", "1-2%", "2-5%", "5-10%", "10-20%"]
    x = np.arange(len(order))
    ax.axhline(1.0, color=AXIS, linewidth=1)
    w = 0.22
    for i, (m, lab) in enumerate(zip(models, labels)):
        d = tail[(tail.model == m) & (tail.split == split)].set_index("q_bucket").reindex(order)
        xi = x + (i - (len(models) - 1) / 2) * w
        ok = d.ratio_freq_to_p.notna().to_numpy()
        yerr = np.vstack([d.ratio_freq_to_p - d.ratio_lo, d.ratio_hi - d.ratio_freq_to_p])
        ax.errorbar(xi[ok], d.ratio_freq_to_p[ok], yerr=yerr[:, ok], fmt=MARKERS[i], color=SERIES[i], markersize=6,
                    markeredgecolor=SURFACE, elinewidth=1.5, capsize=0, label=lab)
    ax.set_yscale("log", base=2)
    ax.set_yticks([0.25, 0.5, 1, 2, 4])
    ax.set_yticklabels(["0.25", "0.5", "1", "2", "4"])
    ax.set_xticks(x)
    ax.set_xticklabels(order)
    ax.set_xlabel("model probability of the tail event (bucket)", fontsize=8)
    ax.set_ylabel("realized / predicted frequency", fontsize=8)
    ax.legend(fontsize=7, frameon=False, labelcolor=INK2, loc="upper right")
    _save(fig, path)


def seasonality(profile: pd.DataFrame, path: Path | None = None):
    """Sigma factor by ET hour, weekday vs weekend."""
    fig, ax = _fig(6.5, 3.0)
    for i, (kind, lab) in enumerate((("weekday", "weekday"), ("weekend", "weekend"))):
        d = profile[profile.day_type == kind].sort_values("hour_et")
        ax.plot(d.hour_et, d.factor, color=SERIES[i], linewidth=2, marker=MARKERS[i], markersize=4,
                markeredgecolor=SURFACE, markeredgewidth=1, label=lab)
    ax.axhline(1.0, color=AXIS, linewidth=1)
    ax.set_xticks(range(0, 24, 3))
    ax.set_xlabel("hour of day (New York time, bucket start)", fontsize=8)
    ax.set_ylabel("1-min return vol / weekly average", fontsize=8)
    ax.legend(fontsize=7, frameon=False, labelcolor=INK2)
    _save(fig, path)


def dll_by_tau(scores: pd.DataFrame, models: list[str], labels: list[str], path: Path | None = None):
    """Log-loss change vs the baseline by decision time with 95% day-block CIs."""
    fig, ax = _fig(6.5, 3.0)
    ax.axhline(0.0, color=AXIS, linewidth=1)
    for i, (m, lab) in enumerate(zip(models, labels)):
        d = scores[(scores.model == m) & (scores.group == "tau")].copy()
        d["tau"] = d.level.str.replace("m", "").astype(int)
        d = d.sort_values("tau", ascending=False)
        x = np.arange(len(d)) + (i - 1) * 0.15
        yerr = np.vstack([d.d_ll_vs_base - d.d_ll_lo, d.d_ll_hi - d.d_ll_vs_base]) * 1000
        ax.errorbar(x, d.d_ll_vs_base * 1000, yerr=yerr, fmt=MARKERS[i] + "-", color=SERIES[i], markersize=5,
                    linewidth=2, elinewidth=1.2, markeredgecolor=SURFACE, label=lab)
        ticks = d.level.tolist()
    ax.set_xticks(np.arange(len(ticks)))
    ax.set_xticklabels(ticks)
    ax.set_xlabel("decision time (minutes before settlement)", fontsize=8)
    ax.set_ylabel("log loss vs G-raw-2h (millinats)", fontsize=8)
    ax.legend(fontsize=7, frameon=False, labelcolor=INK2)
    _save(fig, path)
