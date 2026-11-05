"""Actual stop times from GPS fixes, a port of the pollers' build.py actual_times()."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gtfs_zone_rt_archive.gtfs import TripLine

GAP = 300  # seconds; fixes further apart than this are not interpolated across
DEP_OFFSET = 50  # metres past the origin that count as departed
NEAR = 100  # metres from a stop that count as at the stop
MAX_SPEED = 40  # m/s; faster forward moves along the trip are projection jumps


@dataclass(frozen=True)
class Actual:
    arrival: int | None
    departure: int
    approx: bool

    @property
    def time(self) -> int:
        """Arrival, else departure (the origin)."""
        return self.arrival if self.arrival is not None else self.departure


def actual_times(
    line: TripLine, fixes: list[tuple[int, float, tuple[float, float]]]
) -> dict[int, Actual]:
    """Actual times by stop index for each stop the vehicle reached.

    `fixes` are TripLine.fix() results. A stop with fixes within NEAR metres
    gets the first and last of them as arrival and departure. Otherwise both
    are the interpolated time the vehicle's distance along the polyline
    crossed the stop (approx). The origin has no arrival.
    """
    pts: list[tuple[int, float, tuple[float, float]]] = []
    for ts, d, xy in sorted(fixes):
        if pts and ts == pts[-1][0]:
            continue
        if pts:
            pt, pd, _ = pts[-1]
            if d <= pd:
                d = pd
            elif (d - pd) / (ts - pt) > MAX_SPEED:
                continue
        pts.append((ts, d, xy))
    out: dict[int, Actual] = {}
    lo = 0  # index of the first fix after the previous stop was reached
    for k, (sxy, target) in enumerate(zip(line.xy, line.dists, strict=True)):
        # Projections overshoot near stops (loops in a lot), so look for a
        # near fix until the vehicle is past the following stop.
        horizon = line.dists[min(k + 1, len(line.dists) - 1)] + NEAR
        near = None
        for i in range(lo, len(pts)):
            if math.dist(pts[i][2], sxy) <= NEAR:
                near = i
                break
            if pts[i][1] > horizon:
                break
        if near is not None:
            j = near
            while j + 1 < len(pts) and math.dist(pts[j + 1][2], sxy) <= NEAR:
                j += 1
            out[k] = Actual(None if k == 0 else pts[near][0], pts[j][0], False)
            lo = j
            continue
        if k == 0:
            target += DEP_OFFSET
        i = next((i for i in range(lo, len(pts)) if pts[i][1] >= target), None)
        if not i or pts[i - 1][1] >= target:
            continue  # never reached, or already past at the first fix
        (t0, d0, _), (t1, d1, _) = pts[i - 1], pts[i]
        if t1 - t0 > GAP:
            continue
        t = round(t0 + (target - d0) / (d1 - d0) * (t1 - t0))
        out[k] = Actual(None if k == 0 else t, t, True)
        lo = i - 1
    return out
