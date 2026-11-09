"""feeds.yaml model and the environment settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

# Feed names are path segments in both buckets and Dagster partition keys.
FeedName = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9-]*$")]


class Feed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool
    producer: str = Field(min_length=1)
    static: str = Field(pattern=r"^https?://")
    vehicle_positions: str = Field(pattern=r"^https?://")
    trip_updates: str = Field(pattern=r"^https?://")
    service_alerts: str | None = Field(default=None, pattern=r"^https?://")
    poll: int = Field(default=15, ge=5, le=300)
    build_lag_days: int = Field(default=1, ge=1, le=7)

    def rt_urls(self) -> dict[str, str]:
        """GTFS-RT URLs by raw line `feed` kind: vp, tu and al when set."""
        urls = {"vp": self.vehicle_positions, "tu": self.trip_updates}
        if self.service_alerts:
            urls["al"] = self.service_alerts
        return urls


class FeedsFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    feeds: dict[FeedName, Feed]

    @classmethod
    def load(cls, path: Path) -> Self:
        return cls.model_validate(yaml.safe_load(path.read_text()))

    def enabled(self) -> dict[str, Feed]:
        return {name: f for name, f in self.feeds.items() if f.enabled}


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError(f"{name} is not set")
    return value


@dataclass(frozen=True)
class S3Settings:
    endpoint: str
    bucket: str
    region: str
    access_key: str
    secret_key: str

    @classmethod
    def from_env(cls, prefix: str = "") -> Self:
        """Prefixed settings; endpoint and region fall back to the unprefixed ones."""
        return cls(
            endpoint=_env(f"{prefix}S3_ENDPOINT", os.environ.get("S3_ENDPOINT")),
            bucket=_env(f"{prefix}S3_BUCKET"),
            region=_env(f"{prefix}S3_REGION", os.environ.get("S3_REGION", "garage")),
            access_key=_env(f"{prefix}S3_ACCESS_KEY"),
            secret_key=_env(f"{prefix}S3_SECRET_KEY"),
        )


@dataclass(frozen=True)
class GatusSettings:
    url: str | None
    endpoint_key: str | None
    token: str

    @classmethod
    def from_env(cls) -> Self:
        return cls(
            url=os.environ.get("GATUS_URL") or None,
            endpoint_key=os.environ.get("GATUS_ENDPOINT_KEY") or None,
            token=os.environ.get("GATUS_TOKEN", ""),
        )


@dataclass(frozen=True)
class RecorderSettings:
    feeds: Path
    data_dir: Path
    s3: S3Settings
    gatus: GatusSettings

    @classmethod
    def from_env(cls) -> Self:
        return cls(
            feeds=Path(_env("RT_ARCHIVE_FEEDS", "feeds.yaml")),
            data_dir=Path(_env("RT_ARCHIVE_DATA_DIR", "data")),
            s3=S3Settings.from_env(),
            gatus=GatusSettings.from_env(),
        )


@dataclass(frozen=True)
class BuildSettings:
    feeds: Path
    raw: S3Settings
    tides: S3Settings
    gatus: GatusSettings

    @classmethod
    def from_env(cls) -> Self:
        return cls(
            feeds=Path(_env("RT_ARCHIVE_FEEDS", "feeds.yaml")),
            raw=S3Settings.from_env("RAW_"),
            tides=S3Settings.from_env("TIDES_"),
            gatus=GatusSettings.from_env(),
        )
