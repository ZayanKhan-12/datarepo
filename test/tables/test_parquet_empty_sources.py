"""Reading a partition that holds no data.

https://github.com/neuralinkcorp/datarepo/issues/41

Polars reports an empty location three different ways depending on how it is
empty, and none of them names the table. One of them is a Rust panic that does
not inherit from Exception, so a caller cannot catch it at all. All three are
reported as DatasourceNotAvailable instead.
"""

from pathlib import Path

import polars as pl
import pytest

from datarepo.core.tables.exceptions import DatasourceNotAvailable
from datarepo.core.tables.filters import Filter
from datarepo.core.tables.parquet_table import (
    ParquetTable,
    _describes_empty_source,
    _scan_frame_class,
)
from datarepo.core.tables.util import Partition, PartitioningScheme


def _table(tmp_path: Path, schema: dict[str, pl.DataType] | None = None):
    """A hive-partitioned table with exactly one populated partition."""
    populated = tmp_path / "date=2024-01-01"
    populated.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"value": [1, 2, 3]}).write_parquet(populated / "df.parquet")

    return ParquetTable(
        name="events",
        uri=str(tmp_path),
        partitioning=[Partition(column="date", col_type=pl.String)],
        partitioning_scheme=PartitioningScheme.HIVE,
        schema=schema,
    )


def _on(date: str) -> list[Filter]:
    return [Filter(column="date", operator="=", value=date)]


class TestEmptyPartitions:
    def test_a_partition_that_was_never_written(self, tmp_path: Path) -> None:
        table = _table(tmp_path)

        with pytest.raises(DatasourceNotAvailable) as caught:
            table(filters=_on("1999-01-01")).collect()

        assert caught.value.table_name == "events"
        assert str(tmp_path) in caught.value.uri

    def test_a_partition_directory_that_exists_but_is_empty(
        self, tmp_path: Path
    ) -> None:
        """Polars reports this one as "expected at least 1 source"."""
        table = _table(tmp_path)
        (tmp_path / "date=1999-01-01").mkdir()

        with pytest.raises(DatasourceNotAvailable):
            table(filters=_on("1999-01-01")).collect()

    def test_an_empty_partition_under_a_declared_schema(
        self, tmp_path: Path
    ) -> None:
        """This is the case polars turns into a Rust panic.

        A PanicException does not inherit from Exception, so before this it
        could not be caught by ordinary error handling at all.
        """
        table = _table(tmp_path, schema={"value": pl.Int64})
        (tmp_path / "date=1999-01-01").mkdir()

        with pytest.raises(Exception) as caught:
            table(filters=_on("1999-01-01")).collect()

        assert isinstance(caught.value, DatasourceNotAvailable)

    def test_the_error_survives_further_operations(self, tmp_path: Path) -> None:
        """Polars rebuilds the frame on every call, so this is worth pinning."""
        table = _table(tmp_path)

        frame = (
            table(filters=_on("1999-01-01"))
            .filter(pl.col("value") > 1)
            .with_columns(pl.lit(1).alias("extra"))
            .select(["value", "extra"])
        )

        with pytest.raises(DatasourceNotAvailable) as caught:
            frame.collect()

        assert caught.value.table_name == "events"


class TestWhatIsLeftAlone:
    def test_a_populated_partition_still_reads(self, tmp_path: Path) -> None:
        table = _table(tmp_path)

        result = table(filters=_on("2024-01-01")).collect()

        assert result.height == 3
        assert result["value"].to_list() == [1, 2, 3]

    def test_a_corrupt_file_keeps_its_own_error(self, tmp_path: Path) -> None:
        """An unreadable file is a real failure and must not be reported as
        an absent one."""
        table = _table(tmp_path)
        broken = tmp_path / "date=2030-01-01"
        broken.mkdir()
        (broken / "df.parquet").write_bytes(b"not a parquet file")

        with pytest.raises(Exception) as caught:
            table(filters=_on("2030-01-01")).collect()

        assert not isinstance(caught.value, DatasourceNotAvailable)

    def test_the_scan_stays_lazy(self, tmp_path: Path) -> None:
        """Building the query must not touch the store.

        The whole point of translating at collect() rather than checking up
        front is that reading a table costs nothing until it is collected.
        """
        table = _table(tmp_path)

        frame = table(filters=_on("1999-01-01"))  # must not raise

        assert isinstance(frame, pl.LazyFrame)


class TestErrorClassification:
    @pytest.mark.parametrize(
        "error",
        [
            FileNotFoundError("No such file or directory (os error 2): /x/y"),
            pl.exceptions.ComputeError("expected at least 1 source"),
        ],
    )
    def test_empty_source_signatures_are_recognised(
        self, error: BaseException
    ) -> None:
        assert _describes_empty_source(error)

    @pytest.mark.parametrize(
        "error",
        [
            pl.exceptions.ComputeError("parquet: File out of specification"),
            pl.exceptions.SchemaError("schemas differ"),
            PermissionError("Access Denied"),
            ValueError("something else entirely"),
        ],
    )
    def test_other_failures_are_not_recognised(
        self, error: BaseException
    ) -> None:
        assert not _describes_empty_source(error)


class TestScanFrameClass:
    def test_each_table_gets_its_own_identity(self) -> None:
        first = _scan_frame_class("a", "s3://bucket/a/")
        second = _scan_frame_class("b", "s3://bucket/b/")

        assert first._table_name == "a"
        assert second._table_name == "b"

    def test_the_class_is_reused_for_the_same_table(self) -> None:
        """Reading a table in a loop should not build a class each time."""
        assert _scan_frame_class("a", "s3://bucket/a/") is _scan_frame_class(
            "a", "s3://bucket/a/"
        )

    def test_the_frame_is_still_a_polars_lazyframe(self) -> None:
        cls = _scan_frame_class("a", "s3://bucket/a/")

        assert issubclass(cls, pl.LazyFrame)


class TestExceptionItself:
    def test_it_names_the_table_the_uri_and_the_cause(self) -> None:
        error = DatasourceNotAvailable("events", "s3://bucket/events/", "boom")

        assert "events" in str(error)
        assert "s3://bucket/events/" in str(error)
        assert "boom" in str(error)

    def test_it_is_a_file_not_found_error(self) -> None:
        """Existing callers catch FileNotFoundError; keep them working."""
        assert issubclass(DatasourceNotAvailable, FileNotFoundError)
