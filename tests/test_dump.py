"""Tests for the base module."""
import csv
import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from datafaker.dump import CsvTableWriter, get_parquet_table_writer
from datafaker.main import app
from tests.utils import DatafakerTestCase, RequiresDBTestCase, TestDuckDb


class DumpTests(RequiresDBTestCase):
    """Testing configure-tables."""

    dump_file_path = "instrument.sql"
    database_name = "instrument"
    schema_name = "public"

    def assert_timestamps_equal(  # type: ignore[no-any-unimported]
        self, ts1: pd.Timestamp, ts2: pd.Timestamp
    ) -> None:
        """
        Assert that the timestamps are equal.

        Timezone-naive timestamps are put into UTC.
        """
        if ts1.tz is None:
            if ts2.tz is not None:
                self.assertEqual(ts1.tz_localize("UTC"), ts2)
                return
        elif ts2.tz is None:
            self.assertEqual(ts1, ts2.tz_localize("UTC"))
            return
        self.assertEqual(ts1, ts2)

    def test_dump_data_csv(self) -> None:
        """Test dump-data for CSV output."""
        outdir = Path(tempfile.mkdtemp("dump"))
        writer = CsvTableWriter(self.metadata, self.dsn, self.schema_name)
        table_name = "manufacturer"
        writer.write(self.metadata.tables[table_name], outdir)
        table_path = outdir / f"{table_name}.csv"
        self.assertTrue(table_path.is_file())
        with table_path.open() as table_fh:
            reader = csv.reader(table_fh)
            content = list(reader)
            self.assertListEqual(content[0], ["id", "name", "founded"])
            self.assertListEqual(
                content[1], ["1", "Blender", "1951-01-08 12:05:06+00:00"]
            )
            self.assertListEqual(
                content[2], ["2", "Gibbs", "1959-03-04 15:08:09+00:00"]
            )

    def test_dump_data_parquet(self) -> None:
        """Test dump-data for Parquet output."""
        outdir = Path(tempfile.mkdtemp("dump"))
        writer = get_parquet_table_writer(self.metadata, self.dsn, self.schema_name)
        table_name = "manufacturer"
        writer.write(self.metadata.tables[table_name], outdir)
        table_path = outdir / f"{table_name}.parquet"
        self.assertTrue(table_path.is_file())
        df = pd.read_parquet(table_path)
        self.assertListEqual(df.columns.to_list(), ["id", "name", "founded"])
        self.assertListEqual(df["id"].to_list(), [1, 2])
        self.assertListEqual(df["name"].to_list(), ["Blender", "Gibbs"])
        self.assertEqual(len(df["founded"]), 2)
        self.assert_timestamps_equal(
            df["founded"][0], pd.Timestamp("1951-01-08 12:05:06+00:00")
        )
        self.assert_timestamps_equal(
            df["founded"][1], pd.Timestamp("1959-03-04 15:08:09+00:00")
        )


class DumpTestsDuckDb(DumpTests):
    """DumpTests against DuckDB."""

    database_type = TestDuckDb


class EndToEndParquetTestCase(DatafakerTestCase):
    """Read in parquet, make some generators, output parquet."""

    database_type = TestDuckDb
    examples_dir = Path("tests/examples/duckdb")

    def setUp(self) -> None:
        """Set up the files in a temporary directory."""
        super().setUp()
        # Grab all the files
        self.parquet_dir = Path(tempfile.mkdtemp("parq"))
        for fname in os.listdir(self.examples_dir):
            shutil.copy(self.examples_dir / fname, self.parquet_dir / fname)
        self.start_dir = os.getcwd()
        os.chdir(self.parquet_dir)

    def tearDown(self) -> None:
        """Return to the start directory."""
        os.chdir(self.start_dir)
        return super().tearDown()

    def test_end_to_end_parquet(self) -> None:
        """
        Test that parquet with an orm.yaml works.

        Read it in, set some generators, then ``dump-data``.
        """
        # Set up the runner
        runner = CliRunner(
            mix_stderr=False,
            env={
                # this file need not exist
                "src_dsn": "duckdb:///:memory:",
                # this file will be created by Datafaker
                "dst_dsn": "duckdb:///./fake.db",
                # "dst_schema": "fake.dstschema", if you must
            },
        )

        # Configure with the spec file
        result = runner.invoke(
            app, ["configure-generators", "--spec", str(self.parquet_dir / "spec.csv")]
        )
        self.assertSuccess(result)
        self.assertNotIn("no changes", result.stdout)

        # Generate source stats
        result = runner.invoke(app, ["make-stats"])
        self.assertSuccess(result)
        self.assertNotIn("no changes", result.stdout)

        # Generate the fake data
        result = runner.invoke(app, ["create-tables"])
        self.assertSuccess(result)
        result = runner.invoke(app, ["create-generators"])
        self.assertSuccess(result)
        num_passes = 70
        result = runner.invoke(app, ["create-data", "--num-passes", str(num_passes)])
        self.assertSuccess(result)

        # Dump the fake tables
        outdir = Path(tempfile.mkdtemp("dump"))
        result = runner.invoke(app, ["dump-data", "--output", str(outdir), "--parquet"])
        print(result)
        self.assertSuccess(result)

        # Check the dumped files
        # There should be three of them
        expected_names = {
            "model": "model.parquet",
            "player": "player.parquet",
            "signature-model": "signature_model.parquet",
        }
        names = os.listdir(outdir)
        self.assertSetEqual(set(names), set(expected_names.values()))

        # load the output files
        dfs = {k: pd.read_parquet(outdir / v) for k, v in expected_names.items()}

        # Each one should have the correct number of rows
        for v in dfs.values():
            self.assertEqual(v.shape[0], num_passes)

        # Check the foreign keys
        player_ids = set()
        for i, v in enumerate(dfs["signature-model"]["player_id"]):
            player_ids.add(v)
            self.assertLessEqual(v, i + 1)
        # Check that many of the possible keys have been used
        self.assertLess(num_passes / 3, len(player_ids))
