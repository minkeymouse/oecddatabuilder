import pandas as pd
import pytest

from oecddatabuilder.databuilder import OECDAPI_Databuilder


@pytest.fixture
def dummy_config():
    # Simple configuration for testing purposes.
    return {
        "TEST_INDICATOR": {
            "FREQ": "Q",
            "ADJUSTMENT": "",
            "REF_AREA": "USA",
            "SECTOR": "S1",
            "COUNTERPART_SECTOR": "",
            "TRANSACTION": "T1",
            "INSTR_ASSET": "",
            "ACTIVITY": "",
            "EXPENDITURE": "",
            "UNIT_MEASURE": "USD",
            "PRICE_BASE": "LR",
            "TRANSFORMATION": "",
            "TABLE_IDENTIFIER": "",
        }
    }


@pytest.fixture
def dummy_builder(dummy_config, tmp_path):
    # Use a temporary directory for CSV file storage (dbpath).
    dbpath = tmp_path / "data"
    # Set request_interval to zero to speed up tests.
    builder = OECDAPI_Databuilder(
        config=dummy_config,
        start="2024-Q1",
        end="2024-Q1",
        freq="Q",
        response_format="csv",
        base_url="https://example.com/api/",
        dbpath=str(dbpath),
        request_interval=0,
    )
    return builder


def test_databuilder_initialization(dummy_builder, dummy_config):
    # Check that the configuration is loaded as expected and countries are computed.
    assert dummy_builder.config == dummy_config
    assert "USA" in dummy_builder.countries


def test_parse_response_csv(dummy_builder):
    # Simulate a CSV response and test _parse_response.
    csv_text = "REF_AREA,TIME_PERIOD,OBS_VALUE\nUSA,2024-Q1,100\n"
    df = dummy_builder._parse_response(csv_text)
    assert isinstance(df, pd.DataFrame)
    assert {"REF_AREA", "TIME_PERIOD", "OBS_VALUE"}.issubset(df.columns)
    # Expect exactly one row and three columns.
    assert df.shape == (1, 3)


def test_build_time_chunks(dummy_builder):
    # Test the _build_time_chunks method.
    period_range = pd.period_range(start="2024-Q1", end="2024-Q2", freq="Q")
    # With a chunk size of 1, we should get two chunks.
    chunks = dummy_builder._build_time_chunks(period_range, 1)
    expected_chunks = [("2024-Q1", "2024-Q1"), ("2024-Q2", "2024-Q2")]
    assert chunks == expected_chunks


def test_create_dataframe(dummy_builder):
    # Test create_dataframe method by writing a dummy CSV file.
    dbpath = dummy_builder.dbpath
    dbpath.mkdir(parents=True, exist_ok=True)
    csv_content = "REF_AREA,TIME_PERIOD,OBS_VALUE\nUSA,2024-Q1,100\n"
    # Write a CSV file for TEST_INDICATOR.
    csv_file = dbpath / "TEST_INDICATOR.csv"
    csv_file.write_text(csv_content)

    # Call create_dataframe to merge the data.
    df = dummy_builder.create_dataframe()
    # The resulting DataFrame should include 'date', 'country', and a column for the indicator.
    assert "date" in df.columns
    assert "country" in df.columns
    assert "TEST_INDICATOR" in df.columns
    # Verify the indicator value is numeric and as expected.
    row = df.iloc[0]
    assert row["TEST_INDICATOR"] == 100
