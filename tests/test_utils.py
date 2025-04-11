import pytest
import requests

from oecddatabuilder.utils import create_retry_session, test_api_connection


@pytest.fixture
def test_create_retry_session():
    # Create a session with a specific timeout.
    session = create_retry_session(timeout=5)
    # Verify that the returned object is an instance of requests.Session.
    assert isinstance(session, requests.Session)
    # Make a sample GET request to a reliable endpoint.
    response = session.request(
        "GET",
        "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NASEC1@DF_QSA,1.1/Q..AUT....P3.......?startPeriod=2023-Q3",
    )
    assert response.status_code == 200


@pytest.fixture
def test_api_connection_logs(caplog):
    # Run test_api_connection() and capture logging output.
    test_api_connection()
    # Check that one of the expected log messages appears.
    # (Either "API connection successful" or "API Test failed" should be logged.)
    assert "API connection successful" in caplog.text or "API Test failed" in caplog.text
