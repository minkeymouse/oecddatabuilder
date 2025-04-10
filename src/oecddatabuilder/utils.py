import logging
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

__all__ = ["test_api_connection", "test_recipe", "create_retry_session"]


def test_api_connection() -> None:
    """
    Performs a simple test of the OECD API by sending a request to a known query URL.

    Logs a success message if the connection is successful, or an error message if it fails.
    """
    test_url = (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_CAPITA,1.1/"
        "Q............?startPeriod=2024-Q1"
    )
    try:
        resp = requests.get(test_url)
        resp.raise_for_status()
        logger.info("API connection successful.")
    except Exception as e:
        logger.error("API Test failed: %s", e)


def test_recipe(
    recipe_conf: Optional[Dict[str, Any]] = None, base_url: Optional[str] = None
) -> None:
    """
    Tests whether the OECD API fetches data for all indicators in the given recipe configuration.

    Due to strict OECD API limits (20 queries per minute and 20 downloads per hour),
    this test uses a minimal time range (a single quarter) to avoid blocking.

    If no recipe configuration is provided, the function loads the default configuration
    for the 'DEFAULT' recipe group using the RecipeLoader. (Adjust the recipe group name as needed.)

    Parameters:
        recipe_conf (Optional[Dict[str, Any]]): The recipe configuration dictionary.
                                                 If None, the default 'DEFAULT' configuration is loaded.
    """
    from .databuilder import OECDAPI_Databuilder
    from .recipe_loader import RecipeLoader

    if recipe_conf is None:
        try:
            loader = RecipeLoader()
            recipe_conf = loader.load("DEFAULT")
            logger.info("Using default recipe configuration: 'DEFAULT'.")
        except ValueError as e:
            logger.error("Default recipe configuration 'DEFAULT' not found: %s", e)
            return

    logger.warning(
        "WARNING: Due to strict OECD API rate limits (20 queries per minute and "
        "20 downloads per hour), this test uses a minimal time range to avoid blocking."
    )

    test_start = "2024-Q1"
    test_end = "2024-Q1"

    try:
        builder = OECDAPI_Databuilder(
            config=recipe_conf,
            start=test_start,
            end=test_end,
            freq="Q",
            response_format="csv",
            base_url=base_url,
        )
        builder.fetch_data(chunk_size=1)
        df = builder.create_dataframe()
        logger.info("Test recipe successful. DataFrame shape: %s", df.shape)
    except Exception as e:
        logger.error("Test recipe failed: %s", e)


class TimeoutSession(requests.Session):
    """
    A custom requests.Session subclass that enforces a default timeout for all requests.
    """

    def __init__(self, timeout: int = 10) -> None:
        super().__init__()
        self.timeout = timeout
        retries = Retry(
            total=5,
            backoff_factor=1,  # Exponential backoff: 1, 2, 4, 8, ... seconds.
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def request(self, method, url, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return super().request(method, url, **kwargs)


def create_retry_session(timeout: int = 10) -> requests.Session:
    """
    Create and return a TimeoutSession object that uses a retry strategy and enforces a timeout.

    :param timeout: Global timeout (in seconds) for requests.
    :return: Configured requests.Session object.
    """
    return TimeoutSession(timeout=timeout)


if __name__ == "__main__":
    test_api_connection()
    test_recipe()
