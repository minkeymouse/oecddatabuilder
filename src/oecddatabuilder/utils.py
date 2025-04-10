# src/oecddatabuilder/utils.py

import json
import logging
from typing import Dict, Any, Optional

import requests
import xml.etree.ElementTree as ET

from . import OECDAPI_Databuilder
from . import RecipeLoader

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def test_api_connection() -> None:
    """
    Performs a simple test of the OECD API by sending a request to a known query URL.
    
    Logs a success message if the connection is successful or an error message if it fails.
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
        logger.error(f"API Test failed: {e}")


def test_recipe(recipe_conf: Optional[Dict[str, Any]] = None) -> None:
    """
    Tests whether the OECD API fetches data for all indicators in the given recipe configuration.
    
    Due to strict OECD API limits (20 queries per minute and 20 downloads per hour),
    this test uses a minimal time range (a single quarter) to avoid blocking.
    
    If no recipe configuration is provided, the function loads the default configuration
    for the 'QNADATA' recipe group using the RecipeLoader. (Change the recipe group name
    as needed.)
    
    Parameters:
        recipe_conf (Optional[Dict[str, Any]]): The recipe configuration dictionary.
                                                 If None, the default 'QNADATA' configuration is loaded.
    """
    # Load default recipe via RecipeLoader if no external configuration is provided.
    if recipe_conf is None:
        try:
            loader = RecipeLoader()
            # Replace 'QNADATA' with the appropriate key from your built-in defaults.
            recipe_conf = loader.load("QNADATA")
            logger.info("Using default recipe configuration: 'QNADATA'.")
        except ValueError as e:
            logger.error(f"Default recipe configuration 'QNADATA' not found: {e}")
            return

    logger.warning(
        "WARNING: Due to strict OECD API rate limits (20 queries per minute and "
        "20 downloads per hour), this test uses a minimal time range to avoid blocking."
    )

    # Define a minimal test period to reduce the number of API calls.
    test_start = "2024-Q1"
    test_end = "2024-Q1"

    try:
        builder = OECDAPI_Databuilder(
            config=recipe_conf,
            start=test_start,
            end=test_end,
            freq="Q",
            response_format="csv",
            base_url="https://sdmx.oecd.org/public/rest/data/"
        )
        # Use a small chunk size to limit the number of API calls.
        builder.fetch_data(chunk_size=1)
        df = builder.create_dataframe()
        logger.info(f"Test recipe successful. DataFrame shape: {df.shape}")
    except Exception as e:
        logger.error(f"Test recipe failed: {e}")
