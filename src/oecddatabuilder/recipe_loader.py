import json
from typing import Dict, Any, Optional
from pathlib import Path
import os
import logging
import copy
import requests
from lxml import etree
from .utils import create_retry_session  # Import the retry-enabled session helper

# Set up logging configuration.
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Compute the base and config directories relative to this file.
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = (BASE_DIR / ".." / ".." / "config").resolve()
USER_CONFIG_PATH = CONFIG_DIR / "recipe.json"

# The built-in defaults are now placed under the "DEFAULT" group.
_DEFAULT_RECIPE = {
    "DEFAULT": {
        "Y": {
            "FREQ": "Q",
            "ADJUSTMENT": "",
            "REF_AREA": "KOR+CAN+USA+CHN+GBR+DEU+FRA+JPN+ITA+IND+MEX+IRL",
            "SECTOR": "S1",
            "COUNTERPART_SECTOR": "",
            "TRANSACTION": "B1GQ",
            "INSTR_ASSET": "",
            "ACTIVITY": "",
            "EXPENDITURE": "",
            "UNIT_MEASURE": "USD_PPP",
            "PRICE_BASE": "LR",
            "TRANSFORMATION": "",
            "TABLE_IDENTIFIER": ""
        },
        "C": {
            "FREQ": "Q",
            "ADJUSTMENT": "",
            "REF_AREA": "KOR+CAN+USA+CHN+GBR+DEU+FRA+JPN+ITA+IND+MEX+IRL",
            "SECTOR": "S1M",
            "COUNTERPART_SECTOR": "",
            "TRANSACTION": "P3",
            "INSTR_ASSET": "",
            "ACTIVITY": "",
            "EXPENDITURE": "",
            "UNIT_MEASURE": "USD_PPP",
            "PRICE_BASE": "LR",
            "TRANSFORMATION": "",
            "TABLE_IDENTIFIER": ""
        },
        "G": {
            "FREQ": "Q",
            "ADJUSTMENT": "",
            "REF_AREA": "KOR+CAN+USA+CHN+GBR+DEU+FRA+JPN+ITA+IND+MEX+IRL",
            "SECTOR": "S13",
            "COUNTERPART_SECTOR": "",
            "TRANSACTION": "P3",
            "INSTR_ASSET": "",
            "ACTIVITY": "",
            "EXPENDITURE": "",
            "UNIT_MEASURE": "USD_PPP",
            "PRICE_BASE": "LR",
            "TRANSFORMATION": "",
            "TABLE_IDENTIFIER": ""
        },
        "I": {
            "FREQ": "Q",
            "ADJUSTMENT": "",
            "REF_AREA": "KOR+CAN+USA+CHN+GBR+DEU+FRA+JPN+ITA+IND+MEX+IRL",
            "SECTOR": "S1",
            "COUNTERPART_SECTOR": "",
            "TRANSACTION": "P51G",
            "INSTR_ASSET": "",
            "ACTIVITY": "",
            "EXPENDITURE": "",
            "UNIT_MEASURE": "USD_PPP",
            "PRICE_BASE": "LR",
            "TRANSFORMATION": "",
            "TABLE_IDENTIFIER": ""
        },
        "EX": {
            "FREQ": "Q",
            "ADJUSTMENT": "",
            "REF_AREA": "KOR+CAN+USA+CHN+GBR+DEU+FRA+JPN+ITA+IND+MEX+IRL",
            "SECTOR": "S1",
            "COUNTERPART_SECTOR": "",
            "TRANSACTION": "P6",
            "INSTR_ASSET": "",
            "ACTIVITY": "",
            "EXPENDITURE": "",
            "UNIT_MEASURE": "USD_PPP",
            "PRICE_BASE": "LR",
            "TRANSFORMATION": "",
            "TABLE_IDENTIFIER": ""
        },
        "IM": {
            "FREQ": "Q",
            "ADJUSTMENT": "",
            "REF_AREA": "KOR+CAN+USA+CHN+GBR+DEU+FRA+JPN+ITA+IND+MEX+IRL",
            "SECTOR": "S1",
            "COUNTERPART_SECTOR": "",
            "TRANSACTION": "P7",
            "INSTR_ASSET": "",
            "ACTIVITY": "",
            "EXPENDITURE": "",
            "UNIT_MEASURE": "USD_PPP",
            "PRICE_BASE": "LR",
            "TRANSFORMATION": "",
            "TABLE_IDENTIFIER": ""
        }
    }
}


class RecipeLoader:
    def __init__(self, recipe_config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the RecipeLoader instance.
        The built-in defaults are used only for initializing the in-memory configuration.
        """
        if recipe_config is None:
            # Use built-in defaults for initialization.
            self.recipe = copy.deepcopy(_DEFAULT_RECIPE)
        else:
            self.recipe = recipe_config

        self._update_recipe()

    def _update_recipe(self) -> None:
        """
        Write the current in-memory recipe configuration to the user configuration file.
        Bypasses merging with stored defaults; writes the configuration as-is.
        """
        try:
            self._atomic_write(str(USER_CONFIG_PATH), self.recipe)
            logger.info(f"User configuration updated successfully in {USER_CONFIG_PATH}.")
        except Exception as e:
            logger.error(f"Error saving updated recipe configuration: {e}")

    def _atomic_write(self, output_file: str, data: Dict[str, Any]) -> None:
        """
        Atomically write the given data as JSON to output_file.
        Writes to a temporary file first, then replaces the target file
        to minimize the risk of partial writes.

        :param output_file: Path to the target JSON file.
        :param data: The configuration data to write.
        """
        temp_file = output_file + ".tmp"
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            os.replace(temp_file, output_file)
            logger.info(f"Atomic write successful to {output_file}")
        except Exception as e:
            logger.error(f"Error performing atomic write to {output_file}: {e}")
            raise

    def _deep_merge(self, source: Dict[Any, Any], overrides: Dict[Any, Any]) -> Dict[Any, Any]:
        """
        Recursively merge the 'overrides' dictionary into the 'source' dictionary.
        For keys present in both:
          - If both values are dictionaries, merge recursively.
          - Otherwise, the override value replaces the source value.

        :param source: Original configuration dictionary.
        :param overrides: Dictionary with override values.
        :return: Merged dictionary.
        """
        merged = copy.deepcopy(source)
        for key, override_value in overrides.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(override_value, dict):
                merged[key] = self._deep_merge(merged[key], override_value)
            else:
                merged[key] = override_value
        return merged

    def load(self, recipe_name: str) -> Dict[str, Any]:
        """
        Load the recipe configuration for the specified recipe group.
        If a user configuration file exists and contains an override for the group,
        it will replace the in-memory configuration for that group; otherwise, the
        originally initialized configuration is used.

        :param recipe_name: Key identifying the recipe group.
        :return: Configuration dictionary for the specified recipe group.
        """
        if USER_CONFIG_PATH.exists():
            try:
                with USER_CONFIG_PATH.open("r", encoding="utf-8") as f:
                    user_config = json.load(f)
                if recipe_name in user_config:
                    self.recipe[recipe_name] = user_config[recipe_name]
                    logger.info(f"User configuration loaded for group '{recipe_name}'.")
                else:
                    logger.info(f"No user configuration for group '{recipe_name}'. Using in-memory configuration.")
            except Exception as e:
                logger.error(f"Error loading user configuration from {USER_CONFIG_PATH}: {e}")
        else:
            logger.info(f"No user configuration file found at {USER_CONFIG_PATH}. Using in-memory configuration.")
        return self.recipe.get(recipe_name, {})

    def update_recipe_from_url(self, recipe_name: str, indicator_urls: Dict[str, str]) -> None:
        """
        Update the recipe configuration for a specific group using indicator URLs.
        For each indicator, fetch the XML data from the provided URL and extract the
        transaction filters from the first <Series> element's <SeriesKey> component.
        The extracted values (e.g. FREQ, ADJUSTMENT, REF_AREA, SECTOR, etc.) are stored
        into the recipe for the given indicator key. The URL is stored under "URL".
        Finally, the updated configuration is written to the user configuration file.
        
        :param recipe_name: Key identifying the recipe group to update.
        :param indicator_urls: Mapping of indicator keys to their associated URLs.
        """
        # Retrieve or initialize the configuration for the specified group.
        recipe_config = self.recipe.get(recipe_name, {})

        # HTTP headers for requesting the proper SDMX generic XML format.
        headers = {
            "Accept": "application/vnd.sdmx.genericdata+xml; charset=utf-8; version=2.1"
        }

        # Use a retry-enabled session from utils.
        session = create_retry_session()

        for indicator, url in indicator_urls.items():
            if indicator not in recipe_config:
                logger.info(f"Indicator '{indicator}' not found in group '{recipe_name}'. Creating new entry.")
                recipe_config[indicator] = {}
            logger.info(f"Updated indicator '{indicator}' with URL: {url}")

            try:
                response = session.get(url, headers=headers)
                response.raise_for_status()
                root = etree.fromstring(response.content)

                # Use lxml's XPath to find the first <Series> element.
                series_list = root.xpath('//*[local-name()="Series"]')
                if series_list:
                    series = series_list[0]
                    series_key_list = series.xpath('.//*[local-name()="SeriesKey"]')
                    if series_key_list:
                        series_key = series_key_list[0]
                        new_config = {}
                        # Retrieve all <Value> elements from the SeriesKey.
                        value_elements = series_key.xpath('.//*[local-name()="Value"]')
                        for value_elem in value_elements:
                            key_attr = value_elem.get("id")
                            val_attr = value_elem.get("value")
                            if key_attr and val_attr:
                                new_config[key_attr] = val_attr
                        if new_config:
                            recipe_config[indicator].update(new_config)
                            logger.info(f"Metadata for indicator '{indicator}' updated with: {new_config}")
                        else:
                            logger.warning(f"No metadata extracted from SeriesKey for indicator '{indicator}' from URL: {url}")
                    else:
                        logger.warning(f"No <SeriesKey> element found in the XML from URL: {url} for indicator '{indicator}'")
                else:
                    logger.warning(f"No <Series> element found in the XML from URL: {url} for indicator '{indicator}'")
            except Exception as e:
                logger.error(f"Failed to update metadata for indicator '{indicator}' from URL {url}: {e}")

        # Update in-memory configuration.
        self.recipe[recipe_name] = recipe_config

        # Write the updated configuration back to the user configuration file.
        try:
            if USER_CONFIG_PATH.exists():
                with USER_CONFIG_PATH.open("r", encoding="utf-8") as f:
                    current_overrides = json.load(f)
            else:
                current_overrides = {}
            current_overrides[recipe_name] = recipe_config
            self._atomic_write(str(USER_CONFIG_PATH), current_overrides)
            logger.info(f"Recipe group '{recipe_name}' updated successfully in {USER_CONFIG_PATH}.")
        except Exception as e:
            logger.error(f"Error saving updated recipe for group '{recipe_name}': {e}")

        logger.info("Recipe update completed successfully.")
