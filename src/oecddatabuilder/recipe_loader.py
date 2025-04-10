"""
Module for loading and updating OECD API recipe configurations.

This module defines the RecipeLoader class that loads, merges, and updates recipe
configurations used for fetching and processing OECD API data.
"""

import copy
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, cast

from lxml import etree  # Ensure lxml is installed

from .utils import create_retry_session  # Import the retry session helper from utils

# Set up logging configuration.
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Compute the base and config directories relative to this file.
BASE_DIR = Path(__file__).resolve().parent
RECIPE_PATH = (BASE_DIR / ".." / ".." / "config" / "recipe.json").resolve()

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
            "TABLE_IDENTIFIER": "",
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
            "TABLE_IDENTIFIER": "",
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
            "TABLE_IDENTIFIER": "",
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
            "TABLE_IDENTIFIER": "",
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
            "TABLE_IDENTIFIER": "",
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
            "TABLE_IDENTIFIER": "",
        },
    }
}


class RecipeLoader:
    def __init__(self, verbose: bool = False) -> None:
        """
        Initialize the RecipeLoader instance.

        Attempts to load an existing recipe configuration from RECIPE_PATH.
        If the file does not exist or is invalid, creates a new recipe file using
        _DEFAULT_RECIPE.

        :param verbose: If True, logs additional information.
        """
        self.verbose = verbose
        if RECIPE_PATH.exists():
            try:
                with RECIPE_PATH.open("r", encoding="utf-8") as f:
                    self.recipe = json.load(f)
                # If the "DEFAULT" group is missing, merge with the built-in default.
                if "DEFAULT" not in self.recipe:
                    self.recipe = self._deep_merge(_DEFAULT_RECIPE, self.recipe)
                if self.verbose:
                    logger.info("Loaded recipe configuration from file.")
            except Exception as e:
                logger.error("Error loading recipe file: %s", e)
                logger.warning(
                    "Creating new recipe configuration with default settings."
                )
                self.recipe = copy.deepcopy(_DEFAULT_RECIPE)
                self.save()
        else:
            logger.warning(
                "No recipe.json file found; creating one with default configuration."
            )
            self.recipe = copy.deepcopy(_DEFAULT_RECIPE)
            self.save()

    def _atomic_write(self, output_file: str, data: Dict[str, Any]) -> None:
        """
        Atomically write the given data as JSON to output_file.
        Writes to a temporary file first, then replaces the target file.
        :param output_file: Path to the target JSON file.
        :param data: The configuration data to write.
        """
        temp_file = output_file + ".tmp"
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            os.replace(temp_file, output_file)
            logger.info("Atomic write successful to %s", output_file)
        except Exception as e:
            logger.error("Error performing atomic write to %s: %s", output_file, e)
            raise

    def _deep_merge(
        self, source: Dict[Any, Any], overrides: Dict[Any, Any]
    ) -> Dict[Any, Any]:
        """
        Recursively merge the 'overrides' dictionary into the 'source' dictionary.
        For keys present in both dictionaries:
          - If both values are dictionaries, merge them recursively.
          - Otherwise, the override value replaces the source value.
        :param source: The original configuration dictionary.
        :param overrides: The dictionary with override values.
        :return: The merged configuration dictionary.
        """
        merged = copy.deepcopy(source)
        for key, override_value in overrides.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(override_value, dict)
            ):
                merged[key] = self._deep_merge(merged[key], override_value)
            else:
                merged[key] = override_value
        return merged

    def load(self, recipe_name: str) -> Dict[str, Any]:
        """
        Load the recipe configuration for the specified recipe group.
        Merges any stored overrides from the recipe file with the in-memory configuration.
        :param recipe_name: Key identifying the recipe group.
        :return: The configuration dictionary for the specified group.
        """
        if RECIPE_PATH.exists():
            try:
                with RECIPE_PATH.open("r", encoding="utf-8") as f:
                    user_config = json.load(f)
                if recipe_name in user_config:
                    self.recipe[recipe_name] = self._deep_merge(
                        self.recipe.get(recipe_name, {}), user_config[recipe_name]
                    )
                    logger.info(
                        "User configuration merged for group '%s'.", recipe_name
                    )
                else:
                    logger.info(
                        "No stored configuration for group '%s'; using in-memory configuration.",
                        recipe_name,
                    )
            except Exception as e:
                logger.error("Error loading configuration from %s: %s", RECIPE_PATH, e)
        else:
            logger.info(
                "No recipe file found at %s; using in-memory configuration.",
                RECIPE_PATH,
            )
        return self.recipe.get(recipe_name, {})

    def update_recipe_from_url(
        self, recipe_name: str, indicator_urls: Dict[str, str]
    ) -> None:
        """
        Update the recipe configuration for a specific group using indicator URLs.
        For each indicator, fetch the XML data from the provided URL and extract transaction
        filters from the first <Series> element's <SeriesKey> component. The extracted values are
        merged with any existing configuration, and the URL is stored under "URL". Finally, the
        updated configuration is written to the recipe file.
        :param recipe_name: Key identifying the recipe group to update.
        :param indicator_urls: Mapping of indicator keys to their associated URLs.
        """
        # Retrieve (or initialize) the configuration for the group.
        recipe_config = self.recipe.get(recipe_name, {})

        headers = {
            "Accept": "application/vnd.sdmx.genericdata+xml; charset=utf-8; version=2.1"
        }
        session = create_retry_session()

        for indicator, url in indicator_urls.items():
            current_entry = recipe_config.get(indicator, {})
            logger.info("Updating indicator '%s' with URL: %s", indicator, url)
            try:
                response = session.get(url, headers=headers)
                response.raise_for_status()
                root = etree.fromstring(response.content)
                # Cast the result of xpath to a list of _Element.
                series_list = cast(
                    list[etree._Element], root.xpath('//*[local-name()="Series"]')
                )
                if series_list:
                    series = series_list[0]
                    series_key_list = cast(
                        list[etree._Element],
                        series.xpath('.//*[local-name()="SeriesKey"]'),
                    )
                    if series_key_list:
                        series_key = series_key_list[0]
                        new_config: Dict[str, str] = {}
                        value_elements = cast(
                            list[etree._Element],
                            series_key.xpath('.//*[local-name()="Value"]'),
                        )
                        for value_elem in value_elements:
                            key_attr = value_elem.get("id")
                            val_attr = value_elem.get("value")
                            if key_attr and val_attr:
                                new_config[key_attr] = val_attr
                        if new_config:
                            current_entry = self._deep_merge(current_entry, new_config)
                            logger.info(
                                "Metadata for indicator '%s' updated with: %s",
                                indicator,
                                new_config,
                            )
                        else:
                            logger.warning(
                                "No metadata extracted for indicator '%s' from URL: %s",
                                indicator,
                                url,
                            )
                    else:
                        logger.warning(
                            "No <SeriesKey> element found in XML from URL: %s for indicator '%s'",
                            url,
                            indicator,
                        )
                else:
                    logger.warning(
                        "No <Series> element found in XML from URL: %s for indicator '%s'",
                        url,
                        indicator,
                    )
            except Exception as e:
                logger.error(
                    "Failed to update metadata for indicator '%s' from URL %s: %s",
                    indicator,
                    url,
                    e,
                )
            recipe_config[indicator] = current_entry

        self.recipe[recipe_name] = recipe_config

        # Merge updated configuration into the stored recipe file.
        try:
            if RECIPE_PATH.exists():
                with RECIPE_PATH.open("r", encoding="utf-8") as f:
                    current_overrides = json.load(f)
            else:
                current_overrides = {}
            current_overrides[recipe_name] = recipe_config
            self._atomic_write(str(RECIPE_PATH), current_overrides)
            logger.info(
                "Recipe group '%s' updated successfully in %s.",
                recipe_name,
                RECIPE_PATH,
            )
        except Exception as e:
            logger.error(
                "Error saving updated recipe for group '%s': %s", recipe_name, e
            )

        logger.info("Recipe update completed successfully.")

    def save(self) -> None:
        """
        Persist the entire in-memory recipe configuration to the recipe file.
        """
        try:
            self._atomic_write(str(RECIPE_PATH), self.recipe)
            logger.info(
                "Entire recipe configuration saved successfully to %s.", RECIPE_PATH
            )
        except Exception as e:
            logger.error("Error saving the recipe configuration: %s", e)

    def show(self) -> None:
        """
        Display the current in-memory recipe configuration in a human-readable format.
        """
        import pprint

        pprint.pprint(self.recipe)
