import json
from typing import Dict, Any, Optional
from pathlib import Path
import os
import requests
import xml.etree.ElementTree as ET
import logging
import copy

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

        This method bypasses any merging with stored defaults and writes the configuration as-is.
        """
        try:
            self._atomic_write(str(USER_CONFIG_PATH), self.recipe)
            logger.info(f"User configuration updated successfully in {USER_CONFIG_PATH}.")
        except Exception as e:
            logger.error(f"Error saving updated recipe configuration: {e}")

    def _atomic_write(self, output_file: str, data: Dict[str, Any]) -> None:
        """
        Atomically write the given data as JSON to output_file.
        Writes first to a temporary file and then replaces the target file
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
        This helper is retained for cases where partial updates are needed.
        For keys present in both dictionaries:
          - If both values are dictionaries, merge them recursively.
          - Otherwise, the override value replaces the source value.

        :param source: The original configuration dictionary.
        :param overrides: The dictionary with override values.
        :return: The merged dictionary.
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

        If a user configuration file exists and contains an override for the given
        recipe group, it will replace the in-memory configuration for that group.
        Otherwise, the originally initialized configuration is used.

        :param recipe_name: The key identifying the recipe group.
        :return: The configuration dictionary for the specified recipe group.
        """
        if USER_CONFIG_PATH.exists():
            try:
                with USER_CONFIG_PATH.open("r", encoding="utf-8") as f:
                    user_config = json.load(f)
                if recipe_name in user_config:
                    self.recipe[recipe_name] = user_config[recipe_name]
                    logger.info(f"User configuration loaded for group '{recipe_name}'.")
                else:
                    logger.info(f"No user configuration found for group '{recipe_name}'. Using in-memory configuration.")
            except Exception as e:
                logger.error(f"Error loading user configuration from {USER_CONFIG_PATH}: {e}")
        else:
            logger.info(f"No user configuration file found at {USER_CONFIG_PATH}. Using in-memory configuration.")
        return self.recipe.get(recipe_name, {})

    def update_recipe_from_url(self, recipe_name: str, indicator_urls: Dict[str, str]) -> None:
        """
        Update the recipe configuration for a specific group using indicator URLs.

        For each indicator in indicator_urls, fetch the XML metadata from the provided URL and 
        update the configuration with the retrieved metadata. If the indicator does not exist,
        a new entry is created.

        After updating the in-memory configuration, the changes are written to the user configuration file.

        :param recipe_name: The key identifying the recipe group to update.
        :param indicator_urls: A mapping of indicator keys to their associated URLs.
        """
        # Get the current configuration for the specified group.
        recipe_config = self.recipe.get(recipe_name, {})

        # Define a headers dictionary to request the proper SDMX-ML structure-specific data format.
        headers = {
            "Accept": "application/vnd.sdmx.structurespecificdata+xml; charset=utf-8; version=2.1"
        }

        for indicator, url in indicator_urls.items():
            if indicator not in recipe_config:
                logger.info(f"Indicator '{indicator}' not found in group '{recipe_name}'. Creating new entry.")
                recipe_config[indicator] = {}
            recipe_config[indicator]["URL"] = url
            logger.info(f"Updated indicator '{indicator}' with URL: {url}")
            try:
                # Request the metadata with the appropriate Accept header.
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                root = ET.fromstring(response.content)
                # Namespace-agnostic search for the <Concept> element.
                concept = root.find('.//*[local-name()="Concept"]')
                if concept is not None:
                    concept_id = (concept.find('.//*[local-name()="ID"]').text 
                                if concept.find('.//*[local-name()="ID"]') is not None else "")
                    description = (concept.find('.//*[local-name()="Description"]').text 
                                if concept.find('.//*[local-name()="Description"]') is not None else "")
                    recipe_config[indicator]["variable_example"] = {
                        "id": concept_id,
                        "description": description
                    }
                    logger.info(f"Metadata updated for '{indicator}': ID={concept_id}, Description={description}")
                else:
                    logger.warning(f"No Concept metadata found for '{indicator}' at URL: {url}")
            except Exception as e:
                logger.error(f"Failed to update metadata for '{indicator}' from URL {url}: {e}")

        # Update the in-memory configuration.
        self.recipe[recipe_name] = recipe_config

        # Write back the updated configuration to the user configuration file.
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
