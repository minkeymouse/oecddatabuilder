import json
from typing import Dict, Any, Optional
from . import recipe
from pathlib import Path
import os
import requests
import xml.etree.ElementTree as ET
import logging

# Set up logging configuration.
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Compute the base and config directories relative to this file.
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = (BASE_DIR / ".." / ".." / "config").resolve()
USER_CONFIG_PATH = CONFIG_DIR / "user_recipe.json"

class RecipeLoader:
    def __init__(self, recipe_config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the RecipeLoader instance.

        :param recipe_config: The default recipe configuration dictionary.
                              If None, uses the default recipe (e.g., recipe.QNA).
        """
        if recipe_config is None:
            # Assume the default recipe group is QNA defined in the recipe module.
            if not hasattr(recipe, "QNA"):
                raise ValueError("Default recipe 'QNA' not found in recipe module.")
            self.recipe = recipe.QNA.copy()
        else:
            self.recipe = recipe_config

    def _atomic_write(self, output_file: str, data: Dict[str, Any]) -> None:
        """
        Atomically write the given data as JSON to output_file.
        Writes first to a temporary file and then replaces the target file
        to minimize the risk of partial writes or race conditions.
        
        :param output_file: Path to the target JSON file.
        :param data: The dictionary data to write.
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
        For any key present in both dictionaries:
          - If both values are dictionaries, merge them recursively.
          - Otherwise, the value from overrides replaces the value in source.
        
        :param source: The original dictionary.
        :param overrides: The dictionary with override values.
        :return: The merged dictionary.
        """
        for key, override_value in overrides.items():
            if key in source and isinstance(source[key], dict) and isinstance(override_value, dict):
                source[key] = self._deep_merge(source[key], override_value)
            else:
                source[key] = override_value
        return source

    def load(self, recipe_name: str) -> Dict[str, Any]:
        """
        Load the active recipe configuration for the given recipe group.
        
        Priority:
          1. Built-in defaults from recipe.py.
          2. User overrides from config/user_recipe.json, if available.
          
        Merging is performed recursively.

        :param recipe_name: The name of the recipe group (e.g., "QNA").
        :return: The merged configuration dictionary.
        :raises ValueError: If the recipe group is not found in the recipe module.
        """
        if not hasattr(recipe, recipe_name):
            raise ValueError(f"Recipe '{recipe_name}' not found in the recipe module.")
        
        config = getattr(recipe, recipe_name).copy()
        logger.info(f"Loaded default configuration for recipe '{recipe_name}'.")

        if USER_CONFIG_PATH.exists():
            try:
                with USER_CONFIG_PATH.open("r", encoding="utf-8") as f:
                    user_config = json.load(f)
                if recipe_name in user_config:
                    config = self._deep_merge(config, user_config[recipe_name])
                    logger.info(f"User overrides found and merged for '{recipe_name}'.")
            except Exception as e:
                logger.error(f"Error loading user configuration: {e}")
        else:
            logger.info(f"No user override configuration found at {USER_CONFIG_PATH}.")

        # Update the internal recipe configuration.
        self.recipe = config
        return config

    def update_recipe(self, recipe_name: str, indicator_urls: Dict[str, str]) -> None:
        """
        Update the recipe configuration with new URL values and XML metadata.
        
        For each indicator provided in indicator_urls:
          1. Updates (or creates) the indicator entry in the designated recipe group.
          2. Sets the URL.
          3. Fetches XML metadata from the URL, extracts the first Concept's ID and Description,
             and stores them as "variable_example" within the indicator's configuration.
          4. Persists the updated configuration to the external JSON file.
          
        :param recipe_name: The name of the recipe group (e.g., "QNA").
        :param indicator_urls: A dictionary mapping indicator names (e.g., "productivity")
                               to the complete URL.
        :raises ValueError: If the recipe group is not found.
        """
        if not hasattr(recipe, recipe_name):
            raise ValueError(f"Recipe '{recipe_name}' not found in the recipe module.")

        # Get the base configuration from the recipe module.
        recipe_config = getattr(recipe, recipe_name)
        logger.info(f"Updating recipe for recipe group '{recipe_name}'.")

        for indicator, url in indicator_urls.items():
            if indicator not in recipe_config:
                logger.info(f"Indicator '{indicator}' not found in recipe '{recipe_name}'. Creating new entry.")
                recipe_config[indicator] = {}
            recipe_config[indicator]["URL"] = url
            logger.info(f"Updated recipe for '{indicator}' with URL: {url}")

            try:
                response = requests.get(url)
                response.raise_for_status()
                root = ET.fromstring(response.content)
                concept = root.find('.//Concept')
                if concept is not None:
                    concept_id = concept.find('ID').text if concept.find('ID') is not None else ""
                    description = concept.find('Description').text if concept.find('Description') is not None else ""
                    recipe_config[indicator]["variable_example"] = {
                        "id": concept_id,
                        "description": description
                    }
                    logger.info(f"Metadata for '{indicator}' updated: ID={concept_id}, Description={description}")
                else:
                    logger.warning(f"No Concept metadata found for '{indicator}' at URL: {url}")
            except Exception as e:
                logger.error(f"Failed to fetch/update metadata for indicator '{indicator}' from URL {url}: {e}")

        # Persist the updated recipe externally.
        output_file = str(USER_CONFIG_PATH)
        try:
            updated_recipe = {recipe_name: recipe_config}
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(updated_recipe, f, indent=4)
            logger.info(f"Updated recipe for '{recipe_name}' saved to {output_file}.")
        except Exception as e:
            logger.error(f"Error saving updated recipe to file: {e}")

        logger.info("Recipe update completed successfully.")
