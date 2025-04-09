# utils.py
import pprint
from typing import List, Dict, Optional
import recipe
import requests
import xml.etree.ElementTree as ET

def test_api(self) -> None:
    """
    Performs a simple test of the OECD API by requesting a known query.
    """
    test_url = (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_CAPITA,1.1/"
        "Q............?startPeriod=2024-Q1"
    )
    try:
        resp = requests.get(test_url)
        resp.raise_for_status()
        print("API connection successful.")
    except Exception as e:
        print(f"API Test failed: {e}")

def update_recipe(recipe_name: str, indicator_urls: Dict[str, str]) -> None:
    """
    Update the recipe configuration (in recipe.py) with new URL values and fetch metadata.
    
    This function expects two arguments:
      - recipe_name: The name of the recipe group to update (e.g., "QNAFACTOR").
      - indicator_urls: A dictionary mapping indicator names (e.g., "productivity", "labor_participate")
        to their complete URL. These URLs are assumed to share a common base (e.g.,
        "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA,1.1/").
    
    For each indicator, the function will:
      1. Update the recipe's configuration with the new URL.
      2. Immediately fetch the XML metadata from that URL.
      3. Parse a simple metadata field (here, the first Concept's ID and Description) 
         and update the recipe configuration with the retrieved metadata.
    """

    # Ensure the named recipe exists in the recipe module.
    if not hasattr(recipe, recipe_name):
        raise ValueError(f"Recipe '{recipe_name}' not found in the recipe module.")

    # Get the recipe configuration dictionary (e.g., recipe.QNAFACTOR).
    recipe_config = getattr(recipe, recipe_name)

    # Loop through each indicator provided in the dictionary.
    for indicator, url in indicator_urls.items():
        # Check if the indicator exists in the recipe configuration; if not, create it.
        if indicator not in recipe_config:
            print(f"Indicator '{indicator}' not found in recipe '{recipe_name}'. Creating new entry.")
            recipe_config[indicator] = {}

        # Update the URL field in the configuration.
        recipe_config[indicator]["URL"] = url
        print(f"Updated recipe for '{indicator}' with URL: {url}")

        # Immediately try to fetch the XML metadata from the URL.
        try:
            response = requests.get(url)
            response.raise_for_status()
            root = ET.fromstring(response.content)

            # As an example: extract the first Concept element, then read its ID and Description.
            concept = root.find('.//Concept')
            if concept is not None:
                concept_id = concept.find('ID').text if concept.find('ID') is not None else ""
                description = concept.find('Description').text if concept.find('Description') is not None else ""
                recipe_config[indicator]["variable_example"] = {
                    "id": concept_id,
                    "description": description
                }
                print(f"Metadata for '{indicator}' updated: ID={concept_id}, Description={description}")
            else:
                print(f"No Concept metadata found for '{indicator}' at URL: {url}")
        except Exception as e:
            print(f"Failed to fetch/update metadata for indicator '{indicator}' from URL {url}: {e}")

    print("Recipe updated successfully.")
