# utils.py
import requests
import pprint
from typing import List, Dict, Optional
import recipe

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

def update_recipe(self, urls: Dict = None):
    """
    Update the recipe configuration (in recipe.py) with new URL values.
    The `urls` parameter should be a dictionary where keys correspond to indicator names that exist in recipe.QNADATA.
    For each key, the function adds or updates a 'URL' field in the recipe's dictionary.
    """
    import recipe  # import the recipe module that contains QNADATA

    if urls is None:
        print("No URLs provided for updating recipe.")
        return

    # Ensure that each provided key exists in the recipe's QNADATA.
    missing_keys = set(urls.keys()) - set(recipe.QNADATA.keys())
    if missing_keys:
        raise ValueError(f"The following keys are not found in the recipe: {missing_keys}")

    # Update each entry by adding/updating the 'URL' field.
    for key, url in urls.items():
        recipe.QNADATA[key]["URL"] = url
        print(f"Updated recipe for '{key}' with URL: {url}")

    print("Recipe updated successfully.")
