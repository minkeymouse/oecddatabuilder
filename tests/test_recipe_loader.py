import json

import pytest

from oecddatabuilder.recipe_loader import RecipeLoader

# ------------------------------------------------------------------------------
# Fixture: Create a temporary recipe.json file for testing.
# ------------------------------------------------------------------------------


@pytest.fixture
def temp_recipe_file(tmp_path):
    """Create a temporary recipe.json file with minimal default content."""
    recipe_file = tmp_path / "recipe.json"
    # Minimal default recipe configuration.
    default_recipe = {
        "DEFAULT": {
            "TEST": {
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
    }
    recipe_file.write_text(json.dumps(default_recipe))
    return recipe_file


@pytest.fixture
def recipe_loader_with_temp_recipe(temp_recipe_file, monkeypatch):
    """
    Create a RecipeLoader instance with RECIPE_PATH overridden to point to
    a temporary recipe file.
    """

    monkeypatch.setattr("oecddatabuilder.recipe_loader.RECIPE_PATH", temp_recipe_file)
    return RecipeLoader(verbose=True)


# ------------------------------------------------------------------------------
# Tests for RecipeLoader
# ------------------------------------------------------------------------------


def test_load_recipe(recipe_loader_with_temp_recipe):
    """Test that RecipeLoader loads the recipe correctly from the temporary file."""
    config = recipe_loader_with_temp_recipe.load("DEFAULT")
    assert "TEST" in config
    assert config["TEST"]["FREQ"] == "Q"


def test_deep_merge(recipe_loader_with_temp_recipe):
    """Test that the _deep_merge method merges dictionaries recursively."""
    original = {"a": 1, "b": {"c": 2}}
    overrides = {"b": {"d": 3}, "e": 4}
    result = recipe_loader_with_temp_recipe._deep_merge(original, overrides)
    expected = {"a": 1, "b": {"c": 2, "d": 3}, "e": 4}
    assert result == expected


# ------------------------------------------------------------------------------
# Helper: Fake HTTP GET to simulate XML response
# ------------------------------------------------------------------------------


def fake_get(url, headers=None):
    """
    A fake get function that returns a simulated XML response.
    This XML contains one <Series> element with a <SeriesKey> that has one <Value> element.
    """
    from requests.models import Response

    xml_content = """<?xml version="1.0" encoding="utf-8"?>
<Root>
  <Series>
    <SeriesKey>
      <Value id="TEST_KEY" value="test_value"/>
    </SeriesKey>
  </Series>
</Root>
"""
    response = Response()
    response._content = xml_content.encode("utf-8")
    response.status_code = 200
    return response


def fake_create_retry_session():
    """
    Returns a fake session object whose get method is replaced with fake_get.
    """
    import requests

    session = requests.Session()
    session.get = fake_get
    return session


# ------------------------------------------------------------------------------
# Test for update_recipe_from_url using the fake response
# ------------------------------------------------------------------------------


def test_update_recipe_from_url(recipe_loader_with_temp_recipe, monkeypatch):
    """
    Test that update_recipe_from_url correctly updates the configuration
    by simulating an XML response.
    """
    # Replace create_retry_session with our fake version.
    from oecddatabuilder import recipe_loader

    monkeypatch.setattr(recipe_loader, "create_retry_session", lambda: fake_create_retry_session())

    indicator_urls = {
        "TEST_INDICATOR": "https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NASEC1@DF_QSA,1.1/Q..AUT....P3.......?startPeriod=2023-Q3"
    }
    recipe_loader_with_temp_recipe.update_recipe_from_url("DEFAULT", indicator_urls)
    config = recipe_loader_with_temp_recipe.load("DEFAULT")

    # Check that the returned configuration for 'TEST_INDICATOR' now includes the new key.
    assert "TEST_KEY" in config.get("TEST_INDICATOR", {})
    assert config["TEST_INDICATOR"]["TEST_KEY"] == "test_value"


# ------------------------------------------------------------------------------
# Test for remove functionality
# ------------------------------------------------------------------------------


def test_remove_recipe_group(recipe_loader_with_temp_recipe):
    """
    Test that the remove function correctly removes a recipe group.
    """
    # Ensure the group to remove exists.
    initial_config = recipe_loader_with_temp_recipe.load("DEFAULT")
    assert "TEST" in initial_config

    # Remove the group.
    recipe_loader_with_temp_recipe.remove("TEST")

    # Load the configuration for the removed group.
    removed_config = recipe_loader_with_temp_recipe.load("TEST")

    # Expect an empty dictionary since the group is removed.
    assert removed_config == {}
