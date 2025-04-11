<p align="center">
  <img src="./docs/image/oecddatabuilder_logo.png" alt="Project Logo" width="200">
</p>

# oecddatabuilder

**oecddatabuilder** is a Python package that helps researchers build structured time series datasets from OECD APIs using customizable "recipes". It automates the retrieval and assembly of data from the OECD Data API, enabling reproducible data pipelines that overcome the limitations of the OECD web interface (no more manual clicks through the OECD GUI!). With **oecddatabuilder**, you can define what data you need in a simple recipe file and get a ready-to-use dataset for analysis.

## Project Overview and Use Case

Economics researchers and data scientists (especially in macroeconometrics and economic modeling) often need to gather multiple time series from the [OECD Data API](https://data.oecd.org/api/sdmx-json-documentation/) for analysis. Doing this through the web GUI or manually assembling CSVs can be time-consuming and error-prone. **oecddatabuilder** streamlines this process by:

- Allowing users to specify *which* datasets and series to fetch (and how to transform or filter them) in a **recipe file** (a simple JSON configuration).
- Automatically fetching those series via the OECD API and merging them into a single cohesive **pandas DataFrame** for analysis.
- Providing a programmatic, reproducible pipeline: once the recipe is written, you can update your data with a single command, ensuring analyses stay up-to-date.
- Overcoming GUI limitations such as manual filtering and downloading, making it easy to regularly update datasets or switch parameters without repeated manual work.

**Key capabilities:**

- Build **reproducible** data pipelines for OECD data (no more pointing and clicking through the web interface for each update).
- Combine multiple indicators from different OECD databases into one time-indexed dataset, ready for modeling.
- **Customize** dataset content easily by editing a recipe or using the recipe tools provided, without changing the code that fetches the data.
- Ensure consistency and transparency: the recipe documents exactly what data is pulled (source, filters, transformations), which improves research reproducibility.

## Installation

```bash
pip install oecddatabuilder
```

The package is published on PyPI, so installation is straightforward with pip. This will install **oecddatabuilder** and its dependencies. (Ensure you have Python 3.10+ installed, as earlier versions may not be supported.)

*After installing*, you can quickly test that everything is working by running `oecddatabuilder --version` or by importing the package in Python.

## Quick Start Example

Let's walk through a minimal example. Suppose we have a recipe (configuration) that specifies a couple of OECD data series we want to retrieve (for example, GDP and inflation for a set of countries). **oecddatabuilder** will load this recipe and build a dataset for us.

**Example: Fetching and merging data using a recipe**

1. **Define your recipe**: In a JSON file (say `my_recipe.json`), list the datasets/indicators you need from OECD. (We'll discuss the recipe format in detail below.)
2. **Use `RecipeLoader` to load the recipe** and possibly modify it.
3. **Use `OECDAPI_Databuilder` to fetch data according to the recipe** and get a combined pandas DataFrame.

Here's how you might do it in code(Suppose you already have recipe defined):

```python
import oecddatabuilder as OECD_data

# Load the recipe from a JSON file
recipe_loader = OECD_data.RecipeLoader()
default_recipe = recipe_loader.load(recipe_name="DEFAULT")

# Initialize the data builder with the recipe
builder = OECD_data.OECDAPI_Databuilder(config=default_recipe, start="1990-Q1", end="2024-Q4", freq="Q", response_format="csv",
                                    dbpath="../datasets/OECD",
                                    base_url="https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA,1.1/", request_interval=60)

# Merge the downloaded data into a single DataFrame
df = builder.create_dataframe()

# Use the data (for example, print the first few rows)
print(df.head())
```

When you run the above, **oecddatabuilder** will:
- Read the configuration from `my_recipe.json` to know which OECD datasets and series to query.
- Download each specified data series from the OECD API (handling the API calls under the hood).
- Merge all retrieved series into one pandas DataFrame `df`, typically using time as the index (and other common dimensions like country as needed).
- The resulting `df` will contain all the data series side-by-side, ready for analysis or export.

## What is a "Recipe"?

A **recipe** in the context of **oecddatabuilder** is a JSON configuration that describes one or more data extractions from the OECD API. Think of it as a list of ingredients (datasets/indicators) and instructions needed to "cook" your combined dataset.

**Typical contents of a recipe** (JSON file):
- A list of one or more **data sources** or series, each with details such as:
  - **Recipe ID**: the recipe identifier (e.g., `"MEI"` for Main Economic Indicators, or a specific dataset code).
  - **Column Name**: each recipe ID has consisting column names(e.g., `"Y"`, `"C"` for GDP and Consumption).
  - **Query or Filters**: the selection of data within that column, e.g., specific countries, indicators, frequencies, etc. This could be a structured object or an OECD API query string.

**Example recipe (JSON):**
```json
{
"GDP_and_Consumption" :
    {
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
        }
    }
}
```


*(Note: This is a subset of default recipe. remember that you must have only one transaction for each column. Filter can vary for each column)*

In this example:
- We have a recipe with two data sources: one might be a GDP series from a national accounts dataset (`B1_GA` perhaps stands for GDP) for US and Canada, and the second is a CPI series.
- Each source has an `id` (which could combine a database identifier and a data structure ID) and a `filter` string that specifies the dimensions (like frequency, country codes, indicator code, etc.) as per the OECD API syntax.
- An `alias` is given to each series to use as the column name in the final DataFrame (so we get columns "GDP_current_prices" and "CPI_index" instead of the raw OECD codes).
- The recipe has a top-level `"frequency": "annual"` indicating that we expect annual data (for GDP), though one source is marked with "M." which might indicate monthly for CPI. (In such a case, **oecddatabuilder** would handle the frequency differences or you would use separate recipes per frequency. Typically it's best not to mix frequencies in one recipe.)

Recipes can be created manually or generated programmatically. The goal is to have a clear, human-readable JSON that you or colleagues can inspect to see exactly what data series are being pulled.

## Working with Recipes via `RecipeLoader`

The `RecipeLoader` class provides convenient methods to load, save, update and remove recipe configurations.

**Loading a recipe:**
```python
from oecddatabuilder import RecipeLoader
my_recipes = RecipeLoader()
data_recipe = my_recipes.load(recipe_name="Your Recipe Name")
```
This will read the JSON file and return a `Nested Dictionary` object that the rest of the package can work with. If the JSON structure is invalid or missing required fields, `RecipeLoader` will typically raise an error to let you know.

It is also recommanded that you can change recipe.json on your own. Study the OECD's documentations and make your own data recipe!

**Modifying a recipe in Python:**

```python
my_recipes.update_recipe_from_url(
                            "Your Recipe Name",
                           {
                            "Column name": "url copied from OECD data explorer for that specific column of time series",
                            "Column name2": "another url copied from OECD data explorer for that specific column of time series"
                            }
)
```

This will write the updated configuration to the file from xml response of API, so you can reuse it later.

**Updating vs. manual editing of the recipe JSON:**
- You can always open the JSON file in a text editor and manually edit the fields (especially if you find it quicker to copy in a new OECD query). This is a straightforward approach for quick changes.
- **Best practice:** If you manually edit the JSON while a recipe is loaded in Python, remember to re-load it (or update the object) before building the dataset. Otherwise, the in-memory recipe might not include your manual changes.

## Building Datasets with `OECDAPI_Databuilder`

Once you have your recipe ready (and loaded), the **OECDAPI_Databuilder** class is used to fetch the data from OECD and assemble it.

**Basic usage:**
```python
builder = OECD_data.OECDAPI_Databuilder(config=default_recipe, start="1990-Q1", end="2024-Q4", freq="Q", response_format="csv",
                                    dbpath="../datasets/OECD",
                                    base_url="https://sdmx.oecd.org/public/rest/data/OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA,1.1/", request_interval=60)
builder.fetch_data(chunk_size=50)
df = builder.create_dataframe()
```
As shown in the Quick Start, these two lines will:
- Iterate through each data source in the recipe, call the OECD API for each (handling the HTTP requests internally),
- Parse the returned data (which is often in SDMX-JSON format) into pandas DataFrame(s),
- Save them into database designated before merging them into one file.
- Merge those DataFrames into one, typically aligning on the time index (and other common dimensions like country, if applicable).

**What the resulting DataFrame looks like:**
- If all series in the recipe share the same frequency and units (e.g., all annual, or all quarterly data), the DataFrame index will likely be time periods (e.g., years, quarters). 
- If frequencies differ (say annual vs monthly as in our recipe example), the builder might either upsample/downsample to align (which could introduce NaNs for missing periods), or it might produce separate indices. (It’s generally recommended to keep one frequency per recipe to avoid complexity.)
- All series values will be numeric (floats/integers) with appropriate NaNs if any data is missing for some dates/country pair.

You can now proceed to use this DataFrame for your analysis: plot the series, feed them into an multivariate time series econometric model, save to CSV/Excel, etc.

## Caveats and Best Practices for OECD API Usage

Working with the OECD API through **oecddatabuilder** is generally straightforward, but keep in mind a few important points:

- **Rate Limits and Politeness**: The OECD API does not require an API key and is free to use, which means it could be susceptible to overload. While OECD has not documented a strict rate limit, it's good practice not to make too many rapid-fire requests. **oecddatabuilder** tries to batch data where possible, but if your recipe has many series from different datasets, it might have to make multiple calls. The package may implement a short delay between calls or retries on failures. If you plan to update data frequently (e.g., automated daily runs), consider the timing and volume of requests. If you encounter HTTP 429 or 5xx errors, you might need to slow down or split your requests.
- **Query Size**: OECD API calls can sometimes retrieve large volumes of data (especially if you request all countries, all years, all indicators of a dataset). Very large queries might time out or be refused. It's often better to request only what you need. In a recipe, be specific in the filters (e.g., list only required country codes, or a reasonable date range).
- **Consistent Frequency & Merging**: If you mix data of different frequencies (say annual vs quarterly) in one recipe, It is likely not going to work. It’s often simplest to keep recipes to one frequency.
- **Retries and Errors**: The package should handle transient network errors by retrying after a short delay. If a particular data source in the recipe fails (due to an invalid code or server issue), by default it might skip or raise an error. Check the logs or console output; **oecddatabuilder** may log warnings if a series couldn't be retrieved. It's good practice to verify the output DataFrame contents (for example, check if any series came back empty or all NaNs, which could indicate a problem with that part of the recipe).

## Tutorial and Documentation

**OECD Data API Documentation**: For advanced users who want to understand the underlying data structure, the official OECD Data API documentation is included in the repository:
- See `docs/OECD_Data_API_documentation.pdf` for detailed information on how OECD APIs are structured (datasets, dimensions, SDMX concepts, etc.). This is an official guide that can help you formulate the right dataset IDs and filter strings for your recipes.
- OECD's own [API Best Practices page](https://www.oecd.org/api-portal/) (or similar resources) is useful if you intend to build heavy-duty applications on top of the API.
- The OECD Data Explorer web tool (https://data-explorer.oecd.org/) is very handy for interactively selecting data and then clicking the "Developer API" button to get the API query. We recommend using that tool to prototype your data query, then plug the results into a recipe.

## Credits

This project was inspired by the need for reproducible large data pipelines in economic research. Credit to the OECD for providing a robust open data API (no API keys needed, which lowers the barrier for researchers). The structure of recipes and the approach to data building was influenced by best practices in data engineering and by similar projects in other languages (such as the R OECD package).

**Author/Maintainer**: *[minkeymouse]* – feel free to reach out for questions or collaboration opportunities.

If you use **oecddatabuilder** in your work or research, please cite this repository or give a shout-out. We would love to know how it's helping your projects.

# Contributing

Many thanks for taking the time to contribute to **oecddatabuilder**! Your contributions, whether fixing bugs, adding new features, or improving the documentation, are very much appreciated.

## Developer Setup Using Poetry

**Prerequisite:** Install [Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer).

1. **Clone the Repository:**

   ```bash
   git clone <your_github_clone_url>
   ```

2. **Navigate to the Project Directory:**

   ```bash
   cd oecddatabuilder
   ```

3. **Activate the Poetry Virtual Environment:**

   Run the following command to create (if needed) and activate the virtual environment:

   ```bash
   poetry shell
   ```

   To verify the active virtual environment, run:

   ```bash
   poetry env info --path
   ```

4. **Install Dependencies:**

   To install the dependencies specified in `poetry.lock` for reproducibility, run:

   ```bash
   poetry install
   ```

   If you prefer to update the dependencies to their latest versions (and update the lock file), run:

   ```bash
   poetry update
   ```

   **Note:** Avoid committing changes to the `poetry.lock` file unless the purpose of your pull request is to update dependencies.

## Git Hooks and Testing

It is recommended to use pre-commit hooks to automatically format your code and run tests before commits or pushes.

1. **Install Pre-commit Hooks:**

   ```bash
   poetry run pre-commit install
   ```

2. **Run the Test Suite:**

   Execute the tests with:

   ```bash
   poetry run pytest -v
   ```

## Additional Guidelines

- **Issues and Feature Requests:**  
  If you encounter any bugs or have ideas for improvements, please open an issue on GitHub.

- **Pull Requests:**  
  Pull requests are warmly welcome! For major changes, consider opening an issue first to discuss your ideas.

- **Code Style:**  
  Please follow the code style guidelines of the project (using tools like Black, isort, and flake8), and include tests for any new features or fixes.

- **Manual vs. Programmatic Recipe Changes:**  
  While you can directly edit the recipe JSON files under the `config/` directory, it is recommended to use the provided `RecipeLoader` methods (such as `update_recipe_from_url` and `remove`) initially to ensure the recipe structure remains consistent and then edit the filters.

For more detailed instructions on dependency management with Poetry, please refer to this [Tutorial](https://realpython.com/dependency-management-python-poetry/).

Thank you for contributing to **oecddatabuilder**!

## License

This project is licensed under the MIT License - see the `LICENSE` file for details. This means you are free to use, modify, and distribute this software. We only ask that you include the license and any copyright notices in your copies or derivatives.

Happy data building with OECD APIs! 🎉

