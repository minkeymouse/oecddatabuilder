# Why I made this

- OECD Data Explorer has been providing more and more useful data for researchers in variety of fields.

- During COVID period, OECD had launched newly refined OECD data explorer to feature more intuitive UI for accessing OECD database.

- Many researchers has been benefitting from them and now they seem to be opening up with more data with OECD developer's API.

- ISSUE with this kind of data is that users' demand varies. Especially when collecting large amount of data with specific filtering for each time series dataset.

- This made users really difficult to use API for downloading and forced many of them going back to using GUI online.

- Because of this, I have created

# Previous research

- There has been many attempts to create

# Features

- OECD url

- Recipe

    - I introduce a new feature called 'recipe'. it is an abstract nested dictionary which stores blueprint filter information of creating researcher's own dataset for variety of purposes.

    - There are two ways updating the recipe. First, you can update the recipe by using the function called "update_recipe_from_url" which takes inputs of two parameters: name of blueprint, url.

- recipe_loader
    - recipe loader contains 

# What it does

# 