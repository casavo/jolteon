![jolteon](https://assets.pokemon.com/assets/cms2/img/pokedex/full/135.png)

Are you a Lightdash user? Have you ever had to change the name of a metric, dimension or model in your dbt project?

If so, you'd know for sure that adapting the Lightdash charts to these changes is a huge pain.

This python package aims to partially solve this issue automatically updating the Lightdash database.

It works pretty well most of the times, but there are still some corner cases when you'll find your charts a little bit different after the migration. Anyway, this package will still save you hours of manual updates.

## How to install Jolteon

```
pip install jolteon
```

## How to use Jolteon

1. Create a `.env` file like the `.env.example` one you find in this repository and fill it with your Lightdash database connection parameters.

2. Create a `config.yaml` file like the `config_example.yaml` one you find in this repository. This file should be structured as follows:

    - `old_table` should be filled with the previous name of your dbt model (if you have changed it) or with the current name of it (if you haven't changed it).

    - `new_table` should be filled with the current name of your dbt model only when you have changed it, otherwise it should be left empty.

    - `fields_raw_mapping` should be filled with the mapping of the metrics and the dimensions you have changed. If you haven't changed any metric or dimension, you can also leave it empty.

    - `query_ids` should be filled with the ids of the charts you want to affect when updating the database. If you don't known what are the ids of the charts (and you probably won't the first time), you can run `jolteon get-ids`. You will be presented with a table containing the id, the name and the workspace of all the charts of your Lightdash instance.

3. Run `jolteon update-db config.yaml`.
