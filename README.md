## dbt-source-importer
[![CI](https://github.com/syou6162/dbt-source-importer/actions/workflows/ci.yaml/badge.svg)](https://github.com/syou6162/dbt-source-importer/actions/workflows/ci.yaml)

dbt-source-importer is a command line tool to import dbt sources. You can easily generate source yaml files from metadata of data warehouse like BigQuery.

## Usage
You can import dbt sources by specifying project, dataset, and table.

```
./dbt-source-importer --project my-project --dataset my_dataset --table my_table
```

You can also use your custom template.

```
./dbt-source-importer --project my-project --dataset my_dataset --table my_table --template templates/source.tmpl.yml
```
