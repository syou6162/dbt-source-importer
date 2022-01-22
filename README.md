## dbt-source-importer
[![CI](https://github.com/syou6162/dbt-source-importer/actions/workflows/ci.yaml/badge.svg)](https://github.com/syou6162/dbt-source-importer/actions/workflows/ci.yaml)
[![version](https://img.shields.io/github/v/release/syou6162/dbt-source-importer)](https://github.com/syou6162/dbt-source-importer/releases)

dbt-source-importer is a command line tool to import dbt sources. You can easily generate source yaml files from metadata of data warehouse like BigQuery.

## Install
You can install dbt-source-importer via `go get` command, or download binary file from [releases](https://github.com/syou6162/dbt-source-importer/releases).

```
go get github.com/syou6162/dbt-source-importer
```

## Usage
You can import dbt sources by specifying project, dataset, and table.

```
./dbt-source-importer --project my-project --dataset my_dataset --table my_table
```

You can also use your custom template.

```
./dbt-source-importer --project my-project --dataset my_dataset --table my_table --template templates/source.tmpl.yml
```
