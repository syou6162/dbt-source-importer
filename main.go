package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"cloud.google.com/go/bigquery"
	"github.com/Masterminds/sprig"
)

const templateSourceContent = `---
version: 2

sources:
  - name: {{ .Dataset }}
    database: {{ .Project }}
    tables:
      # dbt source ID: source("{{ .Dataset }}", "{{ .Project | replace "-" "_" }}__{{ .Dataset }}__{{ .Table }}")
      - name: {{ .Project | replace "-" "_" }}__{{ .Dataset }}__{{ .Table }}
        identifier: {{ .Dataset }}

        loaded_at_field: CreatedAt
        freshness:
          warn_after:
            count: 24
            period: hour  # minute | hour | day
          error_after:
            count: 36
            period: hour  # minute | hour | day

        description: |-
          {{ .Description | nindent 10 | trim }}

        tags: []

        meta: {}

        columns:
          {{- range $_, $column := .Columns }}
          - name: {{ $column.Name }}
            description: {{ $column.Description }}
          {{- end }}`

type Column struct {
	Name        string
	Description string
}

type DbtSource struct {
	Project     string
	Dataset     string
	Table       string
	Description string
	Columns     []Column
}

func extractColumns(schema bigquery.Schema) []Column {
	var columns []Column
	for _, s := range schema {
		columns = append(columns, Column{s.Name, s.Description})
	}
	return columns
}

func renderDbtSourceTemplate(t *template.Template, wr io.Writer, source DbtSource) error {
	if err := t.Execute(wr, source); err != nil {
		return err
	}
	return nil
}

func makeTemplate(templateFilePath string) (*template.Template, error) {
	if templateFilePath == "" {
		t, err := template.New("format").Funcs(sprig.TxtFuncMap()).Parse(templateSourceContent)
		if err != nil {
			return nil, err
		}
		return t, nil
	}
	name := filepath.Base(templateFilePath)
	return template.New(name).Funcs(sprig.TxtFuncMap()).ParseFiles(templateFilePath)
}

func makeDbtSource(project string, dataset string, table string, meta *bigquery.TableMetadata) DbtSource {
	return DbtSource{
		Project:     project,
		Dataset:     dataset,
		Table:       table,
		Description: meta.Description,
		Columns:     extractColumns(meta.Schema),
	}
}

func main() {
	var (
		project          = flag.String("project", "", "GCP project")
		dataset          = flag.String("dataset", "", "Dataset for source table")
		table            = flag.String("table", "", "Source table")
		templateFilePath = flag.String("template", "", "string flag")
	)
	flag.Parse()

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *project)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	meta, err := client.Dataset(*dataset).Table(*table).Metadata(ctx)
	if err != nil {
		log.Fatal(err)
	}
	dbtSource := makeDbtSource(*project, *dataset, *table, meta)

	t, err := makeTemplate(*templateFilePath)
	if err != nil {
		log.Fatal(err)
	}

	dir := fmt.Sprintf(
		"models/%s/%s/%s",
		strings.ReplaceAll(*project, "-", "_"),
		*dataset,
		*table,
	)

	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	outFilename := fmt.Sprintf(
		"%s/src_%s__%s__%s.yml",
		dir,
		strings.ReplaceAll(*project, "-", "_"),
		*dataset,
		*table,
	)
	outFile, err := os.Create(outFilename)
	defer outFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	if err = t.Execute(outFile, dbtSource); err != nil {
		log.Fatal(err)
	}

	fmt.Println(fmt.Sprintf(
		"File %s is generated under for %s.%s.%s",
		filepath.Dir((outFile.Name())),
		*project,
		*dataset,
		*table,
	))
}
