package main

import (
	"context"
	"flag"
	"fmt"
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
            data_type: {{ $column.Type }}
          {{- end }}`

type Column struct {
	Name        string
	Description string
	Type        string
}

type DbtSource struct {
	Project        string
	ProjectForPath string
	Dataset        string
	Table          string
	Description    string
	Columns        []Column
}

func extractColumns(schema bigquery.Schema) []Column {
	var columns []Column
	for _, s := range schema {
		columns = append(columns, Column{s.Name, s.Description, string(s.Type)})
	}
	return columns
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
		Project:        project,
		ProjectForPath: strings.ReplaceAll(project, "-", "_"),
		Dataset:        dataset,
		Table:          table,
		Description:    meta.Description,
		Columns:        extractColumns(meta.Schema),
	}
}

func main() {
	var (
		project          = flag.String("project", "", "GCP project")
		dataset          = flag.String("dataset", "", "Dataset for source table")
		table            = flag.String("table", "", "Source table")
		templateFilePath = flag.String("template", "", "string flag")
		outDir           = flag.String("outdir", "models/{{.ProjectForPath}}/{{.Dataset}}/{{.Table}}", "Output directory")
		outFile          = flag.String("outfile", "src_{{.ProjectForPath}}__{{.Dataset}}__{{.Table}}.yml", "Output file name")
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

	dirTpl, err := template.New("dirName").Parse(*outDir)
	if err != nil {
		log.Fatal(err)
	}
	builder := strings.Builder{}
	if err = dirTpl.Execute(&builder, dbtSource); err != nil {
		log.Fatal(err)
	}

	dir := builder.String()
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	outFileTpl, err := template.New("outFileName").Parse(*outFile)
	if err != nil {
		log.Fatal(err)
	}
	builder.Reset()
	if err = outFileTpl.Execute(&builder, dbtSource); err != nil {
		log.Fatal(err)
	}
	outFilename := builder.String()
	outFilePath := filepath.Join(dir, outFilename)

	f, err := os.Create(outFilePath)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	if err = t.Execute(f, dbtSource); err != nil {
		log.Fatal(err)
	}

	fmt.Println(fmt.Sprintf(
		"File %s is generated under for %s.%s.%s",
		outFilePath,
		*project,
		*dataset,
		*table,
	))
}
