package main

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestRenderDefaultTemplate(t *testing.T) {
	expect := `---
version: 2

sources:
  - name: my_dataset
    database: my-project
    tables:
      # dbt source ID: source("my_dataset", "my_project__my_dataset__my_table")
      - name: my_project__my_dataset__my_table
        identifier: my_dataset

        loaded_at_field: CreatedAt
        freshness:
          warn_after:
            count: 24
            period: hour  # minute | hour | day
          error_after:
            count: 36
            period: hour  # minute | hour | day

        description: |-
          This
            is
              table
          description

        tags: []

        meta: {}

        columns:
          - name: col1
            description: col1 description
          - name: col2
            description: col2 description
          - name: col3
            description: col3 description
`

	temp, _ := makeTemplate("")
	meta := &bigquery.TableMetadata{
		Name: "my_table",
		Description: `
This
  is
    table
description`,
		Schema: bigquery.Schema{
			&bigquery.FieldSchema{
				Name:        "col1",
				Description: "col1 description",
			},
			&bigquery.FieldSchema{
				Name:        "col2",
				Description: "col2 description",
			},
			&bigquery.FieldSchema{
				Name:        "col3",
				Description: "col3 description",
			},
		},
	}
	source := makeDbtSource("my-project", "my_dataset", "my_table", meta)

	tmpFile, _ := ioutil.TempFile("", "tmptest")
	defer os.Remove(tmpFile.Name())

	temp.Execute(tmpFile, source)

	content, err := ioutil.ReadFile(tmpFile.Name())
	if err != nil {
		log.Fatal(err)
	}
	out := string(content)

	if expect != out {
		t.Errorf(
			"failed to render template.\nout:\n%s\n\nexpect:\n%s\n",
			out, expect)
	}
}
