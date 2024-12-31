package impala

import (
	"fmt"

	"github.com/xo/usql/drivers"
	"github.com/xo/usql/drivers/metadata"
)

type MetaReader struct {
	s metadata.LoggingReader
}

func (m *MetaReader) Catalogs(filter metadata.Filter) (*metadata.CatalogSet, error) {
	return metadata.NewCatalogSet([]metadata.Catalog{
		{
			"default",
		},
	}), nil
}

func (m *MetaReader) Schemas(filter metadata.Filter) (*metadata.SchemaSet, error) {
	qstr := "SHOW SCHEMAS"
	var args []any
	if filter.Schema != "" {
		qstr += " LIKE $1"
		args = append(args, filter.Schema)
	}
	var results []metadata.Schema
	rows, closeRows, err := m.s.Query(qstr)
	if err != nil {
		return nil, err
	}
	defer closeRows()
	var ignore string
	for rows.Next() {
		rec := metadata.Schema{}
		err = rows.Scan(&rec.Schema, &ignore)
		if err != nil {
			return nil, err
		}
		if filter.WithSystem || rec.Schema != "_impala_builtins" {
			results = append(results, rec)
		}
	}
	return metadata.NewSchemaSet(results), nil
}

func (m *MetaReader) Tables(filter metadata.Filter) (*metadata.TableSet, error) {
	schemas, err := m.Schemas(filter)
	if err != nil {
		return nil, err
	}
	var results []metadata.Table
	for schemas.Next() {
		schema := schemas.Get().Schema
		newResults, err := m.getTablesInSchema(filter, schema)
		if err != nil {
			return nil, err
		}
		results = append(results, newResults...)
	}
	return metadata.NewTableSet(results), nil
}

func (m *MetaReader) getTablesInSchema(filter metadata.Filter, schema string) ([]metadata.Table, error) {
	var results []metadata.Table
	qstr := fmt.Sprintf("SHOW TABLES IN `%s`", schema)
	var args []any
	if filter.Name != "" {
		qstr += " LIKE $1"
		args = append(args, filter.Name)
	}
	rows, closeRows, err := m.s.Query(qstr)
	if err != nil {
		return nil, err
	}
	defer closeRows()
	for rows.Next() {
		rec := metadata.Table{}
		rec.Schema = schema
		err = rows.Scan(&rec.Name)
		if err != nil {
			return nil, err
		}
		results = append(results, rec)
	}
	return results, nil
}

func (m *MetaReader) Columns(filter metadata.Filter) (*metadata.ColumnSet, error) {
	//TODO implement me
	panic("implement me")
}

var _ metadata.BasicReader = &MetaReader{}

func New(db drivers.DB, opts ...metadata.ReaderOption) metadata.Reader {
	return &MetaReader{
		s: metadata.NewLoggingReader(db, opts...),
	}
}
