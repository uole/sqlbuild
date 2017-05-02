package sqlbuild

import (
	"database/sql"
)

type engine struct {
	db      *sql.DB
	query   *query
	command *command
}

func (o *engine) CreateQuery() *query {
	o.query = &query{
		engine:o,
	}
	return o.query
}

func (o *engine) CreateCommand() *command {
	o.command = &command{
		engine:o,
	}
	return o.command
}

func (o *engine) Open(driverName, dataSourceName string) error {
	var err error
	if o.db, err = sql.Open(driverName, dataSourceName); err != nil {
		return err
	}
	return nil
}

func (o *engine) Execute(str string, args ...interface{}) (int64, error) {
	if res, err := o.db.Exec(str, args...); err != nil {
		return 0, err
	} else {
		return res.RowsAffected()
	}
}

func (o *engine) Query(str string, args ...interface{}) ([]map[string]sql.RawBytes, error) {
	stmt, err := o.db.Prepare(str)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	res, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	columns, err := res.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	result := make([]map[string]sql.RawBytes, 0)
	for res.Next() {
		err = res.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		value := make(map[string]sql.RawBytes)
		for i, col := range values {
			value[columns[i]] = col
		}
		result = append(result, value)
	}
	return result, nil
}

func (o *engine) Close() {
	if o.db != nil {
		o.db.Close()
	}
}

func Open(driverName, dataSourceName string) (*engine, error) {
	o := &engine{}
	if err := o.Open(driverName, dataSourceName); err != nil {
		return nil, err
	}
	return o, nil
}
