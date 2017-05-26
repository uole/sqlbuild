package sqlbuild

import (
	"database/sql"
	"log"
)

type Engine interface {
	Open(driverName, dataSourceName string) error
	Close()
	Query(str string, args ...interface{}) ([]map[string][]byte, error)
	Execute(str string, args ...interface{}) (int64, error)
}

type Context struct {
	db           *sql.DB
	query        *query
	debug        bool
	command      *command
	insertId     int64
	affectedRows int64
}

func (o *Context) SetDebug(debug bool) {
	o.debug = true
}

func (o *Context) CreateQuery() *query {
	if o.query == nil {
		o.query = &query{
			engine: o,
		}
	} else {
		o.query.Flush()
	}
	return o.query
}

func (o *Context) CreateCommand() *command {
	if o.command == nil {
		o.command = &command{
			engine: o,
		}
	} else {
		o.command.Flush()
	}
	return o.command
}

func (o *Context) Open(driverName, dataSourceName string) error {
	var err error
	if o.db, err = sql.Open(driverName, dataSourceName); err != nil {
		return err
	}
	return nil
}

func (o *Context) Execute(str string, args ...interface{}) (int64, error) {
	if o.debug {
		log.Println("query sql:", str)
	}
	if res, err := o.db.Exec(str, args...); err != nil {
		return 0, err
	} else {
		o.insertId, _ = res.LastInsertId()
		o.affectedRows, _ = res.RowsAffected()
		return res.RowsAffected()
	}
}

func (o *Context) Query(str string, args ...interface{}) ([]map[string][]byte, error) {
	if o.debug {
		log.Println("query sql:", str)
	}
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
	values := make([][]byte, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	result := make([]map[string][]byte, 0, 0)
	for res.Next() {
		err = res.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		value := make(map[string][]byte)
		for i, col := range values {
			value[columns[i]] = col
		}
		result = append(result, value)
	}
	return result, nil
}

func (o *Context) Close() {
	if o.db != nil {
		o.db.Close()
	}
}

func (o *Context) InsertId() int64 {
	return o.insertId
}

func (o *Context) AffectedRows() int64 {
	return o.affectedRows
}

func Open(driverName, dataSourceName string) (*Context, error) {
	o := &Context{}
	if err := o.Open(driverName, dataSourceName); err != nil {
		return nil, err
	}
	return o, nil
}
