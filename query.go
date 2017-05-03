package sqlbuild

import (
	"fmt"
	"strconv"
	"strings"
)

type query struct {
	table     string
	distinct  string
	join      string
	field     string
	condition string
	orderBy   string
	groupBy   string
	having    string
	offset    int
	limit     int
	params    []interface{}
	engine    *engine
}

func (q *query) Select(v string) *query {
	q.field = v
	return q
}

func (q *query) Distinct(v string) *query {
	q.distinct = v
	return q
}

func (q *query) Join(t, table, condition string) *query {
	q.join += fmt.Sprintf(" %s JOIN %s ON %s", t, table, condition)
	return q
}

func (q *query) LeftJoin(table, condition string) *query {
	q.join += fmt.Sprintf(" LEFT JOIN %s ON %s", table, condition)
	return q
}

func (q *query) RightJoin(table, condition string) *query {
	q.join += fmt.Sprintf(" RIGHT JOIN %s ON %s", table, condition)
	return q
}

func (q *query) InnerJoin(table, condition string) *query {
	q.join += fmt.Sprintf(" INNER JOIN %s ON %s", table, condition)
	return q
}

func (q *query) Form(v string) *query {
	q.table = v
	return q
}

func (q *query) Where(v string, avg ...interface{}) *query {
	q.condition = fmt.Sprintf(" WHERE %s", v)
	buffer := make([]interface{}, len(q.params)+len(avg))
	copy(buffer, q.params)
	copy(buffer[len(q.params):], avg)
	q.params = buffer
	return q
}

func (q *query) AndWhere(v string, avg ...interface{}) *query {
	q.condition += fmt.Sprintf(" AND ( %s )", v)
	buffer := make([]interface{}, len(q.params)+len(avg))
	copy(buffer, q.params)
	copy(buffer[len(q.params):], avg)
	q.params = buffer
	return q
}

func (q *query) OrWhere(v string, avg ...interface{}) *query {
	q.condition += fmt.Sprintf(" OR ( %s )", v)
	buffer := make([]interface{}, len(q.params)+len(avg))
	copy(buffer, q.params)
	copy(buffer[len(q.params):], avg)
	q.params = buffer
	return q
}

func (q *query) OrderBy(v string) *query {
	q.orderBy = fmt.Sprintf(" ORDER BY %s", v)
	return q
}

func (q *query) Having(v string) *query {
	q.having = fmt.Sprintf(" HAVING %s", v)
	return q
}

func (q *query) GroupBy(v string) *query {
	q.groupBy = fmt.Sprintf(" GROUP BY %s", v)
	return q
}

func (q *query) Offset(v int) *query {
	q.offset = v
	return q
}

func (q *query) Limit(v int) *query {
	q.limit = v
	return q
}

func (q *query) ToSql() (string, []interface{}) {
	str := "SELECT [DISTINCT] [FIELD] FROM [TABLE][JOIN][WHERE][GROUP][HAVING][ORDER][LIMIT]"
	var limit string
	if q.offset > 0 && q.limit > 0 {
		limit = " LIMIT " + strconv.Itoa(q.offset) + "," + strconv.Itoa(q.limit)
	} else if q.limit > 0 {
		limit = " LIMIT " + strconv.Itoa(q.limit)
	}
	pairs := map[string]string{
		"[TABLE]":    q.table,
		"[DISTINCT]": q.distinct,
		"[FIELD]":    q.field,
		"[JOIN]":     q.join,
		"[WHERE]":    q.condition,
		"[GROUP]":    q.groupBy,
		"[HAVING]":   q.having,
		"[ORDER]":    q.orderBy,
		"[LIMIT]":    limit,
	}
	for k, v := range pairs {
		str = strings.Replace(str, k, v, 1)
	}
	return str, q.params
}

func (q *query) Count() int {
	q.Select("COUNT(*) as COUNT")
	data, err := q.One()
	if err != nil {
		return 0
	}
	if v, err := strconv.Atoi(data["COUNT"]); err != nil {
		return 0
	} else {
		return v
	}
}

func (q *query) One() (map[string]string, error) {
	q.Limit(1)
	data, err := q.All()
	if err != nil || len(data) <= 0 {
		return nil, err
	}
	return data[0], nil
}


func (q *query) All() ([]map[string]string, error) {
	str, args := q.ToSql()
	data,err :=  q.engine.Query(str, args...)
	if err != nil{
		return nil,err
	}
	result := make([]map[string]string,0,len(data))
	for _,val := range data{
		row := make(map[string]string)
		for k,v := range val{
			row[k] = string(v)
		}
		result = append(result,row)
	}
	return result,nil
}