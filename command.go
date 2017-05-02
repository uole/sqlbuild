package sqlbuild

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
)

type command struct {
	flag      int
	table     string
	column    interface{}
	condition string
	params    []interface{}
	engine    *engine
}

func (c *command) Table(name string) *command {
	c.table = name
	return c
}

func (c *command) Insert(column interface{}) *command {
	c.column = column
	c.flag = 0x01
	return c
}

func (c *command) Update(column interface{}, condition string, args ...interface{}) *command {
	c.column = column
	if condition != "" {
		c.condition = fmt.Sprintf(" WHERE %s ", condition)
	}
	c.params = args
	c.flag = 0x02
	return c
}

func (c *command) Delete(condition string, args ...interface{}) *command {
	if condition != "" {
		c.condition = fmt.Sprintf(" WHERE %s ", condition)
	}
	c.params = args
	c.flag = 0x03
	return c
}

func (c *command) ToSql() (string, []interface{}) {
	var tpl string
	var pairs map[string]string
	var args []interface{}
	if c.flag == 0x01 {
		var str string
		tpl = "INSERT INTO [TABLE] SET [VALUE]"
		str, args, _ = builderColumn(c.column, true)
		pairs = map[string]string{
			"[TABLE]": c.table,
			"[VALUE]": str,
		}
	} else if c.flag == 0x02 {
		var str string
		tpl = "UPDATE [TABLE] SET [VALUE] [WHERE] "
		str, args, _ = builderColumn(c.column, true)
		pairs = map[string]string{
			"[TABLE]": c.table,
			"[VALUE]": str,
			"[WHERE]": c.condition,
		}
		buffer := make([]interface{}, len(args)+len(c.params))
		copy(buffer, args)
		copy(buffer[len(args):], c.params)
		args = buffer
	} else if c.flag == 0x03 {
		tpl = "DELETE FROM [TABLE] [WHERE] "
		pairs = map[string]string{
			"[TABLE]": c.table,
			"[WHERE]": c.condition,
		}
		args = c.params
	}
	for k, v := range pairs {
		tpl = strings.Replace(tpl, k, v, 1)
	}
	return tpl, args
}

func (c *command) Execute() (int64, error) {
	str, args := c.ToSql()
	log.Println("execute sql:" + str)
	return c.engine.Execute(str, args...)
}

// build data column to string
func builderColumn(data interface{}, filter bool) (string, []interface{}, error) {
	refValue := reflect.Indirect(reflect.ValueOf(data))
	refType := refValue.Type()
	str := ""
	args := make([]interface{}, 0)
	if refType.Kind() == reflect.Map {
		data, ok := data.(map[string]interface{})
		if !ok {
			return "", nil, errors.New("invalid type")
		}
		for k, v := range data {
			if filter && isEmpty(reflect.ValueOf(v)) {
				continue
			}
			str = str + k + " = ?,"
			args = append(args, v)
		}
		str = strings.TrimRight(str, ",")
	} else if refType.Kind() == reflect.Struct {
		num := refType.NumField()
		dataMap := make(map[string]interface{})
		for i := 0; i < num; i++ {
			v := refValue.Field(i)
			name := refType.Field(i).Tag.Get("json")
			if name == "" {
				name = refType.Field(i).Name
			}
			dataMap[name] = v.Interface()
		}
		log.Println(dataMap)
		return builderColumn(dataMap, filter)
	} else {
		return "", nil, errors.New("invalid type")
	}
	return str, args, nil
}

func isEmpty(val reflect.Value) bool {
	valType := val.Kind()
	switch valType {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int() == 0
	case reflect.Float32, reflect.Float64:
		return val.Float() == 0
	case reflect.String:
		return val.String() == ""
	case reflect.Interface, reflect.Slice, reflect.Ptr, reflect.Map, reflect.Chan, reflect.Func:
		// Check for empty slices and props
		if val.IsNil() {
			return true
		} else if valType == reflect.Slice || valType == reflect.Map {
			return val.Len() == 0
		}
	case reflect.Struct:
		fieldCount := val.NumField()
		for i := 0; i < fieldCount; i++ {
			field := val.Field(i)
			if field.IsValid() && !isEmpty(field) {
				return false
			}
		}
		return true
	default:
		return false
	}
	return false
}
