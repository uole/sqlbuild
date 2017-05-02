# sqlbuild
a light sql build lib

# Usage

go get -u uole/sqlbuild


```
tableName := "test"
query := engine.CreateQuery()
data, err := query.Select("*").Form(tableName).Where("id=?", 1).One()

fmt.Println(data,err)

```
