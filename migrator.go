package migration

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
)

// CreateSchema ...
func CreateSchema(db *pg.DB, models ...interface{}) error {
	if len(models) == 0 {
		return nil
	}
	return db.RunInTransaction(func(tx *pg.Tx) error {
		for _, model := range models {
			if err := createTable(tx, model); err != nil {
				return err
			}
		}
		return nil
	})
}

func createTable(db *pg.Tx, model interface{}) error {
	err := db.CreateTable(model, &orm.CreateTableOptions{
		IfNotExists:   true,
		FKConstraints: true,
	})
	if err != nil {
		return err
	}

	if err = handleTagCpg(db, model, nil); err != nil {
		return err
	}

	return nil
}

func handleTagCpg(db *pg.Tx, model interface{}, rootModel interface{}) error {
	if rootModel == nil {
		rootModel = model
	}
	t := reflect.TypeOf(model).Elem()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Anonymous {
			err := handleTagCpg(db, reflect.New(field.Type).Interface(), rootModel)
			if err != nil {
				return err
			}
			continue
		}
		tag := field.Tag.Get("cpg")
		if tag != "" {
			tagSplit := strings.Split(tag, ":")
			if tagSplit[0] == "index" {
				err := createIndex(db, rootModel, tagSplit[1], underscore(field.Name))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func underscore(s string) string {
	r := make([]byte, 0, len(s)+5)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUpper(c) {
			if i > 0 && i+1 < len(s) && (isLower(s[i-1]) || isLower(s[i+1])) {
				r = append(r, '_', toLower(c))
			} else {
				r = append(r, toLower(c))
			}
		} else {
			r = append(r, c)
		}
	}
	return string(r)
}

func isUpper(c byte) bool {
	return c >= 'A' && c <= 'Z'
}

func isLower(c byte) bool {
	return c >= 'a' && c <= 'z'
}

func toUpper(c byte) byte {
	return c - 32
}

func toLower(c byte) byte {
	return c + 32
}

func getTableName(db *pg.Tx, model interface{}) (string, error) {
	var tableName string
	_, err := db.Model(model).Query(&tableName, "SELECT '?TableName'")
	if err != nil {
		return "", err
	}
	return tableName, nil
}

func createIndex(db *pg.Tx, model interface{}, indexType string, indexColumn string) error {
	tableName, err := getTableName(db, model)
	if err != nil {
		return err
	}
	_, err = db.Model(model).Exec(
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s USING %s (%s)", tableName, indexColumn, tableName, indexType, indexColumn),
	)
	if err != nil {
		return err
	}
	return nil
}
