package ar

// package ar encapsulate database access method.

import (
	"database/sql"
	"log"
	"time"

	"github.com/lib/pq"
)

type ActiveRecord struct {
	url string
	DB  *sql.DB
}

func NewActiveRecord() *ActiveRecord {
	return &ActiveRecord{}
}

func (ar *ActiveRecord) Connect(url string) (err error) {
	if ar.DB, err = sql.Open("postgres", url); err != nil {
		return err
	}
	return nil
}

func (ar *ActiveRecord) Close() {
	ar.DB.Close()
}

func (ar *ActiveRecord) Exec(query string, args ...interface{}) (sql.Result, error) {
	return ar.DB.Exec(query, args...)
}

func (ar *ActiveRecord) GetCount(query string, args ...interface{}) (count int, err error) {
	if err := ar.DB.QueryRow(query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (ar *ActiveRecord) GetRow(query string, args ...interface{}) (map[string]string, error) {
	rows, err := ar.GetRows(query, args...)
	if err != nil || len(rows) == 0 {
		return nil, err
	}

	return rows[0], nil
}

func (ar *ActiveRecord) GetRows(query string, args ...interface{}) (result []map[string]string, err error) {
	log.Printf("query: %s\n", query)

	rows, err := ar.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dest := make([]interface{}, len(columns))
	// fields := make([]interface{}, len(columns))
	fields := make([]sql.NullString, len(columns))
	for i := range fields {
		dest[i] = &fields[i]
	}
	log.Printf("query returned, with %d columns\n", len(columns))

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		r := make(map[string]string)
		for i, v := range fields {
			if v.Valid {
				r[columns[i]] = v.String
			} else {
				r[columns[i]] = ""
			}
		}

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	log.Printf("Finished to gather all documents\n")
	return result, nil
}

// get rows as map[string]interface{}
func (ar *ActiveRecord) GetRowsI(query string, args ...interface{}) (result []map[string]interface{}, err error) {
	log.Printf("query: %s\n", query)

	rows, err := ar.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dest := make([]interface{}, len(columns))
	fields := make([]interface{}, len(columns))
	for i := range fields {
		dest[i] = &fields[i]
	}
	log.Printf("query returned, with %d columns\n", len(columns))

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		r := make(map[string]interface{})
		for i, value := range fields {
			switch v := value.(type) {
			case sql.NullBool:
				if v.Valid {
					r[columns[i]] = v.Bool
				} else {
					r[columns[i]] = nil
				}
			case sql.NullFloat64:
				if v.Valid {
					r[columns[i]] = v.Float64
				} else {
					r[columns[i]] = nil
				}
			case sql.NullInt64:
				if v.Valid {
					r[columns[i]] = v.Int64
				} else {
					r[columns[i]] = nil
				}
			case sql.NullString:
				if v.Valid {
					r[columns[i]] = v.String
				} else {
					r[columns[i]] = nil
				}
			case pq.NullTime:
				if v.Valid {
					r[columns[i]] = v.Time
				} else {
					r[columns[i]] = nil
				}
			case sql.RawBytes:
				r[columns[i]] = string(v)
			case nil:
				r[columns[i]] = nil
			case []uint8:
				r[columns[i]] = string(v)
			case time.Time:
				r[columns[i]] = v
			case int64:
				r[columns[i]] = v
			default:
				log.Printf("column %s is unknow type %v\n", columns[i], v)
				r[columns[i]] = v
			}
		}

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	log.Printf("Finished to gather all documents\n")
	return result, nil
}
