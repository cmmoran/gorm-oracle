package oracle

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"
)

const (
	procCreateExamplePagingQuery = `-- example procedure
create or replace PROCEDURE PRO_EXAMPLE_PAGING_QUERY (
	BASIC_SQL IN VARCHAR2,   -- Base Query SQL
	ORDER_FIELD IN VARCHAR2, -- Order Field
	PAGE_NUM IN NUMBER,	  -- Current Page Number
	PAGE_SIZE IN NUMBER,	 -- Page Size

	TOTAL_NUM OUT NUMBER,		-- Return Total Count 
	RES_CURSOR OUT SYS_REFCURSOR -- Return Result Set
)
AS
BEGIN
	DECLARE
		PAGING_SQL VARCHAR2(4000) := '';  -- Paging Query SQL
		TOTAL_SQL VARCHAR2(4000) := '';   -- Total Count Query SQL
		OFFSET NUMBER(10); -- Paging Query Offset
	BEGIN
		-- Query Total Count
		TOTAL_SQL := 'SELECT TO_NUMBER(COUNT(*)) FROM (' || BASIC_SQL || ') TB';
		EXECUTE IMMEDIATE TOTAL_SQL INTO TOTAL_NUM;

		-- Paging Query
		OFFSET := (PAGE_NUM - 1) * PAGE_SIZE;
		PAGING_SQL := 'SELECT * FROM (SELECT T.*, ROW_NUMBER() OVER (ORDER BY ' || ORDER_FIELD ||
					  ') AS ROW_NUM FROM (' || BASIC_SQL || ') T) WHERE ROW_NUM BETWEEN ' ||
					  TO_CHAR(OFFSET+1) || ' AND ' || TO_CHAR(OFFSET+PAGE_SIZE);

		OPEN RES_CURSOR FOR PAGING_SQL;
	END;
END PRO_EXAMPLE_PAGING_QUERY;`
)

func ExampleRefCursor_Query() {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil || db == nil {
		log.Fatal(err)
	}
	if err = db.Exec(procCreateExamplePagingQuery).Error; err != nil {
		log.Fatal(err)
	}
	var (
		totalNum  uint
		resCursor RefCursor

		values = []any{
			"SELECT * FROM USER_TABLES",
			"TABLE_NAME",
			1,
			10,
			sql.Out{Dest: &totalNum},
			sql.Out{Dest: &resCursor.RefCursor},
		}
	)
	// Execute Stored Procedure
	if err = db.Exec(`
	BEGIN
		PRO_EXAMPLE_PAGING_QUERY(:BASIC_SQL, :ORDER_FIELD, :PAGE_NUM, :PAGE_SIZE, :TOTAL_NUM, :RES_CURSOR);
	END;`, values...).Error; err != nil {
		log.Fatal(err)
	}
	defer func(cursor *RefCursor) {
		_ = cursor.Close()
	}(&resCursor)

	// Read Cursor
	var dataset *DataSet
	if dataset, err = resCursor.Query(); err != nil {
		log.Fatal(err)
	}
	defer func(dataset *DataSet) {
		_ = dataset.Close()
	}(dataset)

	var dataRows []map[string]any
	columns := dataset.Columns()
	dest := make([]driver.Value, len(columns))
	for {
		if err = dataset.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			break
		}
		dataRow := make(map[string]any, len(columns))
		for i, v := range dest {
			dataRow[columns[i]] = v
		}
		dataRows = append(dataRows, dataRow)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(dataRows) > 0)
	//Output: true
}

func TestExecProcedure(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}
	if err = db.Exec(procCreateExamplePagingQuery).Error; err != nil {
		t.Fatal(err)
	}

	var (
		totalNum  uint
		resCursor RefCursor

		values = []any{
			"SELECT * FROM USER_TABLES",         // sql.Named("BASIC_SQL", "SELECT * FROM USER_TABLES"),
			"TABLE_NAME",                        // sql.Named("ORDER_FIELD", "TABLE_NAME"),
			1,                                   // sql.Named("PAGE_NUM", 1),
			10,                                  // sql.Named("PAGE_SIZE", 10),
			sql.Out{Dest: &totalNum},            // sql.Named("TOTAL_NUM", sql.Out{Dest: &totalNum}),
			sql.Out{Dest: &resCursor.RefCursor}, // sql.Named("RES_CURSOR", sql.Out{Dest: &resCursor.RefCursor}),
		}
	)
	// Execute Stored Procedure
	if err = db.Exec(`
	BEGIN
		PRO_EXAMPLE_PAGING_QUERY(:BASIC_SQL, :ORDER_FIELD, :PAGE_NUM, :PAGE_SIZE, :TOTAL_NUM, :RES_CURSOR);
	END;`, values...).Error; err != nil {
		t.Fatal(err)
	}
	defer func(cursor *RefCursor) {
		_ = cursor.Close()
	}(&resCursor)

	// Read Cursor
	var dataset *DataSet
	if dataset, err = resCursor.Query(); err != nil {
		t.Fatal(err)
	}
	defer func(dataset *DataSet) {
		_ = dataset.Close()
	}(dataset)

	var dataRows []map[string]any
	columns := dataset.Columns()
	dest := make([]driver.Value, len(columns))
	for {
		if err = dataset.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			break
		}
		dataRow := make(map[string]any, len(columns))
		for i, v := range dest {
			dataRow[columns[i]] = v
		}
		dataRows = append(dataRows, dataRow)
	}
	if err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(dataRows)
	t.Logf("got total: %d, got size: %d, got data:\n%s", totalNum, len(dataRows), got)
}
