package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"gorm.io/gorm/clause"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	dbNamingCase *gorm.DB
	dbIgnoreCase *gorm.DB

	dbErrors = make([]error, 2)
)

func init() {
	if wait := os.Getenv("GORM_ORA_WAIT_MIN"); wait != "" {
		if waitMin, e := strconv.Atoi(wait); e == nil {
			log.Println("wait for oracle database initialization to complete...")
			time.Sleep(time.Duration(waitMin) * time.Minute)
		}
	}
	var err error
	if dbNamingCase, err = openTestConnection(true, true); err != nil {
		dbErrors[0] = err
	}
	if dbIgnoreCase, err = openTestConnection(true, false); err != nil {
		dbErrors[1] = err
	}
}

func openTestConnection(ignoreCase, namingCase bool) (db *gorm.DB, err error) {
	dsn := getTestDSN()

	db, err = gorm.Open(New(Config{
		DSN:                 dsn,
		IgnoreCase:          ignoreCase,
		NamingCaseSensitive: namingCase,
	}), getTestGormConfig())
	if db != nil && err == nil {
		log.Println("open oracle database connection success!")
	}
	return
}

func getTestDSN() (dsn string) {
	dsn = os.Getenv("GORM_ORA_DSN")
	if dsn == "" {
		server := os.Getenv("GORM_ORA_SERVER")
		port, _ := strconv.Atoi(os.Getenv("GORM_ORA_PORT"))
		if server == "" || port < 1 {
			return
		}

		language := os.Getenv("GORM_ORA_LANG")
		if language == "" {
			language = "ENGLISH"
		}
		territory := os.Getenv("GORM_ORA_TERRITORY")
		if territory == "" {
			territory = "AMERICA"
		}

		dsn = BuildUrl(server, port,
			os.Getenv("GORM_ORA_SID"),
			os.Getenv("GORM_ORA_USER"),
			os.Getenv("GORM_ORA_PASS"),
			map[string]string{
				"CONNECTION TIMEOUT": "90",
				"LANGUAGE":           language,
				"TERRITORY":          territory,
				"SSL":                "false",
			})
	}
	return
}

func getTestGormConfig() *gorm.Config {
	logWriter := new(log.Logger)
	logWriter.SetOutput(os.Stdout)

	return &gorm.Config{
		Logger: logger.New(
			logWriter,
			logger.Config{LogLevel: logger.Info},
		),
		DisableForeignKeyConstraintWhenMigrating: false,
		IgnoreRelationshipsWhenMigrating:         false,
		NamingStrategy: schema.NamingStrategy{
			IdentifierMaxLength: 30,
		},
	}
}

func TestCountLimit0(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	model := TestTableUser{}
	migrator := db.Set("gorm:table_comments", "User Information Table").Migrator()
	if migrator.HasTable(model) {
		if err = migrator.DropTable(model); err != nil {
			t.Fatalf("DropTable() error = %v", err)
		}
	}
	if err = migrator.AutoMigrate(model); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	t.Log("AutoMigrate() success!")

	var count int64
	result := db.Model(&model).Limit(-1).Count(&count)
	if err = result.Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("Limit(-1) count = %d", count)

	if countSql := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Model(&model).Limit(-1).Count(&count)
	}); strings.Contains(countSql, "ORDER BY") {
		t.Error(`The "count(*)" statement contains the "ORDER BY" clause!`)
	}
}

func TestReturning(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}
	ctx := func() context.Context {
		return context.Background()
	}

	err = db.AutoMigrate(TestTableUser{})
	assert.NoError(t, err, "expecting no error")
	model := TestTableUser{
		UID:         "U1",
		Name:        "Lisa",
		Account:     "lisa",
		Password:    "H6aLDNr",
		PhoneNumber: "+8616666666666",
		Sex:         "0",
		UserType:    1,
		Birthday:    ptr(time.Date(1978, 5, 1, 0, 0, 0, 0, time.UTC).UTC()),
		Enabled:     true,
	}
	res := db.WithContext(ctx()).Create(&model)
	assert.NoError(t, err, "expecting no error")
	modelId := model.ID
	res = db.WithContext(ctx()).First(&model)
	assert.NoError(t, err, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	model.Name = "Bob"
	model.Sex = "m"
	model.Enabled = true
	model.PEnabled = ptr(true)
	res = db.WithContext(ctx()).Updates(model)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, "Bob", model.Name, "expecting 'Bob'")
	assert.Equal(t, "m", model.Sex, "expecting 'm'")
	assert.Equal(t, true, model.Enabled, "expecting 'true'")
	assert.Equal(t, ptr(true), model.PEnabled, "expecting '*true'")

	res = db.WithContext(ctx()).Model(&model).Update("name", "Alice").Update("sex", "f").Update("enabled", false)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "Alice", model.Name)
	assert.Equal(t, "f", model.Sex, "expecting 'f'")

	res = db.WithContext(ctx()).Model(&model).Updates(map[string]any{
		"name": "Bob",
		"sex":  "b",
	})
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, "Bob", model.Name)
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "b", model.Sex, "expecting 'b'")

	model.PEnabled = ptr(false)
	res = db.WithContext(ctx()).Model(&model).Clauses(clause.Returning{}).Updates(map[string]any{
		"name":      "charlie",
		"sex":       "m",
		"enabled":   true,
		"penabled":  ptr(true),
		"user_type": gorm.Expr("\"user_type\" + 1"),
	})
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "charlie", model.Name, "expecting 'charlie'")
	assert.Equal(t, "m", model.Sex, "expecting 'm'")
	assert.Equal(t, true, model.Enabled, "expecting 'true'")
	assert.Equal(t, ptr(true), model.PEnabled, "expecting '*true'")

	tm := &TestTableUser{
		ID:   modelId,
		Name: "doug",
	}
	res = db.WithContext(ctx()).Clauses(clause.Returning{}).Updates(tm)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "doug", tm.Name, "expecting 'doug'")
	assert.Equal(t, "m", tm.Sex, "expecting 'm'")
	assert.Equal(t, true, tm.Enabled, "expecting 'true'")
	assert.Equal(t, 2, tm.UserType, "expecting '2'")
	assert.Equal(t, "1978-05-01 00:00:00", tm.Birthday.Format("2006-01-02 15:04:05"), "expecting '1978-05-01 00:00:00'")
	assert.Equal(t, true, *tm.PEnabled, "expecting '(*bool)true'")

	ttm := &TestTableUser{
		ID:   modelId,
		Name: "evan",
	}
	res = db.WithContext(ctx()).Updates(ttm)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "evan", ttm.Name, "expecting 'evan'")
	assert.Equal(t, "", ttm.Sex, "expecting ''")
	assert.Equal(t, false, ttm.Enabled, "expecting 'false'")
	assert.Equal(t, 0, ttm.UserType, "expecting '0'")
	assert.Nil(t, ttm.Birthday, "expecting '(*time.Time)nil'")
	assert.Nil(t, ttm.PEnabled, "expecting '(*bool)nil'")
}

func ptr[T any](v T) *T {
	return &v
}

func TestLimit(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}
	TestMergeCreate(t)

	type args struct {
		offset, limit int
		order         string
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "OffsetLimit0", args: args{offset: 0, limit: 0}},
		{name: "Offset10", args: args{offset: 10, limit: 0}},
		{name: "Limit10", args: args{offset: 0, limit: 10}},
		{name: "Offset10Limit10", args: args{offset: 10, limit: 10}},
		{name: "Offset10Limit10Order", args: args{offset: 10, limit: 10, order: `"id"`}},
		{name: "Offset10Limit10OrderDESC", args: args{offset: 10, limit: 10, order: `"id" DESC`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var data []TestTableUser
			result := db.Model(&TestTableUser{}).
				Offset(tt.args.offset).
				Limit(tt.args.limit).
				Order(tt.args.order).
				Find(&data)
			if err = result.Error; err != nil {
				t.Fatal(err)
			}
			dataBytes, _ := json.MarshalIndent(data, "", "  ")
			t.Logf("Offset(%d) Limit(%d) got size = %d, data = %s",
				tt.args.offset, tt.args.limit, len(data), dataBytes)
		})
	}
}

func TestAddSessionParams(t *testing.T) {
	db, err := dbIgnoreCase, dbErrors[1]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}
	var sqlDB *sql.DB
	if sqlDB, err = db.DB(); err != nil {
		t.Fatal(err)
	}
	type args struct {
		params map[string]string
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "TimeParams", args: args{params: map[string]string{
			"TIME_ZONE":               "+08:00",                       // alter session set TIME_ZONE = '+08:00';
			"NLS_DATE_FORMAT":         "YYYY-MM-DD",                   // alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD';
			"NLS_TIME_FORMAT":         "HH24:MI:SSXFF",                // alter session set NLS_TIME_FORMAT = 'HH24:MI:SS.FF3';
			"NLS_TIMESTAMP_FORMAT":    "YYYY-MM-DD HH24:MI:SSXFF",     // alter session set NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3';
			"NLS_TIME_TZ_FORMAT":      "HH24:MI:SS.FF TZR",            // alter session set NLS_TIME_TZ_FORMAT = 'HH24:MI:SS.FF3 TZR';
			"NLS_TIMESTAMP_TZ_FORMAT": "YYYY-MM-DD HH24:MI:SSXFF TZR", // alter session set NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3 TZR';
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//queryTime := `SELECT SYSDATE FROM DUAL`
			queryTime := `SELECT CAST(SYSDATE AS VARCHAR(30)) AS D FROM DUAL`
			var timeStr string
			if err = db.Raw(queryTime).Row().Scan(&timeStr); err != nil {
				t.Fatal(err)
			}
			t.Logf("SYSDATE 1: %s", timeStr)

			var keys []string
			if keys, err = AddSessionParams(sqlDB, tt.args.params); err != nil {
				t.Fatalf("AddSessionParams() error = %v", err)
			}
			if err = db.Raw(queryTime).Row().Scan(&timeStr); err != nil {
				t.Fatal(err)
			}
			defer DelSessionParams(sqlDB, keys)
			t.Logf("SYSDATE 2: %s", timeStr)
			t.Logf("keys: %#v", keys)
		})
	}
}

func TestGetStringExpr(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	type args struct {
		prepareSQL string
		value      string
		quote      bool
	}
	tests := []struct {
		name    string
		args    args
		wantSQL string
	}{
		{"1", args{`SELECT ? AS HELLO FROM DUAL`, "Hi!", true}, `SELECT 'Hi!' AS HELLO FROM DUAL`},
		{"2", args{`SELECT '?' AS HELLO FROM DUAL`, "Hi!", false}, `SELECT 'Hi!' AS HELLO FROM DUAL`},
		{"3", args{`SELECT ? AS HELLO FROM DUAL`, "What's your name?", true}, `SELECT q'[What's your name?]' AS HELLO FROM DUAL`},
		{"4", args{`SELECT '?' AS HELLO FROM DUAL`, "What's your name?", false}, `SELECT 'What''s your name?' AS HELLO FROM DUAL`},
		{"5", args{`SELECT ? AS HELLO FROM DUAL`, "What's up]'?", true}, `SELECT q'{What's up]'?}' AS HELLO FROM DUAL`},
		{"6", args{`SELECT ? AS HELLO FROM DUAL`, "What's up]'}'?", true}, `SELECT q'<What's up]'}'?>' AS HELLO FROM DUAL`},
		{"7", args{`SELECT ? AS HELLO FROM DUAL`, "What's up]'}'>'?", true}, `SELECT q'(What's up]'}'>'?)' AS HELLO FROM DUAL`},
		{"8", args{`SELECT ? AS HELLO FROM DUAL`, "What's up)'}'>'?", true}, `SELECT q'[What's up)'}'>'?]' AS HELLO FROM DUAL`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
				return tx.Raw(tt.args.prepareSQL, GetStringExpr(tt.args.value, tt.args.quote))
			})
			if !reflect.DeepEqual(gotSQL, tt.wantSQL) {
				t.Fatalf("ToSQL = %v, want %v", gotSQL, tt.wantSQL)
			}
			var results []map[string]interface{}
			if err = db.Raw(gotSQL).Find(&results).Error; err != nil {
				t.Fatalf("finds all records from raw sql got error: %v", err)
			}
			t.Log("result:", results)
		})
	}
}

func TestVarcharSizeIsCharLength(t *testing.T) {
	dsn := getTestDSN()

	db, err := gorm.Open(New(Config{
		DSN:                     dsn,
		IgnoreCase:              true,
		NamingCaseSensitive:     true,
		VarcharSizeIsCharLength: true,
	}), getTestGormConfig())
	if db != nil && err == nil {
		log.Println("open oracle database connection success!")
	} else {
		t.Fatal(err)
	}

	model := TestTableUserVarcharSize{}
	migrator := db.Set("gorm:table_comments", "TestVarcharSizeIsCharLength").Migrator()
	if migrator.HasTable(model) {
		if err = migrator.DropTable(model); err != nil {
			t.Fatalf("DropTable() error = %v", err)
		}
	}
	if err = migrator.AutoMigrate(model); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	t.Log("AutoMigrate() success!")

	type args struct {
		value string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"50", args{strings.Repeat("Nm", 25)}, false},
		{"60", args{strings.Repeat("Nm", 30)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := db.Create(&TestTableUserVarcharSize{TestTableUser{Name: tt.args.value}}).Error
			if (gotErr != nil) != tt.wantErr {
				t.Error(gotErr)
			} else if gotErr != nil {
				t.Log(gotErr)
			}
		})
	}
}

type TestTableUserVarcharSize struct {
	TestTableUser
}

func (TestTableUserVarcharSize) TableName() string {
	return "test_user_varchar_size"
}
