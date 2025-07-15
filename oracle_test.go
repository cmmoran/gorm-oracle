package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	gofrs "github.com/gofrs/uuid/v3"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	dbNamingCase *gorm.DB
	dbIgnoreCase *gorm.DB

	testCtx  = context.Background()
	dbErrors = make([]error, 2)
)

type errorF struct {
	l *slog.Logger
}

func (e *errorF) Errorf(format string, args ...interface{}) {
	e.l.Error(fmt.Sprintf(format, args...))
}

func (e *errorF) FailNow() {
	panic("tests failed")
}

func currentContext() context.Context {
	return testCtx
}

func storeContext(ctx context.Context) {
	testCtx = ctx
}

func TestMain(m *testing.M) {
	l := slog.Default()
	t := &errorF{l: l}

	if _, ok := os.LookupEnv("GORM_NO_DB"); !ok {
		startOracleDatabase(t)
		ctx := currentContext()
		dbNamingCase = setupOracleDatabase(t, ctx, true, true, true)
		dbIgnoreCase = setupOracleDatabase(t, ctx, true, false, true)
		defer func() {
			if _, oraContainer := findDbContextInfo(ctx); oraContainer != nil {
				_ = oraContainer.Terminate(ctx)
			}
		}()
	}

	// Run tests
	exitCode := m.Run()

	os.Exit(exitCode)
}

func startOracleDatabase(t require.TestingT) {
	ctx := currentContext()

	user := os.Getenv("GORM_ORA_USER")
	if user == "" {
		user = "test"
	}
	pass := os.Getenv("GORM_ORA_PASS")
	if pass == "" {
		pass = "test"
	}
	env := map[string]string{
		"ORACLE_PASSWORD":   pass,
		"APP_USER":          user,
		"APP_USER_PASSWORD": pass,
	}
	language := os.Getenv("GORM_ORA_LANG")
	if language == "" {
		language = "AMERICAN"
	}
	territory := os.Getenv("GORM_ORA_TERRITORY")
	if territory == "" {
		territory = "AMERICA"
	}

	service := os.Getenv("GORM_ORA_SERVICE")
	if service != "" && service != "FREEPDB1" {
		service = strings.Split(service, ",")[0]
		if len(service) == 0 {
			service = "FREEPDB1"
		}
	}
	var err error
	if _, ok := os.LookupEnv("GORM_ORA_SKIP_CONTAINER"); !ok {
		req := tc.ContainerRequest{
			Image:        "gvenzl/oracle-free:slim",
			ExposedPorts: []string{"1521/tcp"},
			Env:          env,
			WaitingFor:   wait.ForLog("Completed: ALTER DATABASE OPEN").WithStartupTimeout(2 * time.Minute),
		}

		var oraContainer tc.Container
		oraContainer, err = tc.GenericContainer(ctx, tc.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
			Logger:           &ow{},
		})
		require.NoError(t, err, "failed to start container")
		var (
			host string
			port nat.Port
		)
		host, err = oraContainer.Host(ctx)
		require.NoError(t, err, "Failed to get container host")

		port, err = oraContainer.MappedPort(ctx, "1521")
		require.NoError(t, err, "Failed to get mapped port")
		slog.Default().With("host", host, "port", port.Port()).Debug("Oracle Free is running")
		connectionString := BuildUrl(
			host,
			port.Int(),
			service,
			user,
			pass,
			map[string]string{
				"LANGUAGE":  language,
				"TERRITORY": territory,
				"SSL":       "false",
			},
		)

		ctx = context.WithValue(ctx, "dsn", connectionString)
		ctx = context.WithValue(ctx, "db", oraContainer)
	} else {
		host := os.Getenv("GORM_ORA_HOST")
		if host == "" {
			host = "127.0.0.1"
		}
		port := os.Getenv("GORM_ORA_PORT")
		if port == "" {
			port = "1521"
		}
		var iport int
		iport, err = strconv.Atoi(port)
		require.NoError(t, err, "Failed to get env port")

		connectionString := BuildUrl(
			host,
			iport,
			service,
			user,
			pass,
			map[string]string{
				"LANGUAGE":  language,
				"TERRITORY": territory,
				"SSL":       "false",
			},
		)

		ctx = context.WithValue(ctx, "dsn", connectionString)
	}

	storeContext(ctx)
}

func findDbContextInfo(ctx context.Context) (dsn string, oraContainer tc.Container) {
	var (
		okContainer bool
		okDsn       bool
	)
	oraContainer, okContainer = ctx.Value("db").(tc.Container)
	dsn, okDsn = ctx.Value("dsn").(string)
	if !okContainer {
		oraContainer = nil
	}
	if !okDsn {
		panic("no dsn found")
	}
	return
}

func setupOracleDatabase(t require.TestingT, ctx context.Context, ignoreCase, namingCase, useClobForText bool) *gorm.DB {
	l := logger.New(&ow{}, logger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      logger.Info,
	})

	var (
		db     *gorm.DB
		dsn, _ = findDbContextInfo(ctx)
		err    error
	)

	timeGranularity := -time.Microsecond
	//timeGranularity := time.Duration(0)
	if tgStr, ok := os.LookupEnv("GORM_ORA_TIME_GRANULARITY"); ok {
		timeGranularity, err = time.ParseDuration(tgStr)
		require.NoError(t, err, "Failed to parse GORM_ORA_TIME_GRANULARITY")
	}
	sessionTimezone := time.UTC
	if sessionTimezoneStr, ok := os.LookupEnv("GORM_ORA_TZ"); ok {
		sessionTimezone, err = time.LoadLocation(sessionTimezoneStr)
		require.NoError(t, err, "Failed to parse GORM_ORA_TZ")
	}
	db, err = gorm.Open(New(Config{
		DSN:                     dsn,
		VarcharSizeIsCharLength: true,
		UseClobForTextType:      useClobForText,
		IgnoreCase:              ignoreCase,
		NamingCaseSensitive:     namingCase,
		TimeGranularity:         timeGranularity,
		SessionTimezone:         sessionTimezone.String(),
	}), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			IdentifierMaxLength: 30,
		},
		Logger: l,
		NowFunc: func() time.Time {
			tt := time.Now()
			if timeGranularity < 0 {
				tt = tt.Truncate(-timeGranularity)
			} else if timeGranularity > 0 {
				tt = tt.Round(timeGranularity)
			}
			if sessionTimezone != time.Local {
				tt = tt.In(sessionTimezone)
			}
			return tt
		},
	})
	require.NoError(t, err)

	return db
}

type ow struct{}

func (ow) Printf(s string, i ...interface{}) {
	fmt.Printf(fmt.Sprintf("%s\n", s), i...)
}

func getTestGormConfig(logWriter logger.Interface) *gorm.Config {
	if logWriter == nil {
		logWriter = logger.New(&ow{}, logger.Config{
			SlowThreshold: time.Second,
			Colorful:      true,
			LogLevel:      logger.Info,
		})
	}
	return &gorm.Config{
		Logger:                                   logWriter,
		DisableForeignKeyConstraintWhenMigrating: false,
		IgnoreRelationshipsWhenMigrating:         false,
		NamingStrategy: schema.NamingStrategy{
			IdentifierMaxLength: 30,
		},
	}
}

type TestTableTime struct {
	ID   uint64    `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name *string   `gorm:"column:name;size:50;comment:User Name" json:"name"`
	Time time.Time `gorm:"column:time;type:timestamp with time zone;comment:User Time" json:"time"`
}

func (TestTableTime) TableName() string {
	return "test_user_time"
}

type TestTableUUID struct {
	ID   uint64    `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string    `gorm:"column:name;size:50;comment:User Name" json:"name"`
	User uuid.UUID `gorm:"column:user;type:uuid;comment:User UUID" json:"user"`
}

func (TestTableUUID) TableName() string {
	return "test_user_uuid"
}

type TestTableULID struct {
	ID   uint64    `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string    `gorm:"column:name;size:50;comment:User Name" json:"name"`
	User ulid.ULID `gorm:"column:user;type:ulid;comment:User ULID" json:"user"`
}

func (TestTableULID) TableName() string {
	return "test_user_ulid"
}

type TestTableGUUID struct {
	ID   uint64    `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string    `gorm:"column:name;size:50;comment:User Name" json:"name"`
	User uuid.UUID `gorm:"column:user;type:uuid;comment:User UUID" json:"user"`
}

func (TestTableGUUID) TableName() string {
	return "test_user_uuid"
}

type TestTableGofrsUUID struct {
	ID   uint64     `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string     `gorm:"column:name;size:50;comment:User Name" json:"name"`
	User gofrs.UUID `gorm:"column:user;type:uuid;comment:User UUID" json:"user"`
}

func (TestTableGofrsUUID) TableName() string {
	return "test_user_uuid"
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

func TestGUUIDType(t *testing.T) {
	ctx := currentContext()
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(ctx)
	_ = db.Migrator().DropTable(&TestTableGUUID{})
	err := db.Migrator().AutoMigrate(TestTableGUUID{})
	require.NoError(t, err, "expecting no error")

	u := uuid.New()
	test0 := &TestTableGUUID{
		Name: "test0",
		User: u,
	}
	test00 := &TestTableGUUID{
		Name: "test00",
		User: uuid.New(),
	}
	result := db.Create([]*TestTableGUUID{test0, test00})
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(2), "expecting two records created")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	test0 = &TestTableGUUID{}
	result = db.First(test0)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	require.EqualValuesf(t, u, test0.User, "expecting User to match")

	test1 := &TestTableGUUID{}
	result = db.Model(test1).Where(`"user" = ?`, test00.User).Scan(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test00.User, test1.User, "expecting User to match")

	test2 := &TestTableGUUID{}
	result = db.Raw(`SELECT * FROM "test_user_uuid" WHERE "user" = ?`, test00.User).Scan(test2)
	require.NoError(t, result.Error, "expecting no error")
}

func TestGofrsUUIDType(t *testing.T) {
	ctx := currentContext()
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(ctx)
	_ = db.Migrator().DropTable(&TestTableGofrsUUID{})
	err := db.Migrator().AutoMigrate(TestTableGofrsUUID{})
	require.NoError(t, err, "expecting no error")

	u := gofrs.Must(gofrs.NewV4())
	test0 := &TestTableGofrsUUID{
		Name: "test0",
		User: u,
	}
	test00 := &TestTableGofrsUUID{
		Name: "test00",
		User: gofrs.Must(gofrs.NewV4()),
	}
	result := db.Create([]*TestTableGofrsUUID{test0, test00})
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(2), "expecting two records created")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	test0 = &TestTableGofrsUUID{}
	result = db.First(test0)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	require.EqualValuesf(t, u, test0.User, "expecting User to match")

	test1 := &TestTableGofrsUUID{}
	result = db.Model(test1).Where(`"user" = ?`, test00.User).Scan(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test00.User, test1.User, "expecting User to match")

	test2 := &TestTableGofrsUUID{}
	result = db.Raw(`SELECT * FROM "test_user_uuid" WHERE "user" = ?`, test00.User).Scan(test2)
	require.NoError(t, result.Error, "expecting no error")

	test3 := &TestTableGofrsUUID{
		Name: "test03",
		User: gofrs.Must(gofrs.NewV4()),
	}
	result = db.Create(test3)
	require.NoError(t, result.Error, "expecting no error")
	must := gofrs.Must(gofrs.NewV4())
	result = db.Exec(`UPDATE "test_user_uuid" SET "user" = ? WHERE "user" = ?`, must, test00.User)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(1), "expecting one row affected")
	test4 := &TestTableGofrsUUID{}
	result = db.Model(test4).Where(`"user" = ?`, must).Scan(test4)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, must, test4.User, "expecting User to match")
}

func TestULIDType(t *testing.T) {
	ctx := currentContext()
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(ctx)
	_ = db.Migrator().DropTable(&TestTableULID{})
	err := db.Migrator().AutoMigrate(TestTableULID{})
	require.NoError(t, err, "expecting no error")

	u := ulid.MustNewDefault(time.Now())
	test0 := &TestTableULID{
		Name: "test0",
		User: u,
	}
	test00 := &TestTableULID{
		Name: "test00",
		User: ulid.MustNewDefault(time.Now()),
	}
	result := db.Create([]*TestTableULID{test0, test00})
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(2), "expecting two records created")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	test0 = &TestTableULID{}
	result = db.First(test0)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	require.EqualValuesf(t, u, test0.User, "expecting User to match")

	test1 := &TestTableULID{}
	result = db.Model(test1).Where(`"user" = ?`, test00.User).Scan(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test00.User, test1.User, "expecting User to match")

	test2 := &TestTableULID{}
	result = db.Raw(`SELECT * FROM "test_user_ulid" WHERE "user" = ?`, test00.User).Scan(test2)
	require.NoError(t, result.Error, "expecting no error")
}

func TestTimeTypes(t *testing.T) {
	ctx := currentContext()
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(ctx)
	_ = db.Migrator().DropTable(&TestTableTime{})
	err := db.Migrator().AutoMigrate(TestTableTime{})
	require.NoError(t, err, "expecting no error")

	test0Name := "test0"
	test00Name := "test00"
	test0 := &TestTableTime{
		Name: &test0Name,
		Time: db.NowFunc(),
	}
	test00 := &TestTableTime{
		Name: &test00Name,
		Time: db.NowFunc(),
	}
	result := db.Create([]*TestTableTime{test0, test00})
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(2), "expecting two records created")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	test0Time := test0.Time
	result = db.First(test0)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	require.EqualValuesf(t, test0Time, test0.Time, "expecting Time to match")

	test1 := &TestTableTime{}
	result = db.Model(test1).Where(`"time" = ?`, test0Time).Scan(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0Time, test1.Time, "expecting Time to match")
}

func TestReturningIntoUUID(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	_ = db.Exec(`"DROP TABLE "test_user_uuid" cascade constraints"`)
	err := db.WithContext(currentContext()).Migrator().AutoMigrate(TestTableUUID{})
	require.NoError(t, err, "expecting no error")
	u := uuid.New()
	model := &TestTableUUID{
		Name: "Lisa",
		User: u,
	}
	result := db.WithContext(currentContext()).Create(model)
	require.NoError(t, result.Error, "expecting no error")
	result = db.WithContext(currentContext()).First(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, u, model.User, "expecting model User to be %s", u.String())
}

func TestDeleteReturningIntoUUID(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	_ = db.Migrator().DropTable(TestTableUUID{})
	db = db.WithContext(currentContext())
	err := db.Migrator().AutoMigrate(TestTableUUID{})
	require.NoError(t, err, "expecting no error")
	u := uuid.New()
	model := &TestTableUUID{
		Name: "Lisa",
		User: u,
	}
	result := db.Create(model)
	require.NoError(t, result.Error, "expecting no error")
	result = db.First(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, u, model.User, "expecting model User to be %s", u.String())
	id := model.ID
	model = &TestTableUUID{}
	result = db.Model(model).Where(`"id" = ?`, id).Delete(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, 1, result.RowsAffected, "expecting 1 row affected")
	result = db.Create(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, 1, result.RowsAffected, "expecting 1 row affected")
}

func TestReturningInto(t *testing.T) {
	db := dbNamingCase

	if db == nil {
		t.Log("db is nil!")
		return
	}

	_ = db.Migrator().DropTable(TestTableUser{})

	err := db.WithContext(currentContext()).Migrator().AutoMigrate(TestTableUser{})
	assert.NoError(t, err, "expecting no error automigrating")

	theBirthday := db.NowFunc()
	model := &TestTableUser{
		UID:         "U1",
		Name:        "Lisa",
		Account:     "lisa",
		Password:    "H6aLDNr",
		PhoneNumber: "+8616666666666",
		Sex:         "0",
		UserType:    1,
		Birthday:    &theBirthday,
		Enabled:     true,
	}
	res := db.WithContext(currentContext()).Create(model)
	assert.NoError(t, res.Error, "expecting no error creating model")
	modelId := model.ID
	res = db.WithContext(currentContext()).First(model)
	assert.NoError(t, res.Error, "expecting no error finding first")
	assert.EqualValuesf(t, modelId, model.ID, "expecting model ID to be %d", modelId)

	model.Name = "Alice"
	res = db.WithContext(currentContext()).Updates(model)
	assert.NoError(t, res.Error, "expecting no error updating Name")
	assert.EqualValuesf(t, "Alice", model.Name, "expecting Name to be 'Alice' was %s", model.Name)

	res = db.WithContext(currentContext()).Model(model).Updates(map[string]any{"name": "Zulu", "user_type": clause.Expr{SQL: "\"user_type\" + 1"}})
	assert.NoError(t, res.Error, "expecting no error updating Name")
	assert.EqualValuesf(t, "Zulu", model.Name, "expecting Name to be 'Zulu' was %s", model.Name)
	assert.EqualValuesf(t, 1, model.UserType, "expecting UserType to be unchanged at 1 was %d", model.UserType)
	res = db.WithContext(currentContext()).First(model)
	assert.NoError(t, res.Error, "expecting no error re-finding first")
	assert.EqualValuesf(t, "Zulu", model.Name, "expecting Name to be 'Zulu' was %s", model.Name)
	assert.EqualValuesf(t, 2, model.UserType, "expecting UserType to be refreshed to persisted value at 2 was %d", model.UserType)
	res = db.WithContext(currentContext()).Model(model).Clauses(clause.Returning{}).Updates(map[string]any{"name": "Zulu2", "user_type": clause.Expr{SQL: "\"user_type\" + 1"}})
	assert.NoError(t, res.Error, "expecting no error updating Name and user_type with Returning")
	assert.EqualValuesf(t, "Zulu2", model.Name, "expecting Name to be 'Zulu2' was %s", model.Name)
	assert.EqualValuesf(t, 3, model.UserType, "expecting UserType to be updated in-place at 3 was %d", model.UserType)

	model.Name = "Bob"
	model.Account = "bob"
	res = db.WithContext(currentContext()).Clauses(clause.Returning{}).Updates(model)
	assert.NoError(t, res.Error, "expecting no error updating Name with Returning")
	assert.EqualValuesf(t, "Bob", model.Name, "expecting Name to be 'Bob' was %s", model.Name)

	res = db.WithContext(currentContext()).Clauses(clause.Returning{}).Model(model).Updates(map[string]any{"name": "Charlie", "account": "charlie"})
	assert.NoError(t, res.Error, "expecting no error updating with map with Returning")
	assert.EqualValuesf(t, "Charlie", model.Name, "expecting Name to be 'Charlie' was %s", model.Name)
	assert.EqualValuesf(t, "charlie", model.Account, "expecting Account to be 'charlie' was %s", model.Account)

	res = db.WithContext(currentContext()).Clauses(clause.Returning{}).Model(model).Where("\"name\" = ?", "Delta").Updates(map[string]any{"name": "Charlie", "account": "charlie"})
	assert.NoError(t, res.Error, "expecting no error updating with map with Returning")
	assert.EqualValuesf(t, "Charlie", model.Name, "expecting Name to be 'Charlie' was %s", model.Name)
	assert.EqualValuesf(t, "charlie", model.Account, "expecting Account to be 'charlie' was %s", model.Account)
}

func TestReturning(t *testing.T) {
	//db := openTestConnection(nil, t, true, true, false)
	db := dbNamingCase

	if db == nil {
		t.Log("db is nil!")
		return
	}

	_ = db.Exec("DROP TABLE test_user cascade constraints")

	err := db.Migrator().AutoMigrate(TestTableUser{})
	assert.NoError(t, err, "expecting no error")
	theBirthday := db.NowFunc()
	model := TestTableUser{
		UID:         "U1",
		Name:        "Lisa",
		Account:     "lisa",
		Password:    "H6aLDNr",
		PhoneNumber: "+8616666666666",
		Sex:         "0",
		UserType:    1,
		Birthday:    &theBirthday,
		Enabled:     true,
	}
	res := db.WithContext(currentContext()).Create(&model)
	assert.NoError(t, res.Error, "expecting no error")
	modelId := model.ID
	res = db.WithContext(currentContext()).First(&model)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	model.Name = "Bob"
	model.Sex = "m"
	model.Enabled = true
	model.PEnabled = ptr(true)
	res = db.WithContext(currentContext()).Updates(model)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, "Bob", model.Name, "expecting 'Bob'")
	assert.Equal(t, "m", model.Sex, "expecting 'm'")
	assert.Equal(t, true, model.Enabled, "expecting 'true'")
	assert.Equal(t, ptr(true), model.PEnabled, "expecting '*true'")

	res = db.WithContext(currentContext()).Model(&model).Update("name", "Alice").Update("sex", "f").Update("enabled", false)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "Alice", model.Name)
	assert.Equal(t, "f", model.Sex, "expecting 'f'")

	res = db.WithContext(currentContext()).Model(&model).Updates(map[string]any{
		"name": "Bob",
		"sex":  "b",
	})
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, "Bob", model.Name)
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "b", model.Sex, "expecting 'b'")

	model.PEnabled = ptr(false)
	res = db.WithContext(currentContext()).Model(&model).Clauses(clause.Returning{}).Updates(map[string]any{
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
	res = db.WithContext(currentContext()).Clauses(clause.Returning{}).Updates(tm)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "doug", tm.Name, "expecting 'doug'")
	assert.Equal(t, "m", tm.Sex, "expecting 'm'")
	assert.Equal(t, true, tm.Enabled, "expecting 'true'")
	assert.Equal(t, 2, tm.UserType, "expecting '2'")
	assert.Equal(t, theBirthday.Format("2006-01-02 15:04:05"), tm.Birthday.Format("2006-01-02 15:04:05"), "expecting '1978-05-01 00:00:00'")
	assert.Equal(t, true, *tm.PEnabled, "expecting '(*bool)true'")

	ttm := &TestTableUser{
		ID:   modelId,
		Name: "evan",
	}
	res = db.WithContext(currentContext()).Updates(ttm)
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
	dsn, _ := findDbContextInfo(currentContext())

	db, err := gorm.Open(New(Config{
		DSN:                     dsn,
		IgnoreCase:              true,
		NamingCaseSensitive:     true,
		VarcharSizeIsCharLength: true,
	}), getTestGormConfig(nil))
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

func Test_reflectDereference(t *testing.T) {
	type args struct {
		obj any
	}
	x := 5
	var px *int = &x
	var nilPtr *int
	var nilIface any = nil
	var ifaceWithPtr any = px
	var ifaceWithNilPtr any = nilPtr
	var nestedPtr **int = &px

	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "primitive int",
			args: args{obj: 5},
			want: 5,
		},
		{
			name: "pointer to int",
			args: args{obj: px},
			want: 5,
		},
		{
			name: "nil pointer",
			args: args{obj: nilPtr},
			want: nil,
		},
		{
			name: "nil interface",
			args: args{obj: nilIface},
			want: nil,
		},
		{
			name: "interface with pointer to int",
			args: args{obj: ifaceWithPtr},
			want: 5,
		},
		{
			name: "interface with nil pointer",
			args: args{obj: ifaceWithNilPtr},
			want: nil,
		},
		{
			name: "nested pointer",
			args: args{obj: nestedPtr},
			want: 5,
		},
		{
			name: "interface wrapping primitive",
			args: args{obj: any(5)},
			want: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, reflectDereference(tt.args.obj), "reflectDereference(%v)", tt.args.obj)
		})
	}
}

func Test_reflectReference(t *testing.T) {
	x := 5
	px := &x
	var nilPtr *int
	var iface any = x
	var ifacePtr any = px
	var ifaceNilPtr any = nilPtr

	type args struct {
		obj          any
		wrapPointers bool
	}
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "wrap primitive int",
			args: args{obj: x},
			want: func() any {
				v := x
				return &v
			}(),
		},
		{
			name: "leave pointer as-is",
			args: args{obj: px},
			want: px,
		},
		{
			name: "wrap pointer again if wrapPointers=true",
			args: args{obj: px, wrapPointers: true},
			want: func() any {
				return &px
			}(),
		},
		{
			name: "interface containing int",
			args: args{obj: iface},
			want: func() any {
				v := 5
				return &v
			}(),
		},
		{
			name: "interface containing pointer",
			args: args{obj: ifacePtr},
			want: px,
		},
		{
			name: "interface containing pointer (original any value)",
			args: args{obj: ifacePtr},
			want: ifacePtr,
		},
		{
			name: "interface containing pointer with wrapPointers=true",
			args: args{obj: ifacePtr, wrapPointers: true},
			want: func() any {
				return &px
			}(),
		},
		{
			name: "nil value",
			args: args{obj: nil},
			want: nil,
		},
		{
			name: "interface(nil pointer)",
			args: args{obj: ifaceNilPtr},
			want: nilPtr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got any
			if tt.args.wrapPointers {
				got = reflectReference(tt.args.obj, true)
			} else {
				got = reflectReference(tt.args.obj)
			}
			assert.Equalf(t, tt.want, got, "reflectReference(%v, %v)", tt.args.obj, tt.args.wrapPointers)
		})
	}
}
