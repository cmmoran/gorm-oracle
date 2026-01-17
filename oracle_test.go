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

	"github.com/cmmoran/go-ora/v2/converters"
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

	var (
		ctx          context.Context
		oraContainer tc.Container
	)
	if _, ok := os.LookupEnv("GORM_NO_DB"); !ok {
		startOracleDatabase(t)
		ctx = currentContext()
		dbNamingCase = setupOracleDatabase(t, ctx, false, true, true)
		dbIgnoreCase = setupOracleDatabase(t, ctx, true, false, true)
		_, oraContainer = findDbContextInfo(ctx)
	}

	// Run tests
	exitCode := m.Run()

	if oraContainer != nil {
		_ = oraContainer.Terminate(ctx)
	}
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
	} else if service == "" {
		service = "FREEPDB1"
	}
	var err error
	if _, ok := os.LookupEnv("GORM_ORA_SKIP_CONTAINER"); !ok {
		req := tc.ContainerRequest{
			Image:        "gvenzl/oracle-free:slim",
			ExposedPorts: []string{"1521/tcp"},
			Env:          env,
			WaitingFor: wait.ForAll(
				wait.ForLog("DATABASE IS READY TO USE!").WithStartupTimeout(8*time.Minute),
				wait.ForListeningPort("1521/tcp").WithStartupTimeout(8*time.Minute),
			),
			//WaitingFor:   wait.ForLog("Completed: ALTER DATABASE OPEN").WithStartupTimeout(2 * time.Minute),
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
		if envHost, envHostFound := os.LookupEnv("GORM_ORA_HOST"); envHostFound && host == "localhost" && envHost != "localhost" {
			host = envHost
			//} else if host == "localhost" {
			//	host = "127.0.0.1"
		}

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

	timeGranularity := time.Nanosecond
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
	if traceEnabled, ok := os.LookupEnv("GORM_ORA_TRACE"); ok {
		if len(dsn) > 0 && strings.Contains(dsn, "?") {
			dsn = fmt.Sprintf("%s&TRACE%%20FILE=%s", dsn, traceEnabled)
		} else if len(dsn) > 0 {
			dsn = fmt.Sprintf("%s?TRACE%%20FILE=%s", dsn, traceEnabled)
		}
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
		NamingStrategy: &NamingStrategy{},
		Logger:         l,
		NowFunc: func() time.Time {
			tt := time.Now()
			if timeGranularity < 0 {
				tt = tt.Round(-timeGranularity)
			} else if timeGranularity > 0 {
				tt = tt.Truncate(timeGranularity)
			}
			if sessionTimezone != time.Local {
				tt = tt.In(sessionTimezone)
			}
			return tt
		},
	})
	require.NoErrorf(t, err, "Failed to open database %+v", err)

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
		NamingStrategy:                           schema.NamingStrategy{},
	}
}

type TestTableTime struct {
	ID           uint64    `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name         *string   `gorm:"size:50;comment:User Name" json:"name"`
	Date         time.Time `gorm:"type:date;comment:Date" json:"date"`
	Timestamp    time.Time `gorm:"type:timestamp;comment:Timestamp" json:"timestamp"`
	TimestampTZ  time.Time `gorm:"type:timestamp with time zone;comment:TSTZ" json:"timestamp_tz"`
	TimestampLTZ time.Time `gorm:"type:timestamp with local time zone;comment:TSLTZ" json:"timestamp_ltz"`
}

func (TestTableTime) TableName() string {
	return "test_user_time"
}

type TestTableTimePtrs struct {
	ID           uint64     `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name         *string    `gorm:"size:50;comment:User Name" json:"name"`
	Date         *time.Time `gorm:"type:date;comment:Date" json:"date"`
	Timestamp    *time.Time `gorm:"type:timestamp;comment:Timestamp" json:"timestamp"`
	TimestampTZ  *time.Time `gorm:"type:timestamp with time zone;comment:TSTZ" json:"timestamp_tz"`
	TimestampLTZ *time.Time `gorm:"type:timestamp with local time zone;comment:TSLTZ" json:"timestamp_ltz"`
}

func (TestTableTimePtrs) TableName() string {
	return "test_user_time"
}

type TestTableUUID struct {
	ID   uint64     `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string     `gorm:"size:50;comment:User Name" json:"name"`
	User uuid.UUID  `gorm:"type:uuid;comment:User UUID" json:"user"`
	Ref  *uuid.UUID `gorm:"comment:Reference UUID" json:"ref,omitempty"`
}

func (TestTableUUID) TableName() string {
	return "test_user_uuid"
}

type TestTableULID struct {
	ID   uint64    `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string    `gorm:"size:50;comment:User Name" json:"name"`
	User ulid.ULID `gorm:"type:ulid;comment:User ULID" json:"user"`
}

func (TestTableULID) TableName() string {
	return "test_user_ulid"
}

type TestTableGUUID struct {
	ID   uint64     `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string     `gorm:"size:50;comment:User Name" json:"name"`
	User uuid.UUID  `gorm:"comment:User UUID" json:"user"`
	Ref  *uuid.UUID `gorm:"comment:Reference UUID" json:"ref,omitempty"`
}

func (TestTableGUUID) TableName() string {
	return "test_user_uuid"
}

type TestTableGofrsUUID struct {
	ID   uint64     `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string     `gorm:"size:50;comment:User Name" json:"name"`
	User gofrs.UUID `gorm:"comment:User UUID" json:"user"`
}

func (TestTableGofrsUUID) TableName() string {
	return "test_user_uuid"
}

type TestTableEmbeddedProfile struct {
	Nickname string `gorm:"size:50;comment:Nickname" json:"nickname"`
}

type TestTableEmbedded struct {
	ID   uint64 `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name string `gorm:"size:50;comment:User Name" json:"name"`
	TestTableEmbeddedProfile
}

func (TestTableEmbedded) TableName() string {
	return "test_user_embedded"
}

type TestTableNullable struct {
	ID     uint64         `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	Name   sql.NullString `gorm:"size:50;comment:Nullable Name" json:"name"`
	Note   *string        `gorm:"size:50;comment:Nullable Note" json:"note"`
	Count  sql.NullInt64  `gorm:"comment:Nullable Count" json:"count"`
	Active *bool          `gorm:"comment:Nullable Active" json:"active"`
}

func (TestTableNullable) TableName() string {
	return "test_user_nullable"
}

type TestTableDefaultValues struct {
	ID        uint64 `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey" json:"id"`
	Name      string `gorm:"size:50" json:"name"`
	Count     int    `gorm:"default:7" json:"count"`
	UpdatedAt time.Time
}

func (TestTableDefaultValues) TableName() string {
	return "test_user_defaults"
}

// ==== Query and clause behavior ====

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

func TestChunkIn(t *testing.T) {
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

	maxIds := make([]uuid.UUID, 2050)
	for i := 0; i < len(maxIds); i++ {
		maxIds[i] = uuid.New()
	}
	test0 := &TestTableGUUID{
		Name: "test0",
		User: maxIds[0],
	}
	result := db.Create(test0)
	require.NoError(t, result.Error, "expecting no error creating test0")
	var finds []TestTableGUUID
	result = db.Model(&TestTableGUUID{}).Where(`"USER" IN ?`, maxIds).Find(&finds)
	// Where "USER" IN Expression
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, int64(1), result.RowsAffected, "expecting one record found")
	require.EqualValuesf(t, maxIds[0], finds[0].User, "expecting ID to match")

	finds = make([]TestTableGUUID, 0)
	result = db.Model(&TestTableGUUID{}).Where(clause.IN{
		Column: clause.Column{
			Name: `USER`,
		},
		Values: []any{maxIds},
	}).Find(&finds)
	// Where "USER" IN clause
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, int64(1), result.RowsAffected, "expecting one record found")
	require.EqualValuesf(t, maxIds[0], finds[0].User, "expecting ID to match")

	nmaxIds := maxIds[1:]
	finds = make([]TestTableGUUID, 0)
	result = db.Model(&TestTableGUUID{}).Where(`"USER" NOT IN ?`, nmaxIds).Find(&finds)
	// Where "USER" NOT IN expression
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, int64(1), result.RowsAffected, "expecting one record found")
	require.EqualValuesf(t, maxIds[0], finds[0].User, "expecting ID to match")

	finds = make([]TestTableGUUID, 0)
	result = db.Model(&TestTableGUUID{}).Not(`"USER" IN ?`, nmaxIds).Find(&finds)
	// Not "USER" NOT IN expression
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, int64(1), result.RowsAffected, "expecting one record found")
	require.EqualValuesf(t, maxIds[0], finds[0].User, "expecting ID to match")

	finds = make([]TestTableGUUID, 0)
	result = db.Model(&TestTableGUUID{}).Not(clause.IN{
		Column: clause.Column{
			Name: `USER`,
		},
		Values: []any{nmaxIds},
	}).Find(&finds)
	// Not "USER" IN clause
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, int64(1), result.RowsAffected, "expecting one record found")
	require.EqualValuesf(t, maxIds[0], finds[0].User, "expecting ID to match")
}

// ==== UUID/ULID types ====

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
	require.EqualValuesf(t, int64(2), result.RowsAffected, "expecting two records created")
	require.EqualValuesf(t, int64(1), test0.ID, "expecting ID to be 1")
	test1 := &TestTableGUUID{
		ID: test0.ID,
	}
	result = db.Find(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, int64(1), test1.ID, "expecting ID to be 1")
	require.EqualValuesf(t, u, test1.User, "expecting User to match")

	test2 := &TestTableGUUID{}
	result = db.Model(test2).Where(&TestTableGUUID{User: test00.User}).First(test2)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test00.User, test2.User, "expecting User to match")

	test3 := &TestTableGUUID{}
	result = db.Model(test3).Where(`"USER"`, test00.User).First(test3)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test00.User, test3.User, "expecting User to match")

	test4 := &TestTableGUUID{}
	result = db.Raw(`SELECT * FROM test_user_uuid WHERE "USER" = ?`, test00.User).Scan(test4)
	require.NoError(t, result.Error, "expecting no error")
	require.Equal(t, result.RowsAffected, int64(1))
	require.EqualValuesf(t, test00, test4, "expecting User to match")
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
	result = db.Model(test1).Where(`"USER"`, test00.User).Scan(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test00.User, test1.User, "expecting User to match")

	test2 := &TestTableGofrsUUID{}
	result = db.Raw(`SELECT * FROM test_user_uuid WHERE "USER" = ?`, test00.User).Scan(test2)
	require.NoError(t, result.Error, "expecting no error")

	test3 := &TestTableGofrsUUID{
		Name: "test03",
		User: gofrs.Must(gofrs.NewV4()),
	}
	result = db.Create(test3)
	require.NoError(t, result.Error, "expecting no error")
	must := gofrs.Must(gofrs.NewV4())
	result = db.Exec(`UPDATE test_user_uuid SET "USER" = ? WHERE "USER" = ?`, must, test00.User)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(1), "expecting one row affected")
	test4 := &TestTableGofrsUUID{}
	result = db.Model(test4).Where(`"USER" = ?`, must).Scan(test4)
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
	result = db.Model(test1).Where(`"USER"`, test00.User).Scan(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test00.User, test1.User, "expecting User to match")

	test2 := &TestTableULID{}
	result = db.Raw(`SELECT * FROM test_user_ulid WHERE "USER" = ?`, test00.User).Scan(test2)
	require.NoError(t, result.Error, "expecting no error")
}

// ==== Time types ====

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

	nowTime := db.NowFunc().UTC()
	nowPrimeTime := db.NowFunc().UTC()
	loc := db.Dialector.(*Dialector).sessionLocation
	if loc == nil {
		loc = time.Local
	}
	test0Name := "test0"
	test00Name := "test00"
	test0 := &TestTableTime{
		ID:           0,
		Name:         &test0Name,
		Date:         nowTime,
		Timestamp:    nowTime,
		TimestampTZ:  nowTime,
		TimestampLTZ: nowTime,
	}
	test00 := &TestTableTime{
		Name:         &test00Name,
		Date:         nowPrimeTime,
		Timestamp:    nowPrimeTime,
		TimestampTZ:  nowPrimeTime,
		TimestampLTZ: nowPrimeTime,
	}
	result := db.Create([]*TestTableTime{test0, test00})
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(2), "expecting two records created")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	test0Date := test0.Date
	test0Timestamp := converters.ToTimestamp(test0.Timestamp)
	test0TimestampTZ := test0.TimestampTZ
	test0TimestampLTZ := test0.TimestampLTZ
	id := test0.ID
	test0 = &TestTableTime{
		ID: id,
	}
	result = db.First(test0)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	require.EqualValuesf(t, test0Date, test0.Date, "expecting Date to match")
	require.EqualValuesf(t, test0Timestamp, test0.Timestamp, "expecting Timestamp to match")
	require.EqualValuesf(t, test0TimestampTZ, test0.TimestampTZ, "expecting TimestampTZ to match")
	require.EqualValuesf(t, test0TimestampLTZ, test0.TimestampLTZ, "expecting TimestampLTZ to match")

	test1 := &TestTableTime{}
	result = db.Model(test1).Where(`date`, test0Date).First(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0Date, test1.Date, "expecting Time to match")

	test1 = &TestTableTime{}
	result = db.Model(test1).Where(`timestamp`, test0Timestamp).First(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0Timestamp, test1.Timestamp, "expecting Date to match")

	test1 = &TestTableTime{}
	result = db.Model(test1).Where(`timestamp_tz`, test0TimestampTZ).First(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0TimestampTZ, test1.TimestampTZ, "expecting Date to match")

	test1 = &TestTableTime{}
	result = db.Model(test1).Where(`timestamp_ltz`, test0TimestampLTZ).First(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0TimestampLTZ, test1.TimestampLTZ, "expecting Date to match")
}

func TestTimePtrTypes(t *testing.T) {
	ctx := currentContext()
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(ctx)
	_ = db.Migrator().DropTable(&TestTableTimePtrs{})
	err := db.Migrator().AutoMigrate(TestTableTimePtrs{})
	require.NoError(t, err, "expecting no error")

	nowTime := ptr(db.NowFunc())
	nowPrimeTime := ptr(db.NowFunc())
	loc := db.Dialector.(*Dialector).sessionLocation
	if loc == nil {
		loc = time.Local
	}
	test0Name := "test0"
	test00Name := "test00"
	test0 := &TestTableTimePtrs{
		ID:           0,
		Name:         &test0Name,
		Date:         nowTime,
		Timestamp:    nowTime,
		TimestampTZ:  nowTime,
		TimestampLTZ: nowTime,
	}
	test00 := &TestTableTimePtrs{
		Name:         &test00Name,
		Date:         nowPrimeTime,
		Timestamp:    nowPrimeTime,
		TimestampTZ:  nowPrimeTime,
		TimestampLTZ: nowPrimeTime,
	}
	result := db.Create([]*TestTableTimePtrs{test0, test00})
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, result.RowsAffected, int64(2), "expecting two records created")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	test0Date := test0.Date
	test0Timestamp := test0.Timestamp
	test0TimestampTZ := test0.TimestampTZ
	test0TimestampLTZ := test0.TimestampLTZ
	id := test0.ID
	test0 = &TestTableTimePtrs{
		ID: id,
	}
	result = db.First(test0)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, test0.ID, int64(1), "expecting ID to be 1")
	require.EqualValuesf(t, test0Date, test0.Date, "expecting Date to match")
	require.EqualValuesf(t, test0Timestamp, test0.Timestamp, "expecting Timestamp to match")
	require.EqualValuesf(t, test0TimestampTZ, test0.TimestampTZ, "expecting TimestampTZ to match")
	require.EqualValuesf(t, test0TimestampLTZ, test0.TimestampLTZ, "expecting TimestampLTZ to match")

	test1 := &TestTableTimePtrs{}
	result = db.Model(test1).Where(`date`, test0.Date).First(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0Date, test1.Date, "expecting Time to match")

	test1 = &TestTableTimePtrs{}
	result = db.Model(test1).Where(`timestamp`, test0Timestamp).Find(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0Timestamp, test1.Timestamp, "expecting Date to match")

	test1 = &TestTableTimePtrs{}
	result = db.Model(test1).Where(`timestamp_tz`, test0TimestampTZ).Find(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0TimestampTZ, test1.TimestampTZ, "expecting Date to match")

	test1 = &TestTableTimePtrs{}
	result = db.Model(test1).Where(`timestamp_ltz`, test0TimestampLTZ).Find(test1)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValues(t, 1, result.RowsAffected, "expecting 1 row affected")
	require.EqualValuesf(t, test0TimestampLTZ, test1.TimestampLTZ, "expecting Date to match")
}

// ==== RETURNING behavior ====

func TestReturningIntoUUID(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(currentContext())
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
	model = &TestTableUUID{
		ID: model.ID,
	}
	result = db.WithContext(currentContext()).Find(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, u, model.User, "expecting model User to be %s", u.String())
	nuuid := uuid.New()
	model.Ref = &nuuid
	result = db.WithContext(currentContext()).Updates(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, nuuid, *model.Ref, "expecting Ref to be %s", nuuid.String())
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
	result = db.Model(model).Where(`id = ?`, id).Delete(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, 1, result.RowsAffected, "expecting 1 row affected")
	result = db.Create(model)
	require.NoError(t, result.Error, "expecting no error")
	require.EqualValuesf(t, 1, result.RowsAffected, "expecting 1 row affected")
}

func TestDeleteReturningBehavior(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}

	t.Run("DefaultNoReturning", func(t *testing.T) {
		toSQL := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			model := &TestTableUser{}
			return tx.Model(model).Where(`id = ?`, 1).Delete(model)
		})
		assert.NotContains(t, strings.ToUpper(toSQL), " RETURNING ")
	})

	t.Run("ExplicitReturning", func(t *testing.T) {
		toSQL := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			model := &TestTableUser{}
			return tx.Model(model).Clauses(clause.Returning{}).Where(`id = ?`, 1).Delete(model)
		})
		assert.Contains(t, strings.ToUpper(toSQL), " RETURNING ")
	})
}

func TestUpdateReturningBehavior(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}

	t.Run("DefaultNoReturning", func(t *testing.T) {
		toSQL := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			return tx.Model(&TestTableUser{}).Where(`id = ?`, 1).Updates(map[string]any{"name": "alpha"})
		})
		assert.NotContains(t, strings.ToUpper(toSQL), " RETURNING ")
	})

	t.Run("NonAddressableDestStillReturnsWhenModelAddressable", func(t *testing.T) {
		toSQL := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			update := TestTableUser{Name: "beta"}
			return tx.Model(&TestTableUser{}).Clauses(clause.Returning{}).Where(`id = ?`, 1).Updates(update)
		})
		assert.Contains(t, strings.ToUpper(toSQL), " RETURNING ")
	})
}

func TestUpdateReturningNonAddressableStruct(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(currentContext())

	_ = db.Migrator().AutoMigrate(TestTableUser{})
	model := &TestTableUser{
		UID:         "U2",
		Name:        "Before",
		Account:     "before",
		Password:    "H6aLDNr",
		PhoneNumber: "+8611111111111",
		Sex:         "0",
		UserType:    1,
		Enabled:     true,
	}
	require.NoError(t, db.Create(model).Error, "expecting no error")

	update := TestTableUser{Name: "After"}
	res := db.Model(&TestTableUser{}).Clauses(clause.Returning{}).Where(`id = ?`, model.ID).Updates(update)
	require.NoError(t, res.Error, "expecting no error")

	var got TestTableUser
	require.NoError(t, db.First(&got, model.ID).Error, "expecting no error")
	assert.Equal(t, "After", got.Name, "expecting Name to be updated")
}

func TestReturningSkipsEmbeddedFields(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(currentContext())

	_ = db.Migrator().DropTable(&TestTableEmbedded{})
	require.NoError(t, db.Migrator().AutoMigrate(TestTableEmbedded{}), "expecting no error")

	model := &TestTableEmbedded{
		Name: "Alpha",
		TestTableEmbeddedProfile: TestTableEmbeddedProfile{
			Nickname: "A1",
		},
	}
	require.NoError(t, db.Create(model).Error, "expecting no error")

	toSQL := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		m := &TestTableEmbedded{
			ID:   1,
			Name: "Beta",
			TestTableEmbeddedProfile: TestTableEmbeddedProfile{
				Nickname: "B1",
			},
		}
		return tx.Model(m).Clauses(clause.Returning{}).Updates(m)
	})
	upperSQL := strings.ToUpper(toSQL)
	assert.Contains(t, upperSQL, " RETURNING ")

	model.Name = "Beta"
	model.Nickname = "B1"
	res := db.Model(model).Clauses(clause.Returning{}).Updates(model)
	require.NoError(t, res.Error, "expecting no error")

	var got TestTableEmbedded
	require.NoError(t, db.First(&got, model.ID).Error, "expecting no error")
	assert.Equal(t, "Beta", got.Name, "expecting Name to be updated")
	assert.Equal(t, "B1", got.Nickname, "expecting Nickname to be updated")
}

func TestReturningNullableFields(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(currentContext())

	_ = db.Migrator().DropTable(&TestTableNullable{})
	require.NoError(t, db.Migrator().AutoMigrate(TestTableNullable{}), "expecting no error")

	model := &TestTableNullable{
		Name:   sql.NullString{String: "Alpha", Valid: true},
		Note:   ptr("note"),
		Count:  sql.NullInt64{Int64: 5, Valid: true},
		Active: ptr(true),
	}
	require.NoError(t, db.Create(model).Error, "expecting no error")

	res := db.Model(model).Clauses(clause.Returning{}).Updates(map[string]any{
		"name":   sql.NullString{},
		"note":   nil,
		"count":  sql.NullInt64{},
		"active": nil,
	})
	require.NoError(t, res.Error, "expecting no error")

	var got TestTableNullable
	require.NoError(t, db.First(&got, model.ID).Error, "expecting no error")
	assert.False(t, got.Name.Valid, "expecting Name to be NULL")
	assert.Nil(t, got.Note, "expecting Note to be NULL")
	assert.False(t, got.Count.Valid, "expecting Count to be NULL")
	assert.Nil(t, got.Active, "expecting Active to be NULL")
}

func TestCreateReturningDefaultValues(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(currentContext())

	_ = db.Migrator().DropTable(&TestTableDefaultValues{})
	require.NoError(t, db.Migrator().AutoMigrate(TestTableDefaultValues{}), "expecting no error")

	model := &TestTableDefaultValues{Name: "Alpha"}
	require.NoError(t, db.Create(model).Error, "expecting no error")
	assert.Equal(t, 7, model.Count, "expecting default Count to be returned")
}

func TestUpdateMapVsStructWithExprAndZeroValues(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}
	db = db.WithContext(currentContext())

	_ = db.Migrator().DropTable(&TestTableDefaultValues{})
	require.NoError(t, db.Migrator().AutoMigrate(TestTableDefaultValues{}), "expecting no error")

	model := &TestTableDefaultValues{Name: "Alpha"}
	require.NoError(t, db.Create(model).Error, "expecting no error")

	res := db.Model(model).Clauses(clause.Returning{}).Updates(map[string]any{
		"count": gorm.Expr("count + 1"),
	})
	require.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, 8, model.Count, "expecting Count to be incremented")

	model.Count = 0
	res = db.Model(model).Clauses(clause.Returning{}).Select("count").Updates(model)
	require.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, 0, model.Count, "expecting Count to be set to zero")
}

func TestMergeCreateNoReturning(t *testing.T) {
	db := dbNamingCase
	if db == nil {
		t.Log("db is nil!")
		return
	}

	toSQL := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		m := &TestTableUser{ID: 1, Name: "Alpha"}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{"name"}),
		}).Create(m)
	})
	upperSQL := strings.ToUpper(toSQL)
	assert.Contains(t, upperSQL, "MERGE INTO")
	assert.NotContains(t, upperSQL, " RETURNING ")
}

func TestPartialIndex(t *testing.T) {
	db := dbNamingCase

	if db == nil {
		t.Log("db is nil!")
		return
	}

	_ = db.Migrator().DropTable(TestTablePartialIndex{})
	err := db.WithContext(currentContext()).Migrator().AutoMigrate(TestTablePartialIndex{})

	require.NoError(t, err, "expecting no error")
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

	// Updates will update deterministic fields but not expressions
	// unless Clauses(clause.Returning{}) is used.
	res = db.WithContext(currentContext()).Model(model).Updates(map[string]any{"NAME": "Zulu", "USER_TYPE": clause.Expr{SQL: "USER_TYPE + 1"}})
	assert.NoError(t, res.Error, "expecting no error updating name and user_type")
	assert.EqualValuesf(t, "Zulu", model.Name, "expecting Name to be 'Alice' was %s", model.Name)
	assert.EqualValuesf(t, 1, model.UserType, "expecting UserType to be unchanged at 1 was %d", model.UserType)
	// Note that the UserType was, in fact, updated in-place to 2
	res = db.WithContext(currentContext()).First(model)
	assert.NoError(t, res.Error, "expecting no error re-finding first")
	assert.EqualValuesf(t, "Zulu", model.Name, "expecting Name to be 'Zulu' was %s", model.Name)
	assert.EqualValuesf(t, 2, model.UserType, "expecting UserType to be refreshed to persisted value at 2 was %d", model.UserType)
	res = db.WithContext(currentContext()).Model(model).Clauses(clause.Returning{}).Updates(map[string]any{"name": "Zulu2", "user_type": clause.Expr{SQL: "user_type + 1"}})
	assert.NoError(t, res.Error, "expecting no error updating Name and user_type with Returning")
	assert.EqualValuesf(t, "Zulu2", model.Name, "expecting Name to be 'Zulu2' was %s", model.Name)
	assert.EqualValuesf(t, 3, model.UserType, "expecting UserType to be updated in-place at 3 was %d", model.UserType)

	model.Name = "Bob"
	model.Account = "bob"
	res = db.WithContext(currentContext()).Clauses(clause.Returning{}).Updates(model)
	assert.NoError(t, res.Error, "expecting no error updating Name with Returning")
	assert.EqualValuesf(t, "Bob", model.Name, "expecting Name to be 'Bob' was %s", model.Name)

	res = db.WithContext(currentContext()).Model(model).Clauses(clause.Returning{}).Updates(map[string]any{"name": "Charlie", "account": "charlie"})
	assert.NoError(t, res.Error, "expecting no error updating with map with Returning")
	assert.EqualValuesf(t, "Charlie", model.Name, "expecting Name to be 'Charlie' was %s", model.Name)
	assert.EqualValuesf(t, "charlie", model.Account, "expecting Account to be 'charlie' was %s", model.Account)

	res = db.WithContext(currentContext()).Model(model).Clauses(clause.Returning{}).Where(`name = ?`, "Delta").Updates(map[string]any{"name": "Charlie", "account": "charlie"})
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
	model.Penabled = ptr(true)
	res = db.WithContext(currentContext()).Updates(model)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, "Bob", model.Name, "expecting 'Bob'")
	assert.Equal(t, "m", model.Sex, "expecting 'm'")
	assert.Equal(t, true, model.Enabled, "expecting 'true'")
	assert.Equal(t, ptr(true), model.Penabled, "expecting '*true'")

	m := map[string]any{
		"name":    "Alice",
		"sex":     "f",
		"enabled": false,
	}
	res = db.WithContext(currentContext()).Model(&model).Clauses(clause.Returning{}).Updates(m)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "Alice", model.Name)
	assert.Equal(t, "f", model.Sex, "expecting 'f'")

	res = db.WithContext(currentContext()).Model(&model).Clauses(clause.Returning{}).Updates(map[string]any{
		"name": "Bob",
		"sex":  "b",
	})
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, "Bob", model.Name)
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "b", model.Sex, "expecting 'b'")

	model.Penabled = ptr(false)
	res = db.WithContext(currentContext()).Model(&model).Clauses(clause.Returning{}).Updates(map[string]any{
		"name":      "charlie",
		"sex":       "m",
		"enabled":   true,
		"penabled":  ptr(true),
		"user_type": gorm.Expr(`user_type + 1`),
	})
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, modelId, model.ID)
	assert.Equal(t, "charlie", model.Name, "expecting 'charlie'")
	assert.Equal(t, "m", model.Sex, "expecting 'm'")
	assert.Equal(t, true, model.Enabled, "expecting 'true'")
	assert.Equal(t, ptr(true), model.Penabled, "expecting '*true'")

	tm := &TestTableUser{
		ID:   modelId,
		Name: "doug",
	}
	res = db.WithContext(currentContext()).Model(tm).Clauses(clause.Returning{}).Updates(tm)
	assert.NoError(t, res.Error, "expecting no error")
	assert.Equal(t, model.ID, tm.ID)
	assert.Equal(t, "doug", tm.Name, "expecting 'doug'")
	assert.Equal(t, "m", tm.Sex, "expecting 'm'")
	assert.Equal(t, true, tm.Enabled, "expecting 'true'")
	assert.Equal(t, 2, tm.UserType, "expecting '2'")
	assert.Equal(t, theBirthday.Format("2006-01-02 15:04:05"), tm.Birthday.Format("2006-01-02 15:04:05"), "expecting '1978-05-01 00:00:00'")
	assert.Equal(t, true, *tm.Penabled, "expecting '(*bool)true'")

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
	assert.Nil(t, ttm.Penabled, "expecting '(*bool)nil'")
}

func ptr[T any](v T) *T {
	return &v
}

// ==== SQL generation and session behavior ====

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
		{name: "Offset10Limit10Order", args: args{offset: 10, limit: 10, order: `id`}},
		{name: "Offset10Limit10OrderDESC", args: args{offset: 10, limit: 10, order: `id DESC`}},
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

// ==== Reflection utilities ====

func Test_reflectDereference(t *testing.T) {
	type args struct {
		obj any
	}
	x := 5
	var px = &x
	var nilPtr *int
	var nilIface any = nil
	var ifaceWithPtr any = px
	var ifaceWithNilPtr any = nilPtr
	var nestedPtr = &px

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
			v, _ := reflectDereference(tt.args.obj)
			assert.Equalf(t, tt.want, v, "reflectDereference(%v)", tt.args.obj)
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
