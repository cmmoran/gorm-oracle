package oracle

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/clause"
)

func TestMergeCreate(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	model := TestTableUser{}
	migrator := db.Set("gorm:table_comments", "User information table").Migrator()
	if migrator.HasTable(model) {
		if err = migrator.DropTable(model); err != nil {
			t.Fatalf("DropTable() error = %v", err)
		}
	}
	if err = migrator.AutoMigrate(model); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	} else {
		t.Log("AutoMigrate() success!")
	}

	data := []TestTableUser{
		{
			UID:         "U1",
			Name:        "Lisa",
			Account:     "lisa",
			Password:    "H6aLDNr",
			PhoneNumber: "+8616666666666",
			Sex:         "0",
			UserType:    1,
			Enabled:     true,
		},
		{
			UID:         "U1",
			Name:        "Lisa",
			Account:     "lisa",
			Password:    "H6aLDNr",
			PhoneNumber: "+8616666666666",
			Sex:         "0",
			UserType:    1,
			Enabled:     true,
		},
		{
			UID:         "U2",
			Name:        "Daniela",
			Account:     "daniela",
			Password:    "Si7l1sRIC79",
			PhoneNumber: "+8619999999999",
			Sex:         "1",
			UserType:    1,
			Enabled:     true,
		},
	}
	t.Run("MergeCreate", func(t *testing.T) {
		tx := db.Create(&data)
		if err = tx.Error; err != nil {
			t.Fatal(err)
		}
		dataJsonBytes, _ := json.MarshalIndent(data, "", "  ")
		t.Logf("result: %s", dataJsonBytes)
	})
}

func TestCreateCaseSensitive(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}
	_ = db.Migrator().DropTable(TestTableCaseSensitive{})
	_ = db.Migrator().DropTable(TestTableCaseSensitiveRegular{})
	err = db.WithContext(currentContext()).Migrator().AutoMigrate(TestTableCaseSensitive{})

	require.NoError(t, err, "expecting no error")

	_ = db.Migrator().DropTable(TestTableCaseSensitive{})
	_ = db.Migrator().DropTable(TestTableCaseSensitiveRegular{})
	err = db.WithContext(currentContext()).Migrator().AutoMigrate(TestTableCaseSensitiveRegular{})

	require.NoError(t, err, "expecting no error")
}

type TestTableUserUnique struct {
	ID          uint64     `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto-increment ID" json:"id"`
	UID         string     `gorm:"type:varchar2;size:50;comment:User identity;unique" json:"uid"`
	Name        string     `gorm:"size:50;comment:User name" json:"name"`
	Account     string     `gorm:"type:varchar2;size:50;comment:Login account" json:"account"`
	Password    string     `gorm:"type:varchar2;size:512;comment:Login password (encrypted)" json:"password"`
	Email       string     `gorm:"type:varchar2;size:128;comment:Email address" json:"email"`
	PhoneNumber string     `gorm:"type:varchar2;size:15;comment:E.164" json:"phoneNumber"`
	Sex         string     `gorm:"type:char;size:1;comment:Gender" json:"sex"`
	Birthday    *time.Time `gorm:"<-:create;comment:Birthday" json:"birthday,omitempty"`
	UserType    int        `gorm:"size:8;comment:User type" json:"userType"`
	Enabled     bool       `gorm:"comment:Is enabled" json:"enabled"`
	Remark      string     `gorm:"size:1024;comment:Remarks" json:"remark"`
}

func (TestTableUserUnique) TableName() string {
	return "test_user_unique"
}

func TestMergeCreateUnique(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	model := TestTableUserUnique{}
	migrator := db.Set("gorm:table_comments", "User information table").Migrator()
	if migrator.HasTable(model) {
		if err = migrator.DropTable(model); err != nil {
			t.Fatalf("DropTable() error = %v", err)
		}
	}
	if err = migrator.AutoMigrate(model); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	} else {
		t.Log("AutoMigrate() success!")
	}

	data := []TestTableUserUnique{
		{
			UID:         "U1",
			Name:        "Lisa",
			Account:     "lisa",
			Password:    "H6aLDNr",
			PhoneNumber: "+8616666666666",
			Sex:         "0",
			UserType:    1,
			Enabled:     true,
		},
		{
			UID:         "U2",
			Name:        "Daniela",
			Account:     "daniela",
			Password:    "Si7l1sRIC79",
			PhoneNumber: "+8619999999999",
			Sex:         "1",
			UserType:    1,
			Enabled:     true,
		},
		{
			UID:         "U2",
			Name:        "Daniela",
			Account:     "daniela",
			Password:    "Si7l1sRIC79",
			PhoneNumber: "+8619999999999",
			Sex:         "1",
			UserType:    1,
			Enabled:     true,
		},
	}
	t.Run("MergeCreateUnique", func(t *testing.T) {
		tx := db.Create(&data)
		if err = tx.Error; err != nil {
			if strings.Contains(err.Error(), "ORA-00001") {
				t.Log(err) // ORA-00001: unique constraint violated
				var gotData []TestTableUserUnique
				tx = db.Where(`"UID" IN (?)`, []string{"U1", "U2"}).Find(&gotData)
				if err = tx.Error; err != nil {
					t.Fatal(err)
				} else {
					if len(gotData) > 0 {
						t.Error("Unique constraint violation, but some data was inserted!")
					} else {
						t.Log("Unique constraint violation, rolled back!")
					}
				}
			} else {
				t.Fatal(err)
			}
			return
		}
		dataJsonBytes, _ := json.MarshalIndent(data, "", "  ")
		t.Logf("result: %s", dataJsonBytes)
	})
}

func TestMergeCreateOnConflictColumnsExecution(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	model := TestTableUserUnique{}
	_ = db.Migrator().DropTable(model)
	require.NoError(t, db.Migrator().AutoMigrate(model), "expecting no error")

	base := &TestTableUserUnique{
		UID:     "U1",
		Name:    "Alpha",
		Enabled: true,
	}
	require.NoError(t, db.Create(base).Error, "expecting no error inserting base row")

	upsertUpdate := &TestTableUserUnique{
		UID:     "U1",
		Name:    "Beta",
		Enabled: true,
	}
	require.NoError(t, db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uid"}},
		DoUpdates: clause.AssignmentColumns([]string{"name"}),
	}).Create(upsertUpdate).Error, "expecting no error on non-PK upsert")

	var rows []TestTableUserUnique
	require.NoError(t, db.Where(`"UID" = ?`, "U1").Find(&rows).Error, "expecting no error querying rows")
	require.Len(t, rows, 1, "expected upsert to update existing row rather than insert duplicate")
	assert.Equal(t, "Beta", rows[0].Name, "expected name to be updated")

	upsertNoUpdate := &TestTableUserUnique{
		UID:     "U1",
		Name:    "Gamma",
		Enabled: true,
	}
	require.NoError(t, db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uid"}},
		DoUpdates: clause.AssignmentColumns([]string{"name"}),
		Where: clause.Where{
			Exprs: []clause.Expression{
				clause.Expr{SQL: "1 = 0"},
			},
		},
	}).Create(upsertNoUpdate).Error, "expecting no error on upsert with matched-update predicate")

	var got TestTableUserUnique
	require.NoError(t, db.Where(`"UID" = ?`, "U1").First(&got).Error, "expecting no error loading row")
	assert.Equal(t, "Beta", got.Name, "expected matched-update WHERE to prevent update")
}

type testModelOra03146TTC struct {
	Id          int64     `gorm:"primaryKey;autoIncrement:false;type:uint;size:20;default:0;comment:id" json:"SL_ID"`
	ApiName     string    `gorm:"type:VARCHAR2;size:100;default:null;comment:Interface Name" json:"SL_API_NAME"`
	RawReceive  string    `gorm:"type:VARCHAR2;size:4000;default:null;comment:Original request parameters" json:"SL_RAW_RECEIVE_JSON"`
	RawSend     string    `gorm:"type:VARCHAR2;size:4000;default:null;comment:Original response parameters" json:"SL_RAW_SEND_JSON"`
	DealReceive string    `gorm:"type:VARCHAR2;size:4000;default:null;comment:Processing request parameters" json:"SL_DEAL_RECEIVE_JSON"`
	DealSend    string    `gorm:"type:VARCHAR2;size:4000;default:null;comment:Handle response parameters" json:"SL_DEAL_SEND_JSON"`
	Code        string    `gorm:"type:VARCHAR2;size:16;default:null;comment:HTTP status" json:"SL_CODE"`
	CreatedTime time.Time `gorm:"type:date;default:null;comment:Creation time" json:"SL_CREATED_TIME"`
}

func TestOra03146TTC(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	model := testModelOra03146TTC{}
	migrator := db.Set("gorm:table_comments", "Test table for invalid buffer length issue in TTC field").Migrator()
	if migrator.HasTable(model) {
		if err = migrator.DropTable(model); err != nil {
			t.Fatalf("DropTable() error = %v", err)
		}
	}
	if err = migrator.AutoMigrate(model); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	} else {
		t.Log("AutoMigrate() success!")
	}

	data := testModelOra03146TTC{
		Id:          9578529926701056,
		ApiName:     "/v1/t100/packingNum",
		RawReceive:  "11111",
		RawSend:     "11111",
		DealReceive: "11111",
		DealSend:    "11111",
		Code:        "111",
		CreatedTime: time.Now(),
	}
	result := db.Create(&data)
	if err = result.Error; err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	t.Log("Execution successful, number of rows affected:", result.RowsAffected)
}

type testNoDefaultDBValues struct {
	UID  string `gorm:"type:varchar2;size:50;comment:User identity" json:"uid"`
	Name string `gorm:"size:50;comment:User name" json:"name"`

	Account  string `gorm:"type:varchar2;size:50;comment:Login account" json:"account"`
	Password string `gorm:"type:varchar2;size:512;comment:Login password (encrypted)" json:"password"`

	Email       string `gorm:"type:varchar2;size:128;comment:Email address" json:"email"`
	PhoneNumber string `gorm:"type:varchar2;size:15;comment:E.164" json:"phoneNumber"`

	Sex      string     `gorm:"type:char;size:1;comment:Gender" json:"sex"`
	Birthday *time.Time `gorm:"<-:create;comment:Birthday" json:"birthday,omitempty"`

	UserType int `gorm:"size:8;comment:User type" json:"userType"`

	Enabled bool   `gorm:"comment:Is enabled" json:"enabled"`
	Remark  string `gorm:"size:1024;comment:Remarks" json:"remark"`
}

func (testNoDefaultDBValues) TableName() string {
	return "test_no_default_db_values"
}

func TestCreateInBatches(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	model := testNoDefaultDBValues{}
	migrator := db.Set("gorm:table_comments", "Test table for fields without database-assigned default values").Migrator()
	if migrator.HasTable(model) {
		if err = migrator.DropTable(model); err != nil {
			t.Fatalf("DropTable() error = %v", err)
		}
	}
	if err = migrator.AutoMigrate(model); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	} else {
		t.Log("AutoMigrate() success!")
	}

	data := []testNoDefaultDBValues{
		{UID: "U1", Name: "Lisa", Account: "lisa", Password: "H6aLDNr", PhoneNumber: "+8616666666666", Sex: "0", UserType: 1, Enabled: true},
		{UID: "U2", Name: "Daniela", Account: "daniela", Password: "Si7l1sRIC79", PhoneNumber: "+8619999999999", Sex: "1", UserType: 1, Enabled: true},
		{UID: "U3", Name: "Tom", Account: "tom", Password: "********", PhoneNumber: "+8618888888888", Sex: "1", UserType: 1, Enabled: true},
		{UID: "U4", Name: "James", Account: "james", Password: "********", PhoneNumber: "+8617777777777", Sex: "1", UserType: 2, Enabled: true},
		{UID: "U5", Name: "John", Account: "john", Password: "********", PhoneNumber: "+8615555555555", Sex: "1", UserType: 1, Enabled: true},
	}
	t.Run("CreateInBatches", func(t *testing.T) {
		tx := db.CreateInBatches(&data, 2)
		if err = tx.Error; err != nil {
			t.Fatal(err)
		}
		dataJsonBytes, _ := json.MarshalIndent(data, "", "  ")
		t.Logf("result: %s", dataJsonBytes)
	})
}
