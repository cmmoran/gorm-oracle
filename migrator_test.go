package oracle

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMigrator_AutoMigrate(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}

	type args struct {
		drop     bool
		models   []interface{}
		comments []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "TestTableUser", args: args{drop: true, models: []interface{}{TestTableUser{}}, comments: []string{"User Information Table"}}},
		{name: "TestTableUserDrop", args: args{drop: true, models: []interface{}{TestTableUser{}}, comments: []string{"User Information Table"}}},
		{name: "TestTableUserNoComments", args: args{drop: true, models: []interface{}{TestTableUserNoComments{}}, comments: []string{"User Information Table"}}},
		{name: "TestTableUserAddColumn", args: args{models: []interface{}{TestTableUserAddColumn{}}, comments: []string{"User Information Table"}}},
		{name: "TestTableUserMigrateColumn", args: args{models: []interface{}{TestTableUserMigrateColumn{}}, comments: []string{"User Information Table"}}},
	}
	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.args.models) == 0 {
				t.Fatal("models is nil")
			}
			migrator := db.Set("gorm:table_comments", tt.args.comments).Migrator()

			if tt.args.drop {
				for _, model := range tt.args.models {
					if !migrator.HasTable(model) {
						continue
					}
					if err = migrator.DropTable(model); err != nil {
						t.Fatalf("DropTable() error = %v", err)
					}
				}
			}

			if err = migrator.AutoMigrate(tt.args.models...); (err != nil) != tt.wantErr {
				t.Errorf("AutoMigrate() error = %v, wantErr %v", err, tt.wantErr)
			} else if err == nil {
				t.Log("AutoMigrate() success!")
			}

			if idx == len(tests)-1 {
				wantUser := TestTableUserMigrateColumn{
					TestTableUser: TestTableUser{
						UID:         "U0",
						Name:        "someone",
						Account:     "guest",
						Password:    "MAkOvrJ8JV",
						Email:       "",
						PhoneNumber: "+8618888888888",
						Sex:         "1",
						UserType:    1,
						Enabled:     true,
						Remark:      "Ahmad",
					},
					AddNewColumn:       "AddNewColumnValue",
					CommentSingleQuote: "CommentSingleQuoteValue",
				}

				result := db.Create(&wantUser)
				if err = result.Error; err != nil {
					t.Fatal(err)
				}

				var gotUser TestTableUserMigrateColumn
				result.Where(&TestTableUser{UID: "U0"}).Find(&gotUser)
				if err = result.Error; err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(wantUser, gotUser) {
					t.Errorf("diff: %s", cmp.Diff(gotUser, wantUser))
				}
			}
		})
	}
}

type TestTablePartialIndex struct {
	gorm.Model
	Name string `gorm:"size:50;comment:User Name"`
	Age  uint8  `gorm:"size:8;comment:User Age"`
	Sex  string `gorm:"type:char;size:1;check:chk_there_can_be_only_two,lower(SEX)='m' or lower(SEX)='f';index:uni_there_can_be_only_two,unique,where:lower(sex) in ('m'\\,'f');"`
}

type TestTableCaseSensitive struct {
	gorm.Model
	Name string `gorm:"column:\"name\";size:50;comment:User Name"`
}

func (TestTableCaseSensitive) TableName() string {
	return "\"test_table_case_sensitive\""
}

type TestTableCaseSensitiveRegular struct {
	gorm.Model
	Name string `gorm:"size:50;comment:User Name"`
}

func (TestTableCaseSensitiveRegular) TableName() string {
	return "test_table_case_sensitive"
}

// TestTableUser Test User Information Table Model
type TestTableUser struct {
	ID   uint64 `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	UID  string `gorm:"type:varchar2;size:50;comment:User Identity" json:"uid"`
	Name string `gorm:"size:50;comment:User Name" json:"name"`

	Account  string `gorm:"type:varchar2;size:50;comment:Login Account" json:"account"`
	Password string `gorm:"type:varchar2;size:512;comment:Login Password (Encrypted)" json:"password"`

	Email       string `gorm:"type:varchar2;size:128;comment:Email Address" json:"email"`
	PhoneNumber string `gorm:"type:varchar2;size:15;comment:E.164" json:"phoneNumber"`

	Sex      string     `gorm:"type:char;size:1;comment:Gender" json:"sex"`
	Birthday *time.Time `gorm:"<-:create;comment:Birthday" json:"birthday,omitempty"`

	UserType int `gorm:"size:8;comment:User Type" json:"userType"`

	Enabled  bool   `gorm:"comment:Is Enabled" json:"enabled"`
	Penabled *bool  `gorm:"comment:Is penabled" json:"penabled"`
	Remark   string `gorm:"size:1024;comment:Remark" json:"remark"`
}

func (TestTableUser) TableName() string {
	return "test_user"
}

type TestTableUserNoComments struct {
	ID   uint64 `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey" json:"id"`
	UID  string `gorm:"type:varchar2;size:50" json:"uid"`
	Name string `gorm:"size:50" json:"name"`

	Account  string `gorm:"type:varchar2;size:50" json:"account"`
	Password string `gorm:"type:varchar2;size:512" json:"password"`

	Email       string `gorm:"type:varchar2;size:128" json:"email"`
	PhoneNumber string `gorm:"type:varchar2;size:15" json:"phoneNumber"`

	Sex      string    `gorm:"type:char;size:1" json:"sex"`
	Birthday time.Time `gorm:"" json:"birthday"`

	UserType int `gorm:"size:8" json:"userType"`

	Enabled bool   `gorm:"" json:"enabled"`
	Remark  string `gorm:"size:1024" json:"remark"`
}

func (TestTableUserNoComments) TableName() string {
	return "test_user"
}

type TestTableUserAddColumn struct {
	TestTableUser

	AddNewColumn string `gorm:"type:varchar2;size:100;comment:Add New Column"`
}

func (TestTableUserAddColumn) TableName() string {
	return "test_user"
}

type TestTableUserMigrateColumn struct {
	TestTableUser

	AddNewColumn       string `gorm:"type:varchar2;size:100;comment:Test Add New Column"`
	CommentSingleQuote string `gorm:"comment:Comments with single quote'[']'"`
}

func (TestTableUserMigrateColumn) TableName() string {
	return "test_user"
}

type testTableColumnTypeModel struct {
	ID   int64  `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey"`
	Name string `gorm:"size:50"`
	Age  uint8  `gorm:"size:8"`

	Avatar []byte `gorm:""`

	Balance float64 `gorm:"type:decimal;precision:18;scale:2"`
	Remark  string  `gorm:"size:-1"`
	Enabled bool    `gorm:""`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt
}

func (t testTableColumnTypeModel) TableName() string {
	return "test_table_column_type"
}

func TestMigrator_TableColumnType(t *testing.T) {
	db, err := dbNamingCase, dbErrors[0]
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Log("db is nil!")
		return
	}
	testModel := new(testTableColumnTypeModel)

	type args struct {
		model interface{}
		drop  bool
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "create", args: args{model: testModel}},
		{name: "alter", args: args{model: testModel, drop: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err = db.AutoMigrate(tt.args.model); err != nil {
				t.Errorf("AutoMigrate failed：%v", err)
			}
			if tt.args.drop {
				_ = db.Migrator().DropTable(tt.args.model)
			}
		})
	}
}

type testFieldNameIsReservedWord struct {
	ID int64 `gorm:"size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey"`

	FLOAT float64 `gorm:"type:decimal;precision:18;scale:2"`
	DESC  string  `gorm:"size:-1"`
	ON    bool

	Order int
	Sort  int

	CREATE time.Time
	UPDATE time.Time
	DELETE gorm.DeletedAt
}

func (t testFieldNameIsReservedWord) TableName() string {
	return "test_name_is_reserved_word"
}

func TestMigrator_FieldNameIsReservedWord(t *testing.T) {
	if err := dbErrors[0]; err != nil {
		t.Fatal(err)
	}
	if dbNamingCase == nil {
		t.Log("dbNamingCase is nil!")
		return
	}
	if err := dbErrors[1]; err != nil {
		t.Fatal(err)
	}
	if dbIgnoreCase == nil {
		t.Log("dbNamingCase is nil!")
		return
	}

	testModel := new(testFieldNameIsReservedWord)
	err := dbNamingCase.Migrator().DropTable(testModel)
	require.NoError(t, err, "expecting no error")
	err = dbIgnoreCase.Migrator().DropTable(testModel)
	require.NoError(t, err, "expecting no error")

	type args struct {
		db    *gorm.DB
		model interface{}
		drop  bool
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "createNamingCase", args: args{db: dbNamingCase, model: testModel}},
		{name: "alterNamingCase", args: args{db: dbNamingCase, model: testModel, drop: true}},
		{name: "createIgnoreCase", args: args{db: dbIgnoreCase, model: testModel}},
		{name: "alterIgnoreCase", args: args{db: dbIgnoreCase, model: testModel, drop: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.args.db
			if err = db.AutoMigrate(tt.args.model); err != nil {
				t.Errorf("AutoMigrate failed：%v", err)
			}
			if tt.args.drop {
				_ = db.Migrator().DropTable(tt.args.model)
			}
		})
	}
}

func TestMigrator_DatatypesJsonMapNamingCase(t *testing.T) {
	if err := dbErrors[0]; err != nil {
		t.Fatal(err)
	}
	if dbNamingCase == nil {
		t.Log("dbNamingCase is nil!")
		return
	}

	type testJsonMapNamingCase struct {
		gorm.Model

		Extras JSONMap `gorm:"check:\"EXTRAS\" IS JSON"`
	}
	testModel := new(testJsonMapNamingCase)
	_ = dbNamingCase.Migrator().DropTable(testModel)

	type args struct {
		db    *gorm.DB
		model interface{}
		drop  bool
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "createDatatypesJsonMapNamingCase", args: args{db: dbNamingCase, model: testModel}},
		{name: "alterDatatypesJsonMapNamingCase", args: args{db: dbNamingCase, model: testModel, drop: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.args.db
			if err := db.AutoMigrate(tt.args.model); err != nil {
				t.Errorf("AutoMigrate failed：%v", err)
			}
			if tt.args.drop {
				_ = db.Migrator().DropTable(tt.args.model)
			}
		})
	}
}

func TestMigrator_DatatypesJsonMapIgnoreCase(t *testing.T) {
	if err := dbErrors[1]; err != nil {
		t.Fatal(err)
	}
	if dbIgnoreCase == nil {
		t.Log("dbNamingCase is nil!")
		return
	}

	type tesJsonMapIgnoreCase struct {
		gorm.Model

		Extras JSONMap `gorm:"check:extras IS JSON"`
	}
	testModel := new(tesJsonMapIgnoreCase)
	_ = dbIgnoreCase.Migrator().DropTable(testModel)

	type args struct {
		db    *gorm.DB
		model interface{}
		drop  bool
	}
	tests := []struct {
		name string
		args args
	}{
		{name: "createDatatypesJsonMapIgnoreCase", args: args{db: dbIgnoreCase, model: testModel}},
		{name: "alterDatatypesJsonMapIgnoreCase", args: args{db: dbIgnoreCase, model: testModel, drop: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.args.db
			if err := db.AutoMigrate(tt.args.model); err != nil {
				t.Errorf("AutoMigrate failed：%v", err)
			}
			if tt.args.drop {
				_ = db.Migrator().DropTable(tt.args.model)
			}
		})
	}
}
