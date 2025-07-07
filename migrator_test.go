package oracle

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

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
		{name: "TestTableUser", args: args{models: []interface{}{TestTableUser{}}, comments: []string{"User Information Table"}}},
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
				gotUserBytes, _ := json.Marshal(gotUser)
				t.Logf("gotUser Result: %s", gotUserBytes)
				if !reflect.DeepEqual(gotUser, wantUser) {
					wantUserBytes, _ := json.Marshal(wantUser)
					t.Errorf("wantUser Info: %s", wantUserBytes)
				}
			}
		})
	}
}

// TestTableUser Test User Information Table Model
type TestTableUser struct {
	ID   uint64 `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey;comment:Auto Increment ID" json:"id"`
	UID  string `gorm:"column:uid;type:varchar(50);comment:User Identity" json:"uid"`
	Name string `gorm:"column:name;size:50;comment:User Name" json:"name"`

	Account  string `gorm:"column:account;type:varchar(50);comment:Login Account" json:"account"`
	Password string `gorm:"column:password;type:varchar(512);comment:Login Password (Encrypted)" json:"password"`

	Email       string `gorm:"column:email;type:varchar(128);comment:Email Address" json:"email"`
	PhoneNumber string `gorm:"column:phone_number;type:varchar(15);comment:E.164" json:"phoneNumber"`

	Sex      string     `gorm:"column:sex;type:char(1);comment:Gender" json:"sex"`
	Birthday *time.Time `gorm:"column:birthday;->:false;<-:create;comment:Birthday" json:"birthday,omitempty"`

	UserType int `gorm:"column:user_type;size:8;comment:User Type" json:"userType"`

	Enabled  bool   `gorm:"column:enabled;comment:Is Enabled" json:"enabled"`
	PEnabled *bool  `gorm:"column:penabled;comment:Is penabled" json:"penabled"`
	Remark   string `gorm:"column:remark;size:1024;comment:Remark" json:"remark"`
}

func (TestTableUser) TableName() string {
	return "test_user"
}

type TestTableUserNoComments struct {
	ID   uint64 `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey" json:"id"`
	UID  string `gorm:"column:name;type:varchar(50)" json:"uid"`
	Name string `gorm:"column:name;size:50" json:"name"`

	Account  string `gorm:"column:account;type:varchar(50)" json:"account"`
	Password string `gorm:"column:password;type:varchar(512)" json:"password"`

	Email       string `gorm:"column:email;type:varchar(128)" json:"email"`
	PhoneNumber string `gorm:"column:phone_number;type:varchar(15)" json:"phoneNumber"`

	Sex      string    `gorm:"column:sex;type:char(1)" json:"sex"`
	Birthday time.Time `gorm:"column:birthday" json:"birthday"`

	UserType int `gorm:"column:user_type;size:8" json:"userType"`

	Enabled bool   `gorm:"column:enabled" json:"enabled"`
	Remark  string `gorm:"column:remark;size:1024" json:"remark"`
}

func (TestTableUserNoComments) TableName() string {
	return "test_user"
}

type TestTableUserAddColumn struct {
	TestTableUser

	AddNewColumn string `gorm:"column:add_new_column;type:varchar(100);comment:Add New Column"`
}

func (TestTableUserAddColumn) TableName() string {
	return "test_user"
}

type TestTableUserMigrateColumn struct {
	TestTableUser

	AddNewColumn       string `gorm:"column:add_new_column;type:varchar(100);comment:Test Add New Column"`
	CommentSingleQuote string `gorm:"column:comment_single_quote;comment:Comments with single quote'[']'"`
}

func (TestTableUserMigrateColumn) TableName() string {
	return "test_user"
}

type testTableColumnTypeModel struct {
	ID   int64  `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey"`
	Name string `gorm:"column:name;size:50"`
	Age  uint8  `gorm:"column:age;size:8"`

	Avatar []byte `gorm:"column:avatar;"`

	Balance float64 `gorm:"column:balance;type:decimal(18, 2)"`
	Remark  string  `gorm:"column:remark;size:-1"`
	Enabled bool    `gorm:"column:enabled;"`

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
	ID int64 `gorm:"column:id;size:64;not null;autoIncrement:true;autoIncrementIncrement:1;primaryKey"`

	FLOAT float64 `gorm:"type:decimal(18, 2)"`
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
	_ = dbNamingCase.Migrator().DropTable(testModel)
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
		{name: "createNamingCase", args: args{db: dbNamingCase, model: testModel}},
		{name: "alterNamingCase", args: args{db: dbNamingCase, model: testModel, drop: true}},
		{name: "createIgnoreCase", args: args{db: dbIgnoreCase, model: testModel}},
		{name: "alterIgnoreCase", args: args{db: dbIgnoreCase, model: testModel, drop: true}},
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

		Extras JSONMap `gorm:"check:\"extras\" IS JSON"`
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
