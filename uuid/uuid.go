package uuid

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"reflect"
)

func New() UUID {
	return UUID(uuid.New())
}

func From(b []byte) *UUID {
	return func() *UUID {
		v := UUID(b[:])
		return &v
	}()
}

type UUID [16]byte

// GormDBDataType tells GORM what column type to use.
func (UUID) GormDBDataType(*gorm.DB, *schema.Field) string {
	// Oracle RAW(16) is the idiomatic binary(16) for UUIDs
	return "RAW(16)"
}

func (u UUID) GormValue(context.Context, *gorm.DB) clause.Expr {
	return clause.Expr{
		SQL:  "HEXTORAW(?)",
		Vars: []interface{}{fmt.Sprintf("%s", u.String())},
	}
}

// Value implements driver.Valuer, called on INSERT/UPDATE.
// We hand back the 16-byte array directly.
func (u UUID) Value() (driver.Value, error) {
	return u[:], nil
}

// Scan implements sql.Scanner, called on SELECT.
// src will be a []byte of length 16.
func (u *UUID) Scan(src interface{}) error {
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into UUIDRaw", src)
	}
	if len(b) != 16 {
		return fmt.Errorf("unexpected RAW length %d for UUIDRaw", len(b))
	}
	// Copy bytes into a uuid.UUID
	var parsed uuid.UUID
	copy(parsed[:], b)
	*u = UUID(parsed)
	return nil
}

func (u *UUID) String() string {
	var buf [32]byte
	hex.Encode(buf[:], u[:])

	return string(buf[:])
}

func (u *UUID) ToUUID() uuid.UUID {
	return uuid.UUID(u[:])
}

func (u *UUID) ToUUIDPtr() *uuid.UUID {
	v := uuid.UUID(u[:])
	return &v
}

func (u *UUID) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, u.String())), nil
}

func (u *UUID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	uu, err := uuid.Parse(s)
	if err != nil {
		return err
	}
	*u = UUID(uu)

	return nil
}

type Serializer struct{}

func (Serializer) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, src interface{}) (err error) {
	if src == nil {
		if field.FieldType.Kind() == reflect.Ptr {
			return field.Set(ctx, dst, (*UUID)(nil))
		} else {
			return field.Set(ctx, dst, UUID{})
		}
	}
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into UUID", src)
	}
	if len(b) != 16 {
		return fmt.Errorf("unexpected RAW length %d for UUID", len(b))
	}
	// Copy bytes into a UUID
	var parsed UUID
	copy(parsed[:], b)
	if field.FieldType.Kind() == reflect.Ptr {
		return field.Set(ctx, dst, &parsed)
	} else {
		return field.Set(ctx, dst, parsed)
	}
}

func (Serializer) Value(_ context.Context, field *schema.Field, _ reflect.Value, fieldValue interface{}) (interface{}, error) {
	if fieldValue == nil {
		return nil, nil
	}
	if field.FieldType.Kind() == reflect.Ptr {
		if u, ok := fieldValue.(*UUID); ok {
			return u[:], nil
		}
	} else {
		if u, ok := fieldValue.(UUID); ok {
			return u[:], nil
		}
	}
	return fieldValue, nil
}
