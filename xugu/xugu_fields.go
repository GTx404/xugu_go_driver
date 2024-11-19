package xugu

import (
	"database/sql"
	"reflect"
	"time"
)

type FieldDescri struct {
	FieldNameLen    int
	FieldName       string
	FieldType       fieldType
	FieldPreciScale fieldPreciScaleInfo
	FieldFlag       uint32
}

type fieldPreciScaleInfo struct {
	scale    uint16
	accuracy uint16
}

type fieldType uint16

const (
	fieldType_EMPTY fieldType = iota + 0x00
	fieldType_NULL
	fieldType_BOOL
	fieldType_I1
	fieldType_I2
	fieldType_I4
	fieldType_I8
	fieldType_NUM
	fieldType_R4
	fieldType_R8

	fieldType_DATE
	fieldType_TIME
	fieldType_TIME_TZ
	fieldType_DATETIME
	fieldType_DATETIME_TZ

	fieldType_INTERVAL_Y
	fieldType_INTERVAL_Y2M
	fieldType_INTERVAL_M

	fieldType_INTERVAL_D
	fieldType_INTERVAL_D2H
	fieldType_INTERVAL_H
	fieldType_INTERVAL_D2M
	fieldType_INTERVAL_H2M
	fieldType_INTERVAL_MI
	fieldType_INTERVAL_D2S
	fieldType_INTERVAL_H2S
	fieldType_INTERVAL_M2S
	fieldType_INTERVAL_S

	fieldType_ROWVER
	fieldType_GUID
	fieldType_CHAR
	fieldType_NCHAR
	fieldType_CLOB

	fieldType_BINARY
	fieldType_BLOB

	fieldType_GEOM
	fieldType_POINT
	fieldType_BOX
	fieldType_POLYLINE
	fieldType_POLYGON

	fieldType_BLOB_I
	fieldType_BLOB_S
	fieldType_BLOB_M
	fieldType_BLOB_OM
	fieldType_STREAM
	fieldType_ROWID
	fieldType_SIBLING
	fieldType_MAX_SYS fieldType = 47

	fieldType_BLADE_BEGIN fieldType = 101
	fieldType_BLADE_END   fieldType = 1000

	fieldType_OBJECT fieldType = 1001 // object type
	fieldType_REFROW
	fieldType_RECORD  // record type
	fieldType_VARRAY  // array type
	fieldType_TABLE   // table type
	fieldType_ITABLE  // Idxby table
	fieldType_CURSOR  // involved ref-record type (cannot change)
	fieldType_REFCUR  // REF_CURSOR type
	fieldType_ROWTYPE // ref row type
	fieldType_COLTYPE // ref column type
	fieldType_CUR_REC
	fieldType_PARAM
)

/* {{ */
func (self *FieldDescri) typeDatabaseName() string {
	switch self.FieldType {
	case fieldType_BOOL:
		return "BOOLEAN"
	case fieldType_CHAR, fieldType_NCHAR:
		return "CHAR"
	case fieldType_I1:
		return "TINYINT"
	case fieldType_I2:
		return "SHORT"
	case fieldType_I4:
		return "INTEGER"
	case fieldType_I8:
		return "BIGINT"
	case fieldType_R4:
		return "FLOAT"
	case fieldType_R8:
		return "DOUBLE"
	case fieldType_NUM:
		return "NUMERIC"
	case fieldType_DATE:
		return "DATE"
	case fieldType_TIME:
		return "TIME"
	case fieldType_TIME_TZ:
		return "TIMEZONE"
	case fieldType_DATETIME:
		return "DATETIME"
	case fieldType_DATETIME_TZ:
		return "DATETIME TIMEZONE"
	case fieldType_BINARY:
		return "BINARY"
	case fieldType_INTERVAL_Y:
		return "INTERVAL YEAR"
	case fieldType_INTERVAL_Y2M:
		return "INTERVAL YEAR TO MONTH"
	case fieldType_INTERVAL_D2S:
		return "INTERVAL DAY TO SECOND"
	case fieldType_CLOB:
		return "CLOB"
	case fieldType_BLOB:
		return "BLOB"
	default:
		return ""
	}
}

/* {{ */
func (self *FieldDescri) scanType() reflect.Type {

	switch self.FieldType {

	case fieldType_BOOL:
		return scanTypeBool
	case fieldType_I1:
		return scanTypeInt8
	case fieldType_I2:
		return scanTypeInt16
	case fieldType_I4:
		return scanTypeInt32
	case fieldType_I8:
		return scanTypeInt64
	case fieldType_R4:
		return scanTypeFloat32
	case fieldType_R8:
		return scanTypeFloat64
	case fieldType_DATE,
		fieldType_TIME,
		fieldType_DATETIME:
		return scanTypeNullTime
	case fieldType_TIME_TZ,
		fieldType_DATETIME_TZ,
		fieldType_CHAR,
		fieldType_NCHAR,
		fieldType_BINARY,
		//fieldTypeInterval,
		fieldType_NUM,
		fieldType_INTERVAL_Y2M,
		fieldType_INTERVAL_D2S,
		//fieldTypeLob,
		fieldType_CLOB,
		fieldType_BLOB:
		return scanTypeRawBytes
	default:
		return scanTypeUnknown

	}
}

var (
	scanTypeFloat32   = reflect.TypeOf(float32(0))
	scanTypeFloat64   = reflect.TypeOf(float64(0))
	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
	scanTypeNullTime  = reflect.TypeOf(time.Time{})
	scanTypeInt8      = reflect.TypeOf(int8(0))
	scanTypeInt16     = reflect.TypeOf(int16(0))
	scanTypeInt32     = reflect.TypeOf(int32(0))
	scanTypeInt64     = reflect.TypeOf(int64(0))
	scanTypeUnknown   = reflect.TypeOf(new(interface{}))
	scanTypeRawBytes  = reflect.TypeOf(sql.RawBytes{})
	scanTypeUint8     = reflect.TypeOf(uint8(0))
	scanTypeUint16    = reflect.TypeOf(uint16(0))
	scanTypeUint32    = reflect.TypeOf(uint32(0))
	scanTypeUint64    = reflect.TypeOf(uint64(0))
	scanTypeBool      = reflect.TypeOf(bool(false))
)


