package xugu

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"
)

type xuguRows struct {
	rows_conn *xuguConn
	aR        *allResult
	results   *SelectResult
	colIdx    int
	prepared  bool
}

func (row *xuguRows) Next(dest []driver.Value) error {

	if row.results.rowIdx >= len(row.results.Values[0]) {
		//return errors.New("The result set has been released")
		return io.EOF
	}

	for j := 0; j < int(row.results.Field_Num); j++ {

		coluType := row.results.Fields[j].FieldType
		if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
			dest[j] = nil
		} else {
			switch coluType {

			case fieldType_BINARY,
				fieldType_CLOB,
				fieldType_BLOB:
				dest[j] = row.results.Values[j][row.results.rowIdx].Col_Data

			case fieldType_INTERVAL_Y, fieldType_INTERVAL_M, fieldType_INTERVAL_D,
				fieldType_INTERVAL_H, fieldType_INTERVAL_S,
				fieldType_INTERVAL_MI:
				dest[j] = binary.BigEndian.Uint32(row.results.Values[j][row.results.rowIdx].Col_Data)

			case fieldType_TIME,
				fieldType_TIME_TZ:
				timeTmp := int32(binary.BigEndian.Uint32(row.results.Values[j][row.results.rowIdx].Col_Data))
				//tv, _ := time.Parse("2006-01-02 15:04:05", string(reverseBytes(row.results.Values[j][row.results.rowIdx].Col_Data)))
				tv := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Add((time.Millisecond * time.Duration(timeTmp)))
				utcTime := tv.UTC()
				dest[j] = utcTime
			case fieldType_DATE:

				timeTmp := int32(binary.BigEndian.Uint32(row.results.Values[j][row.results.rowIdx].Col_Data))

				// 通过增加天数，生成 UTC 时间
				tv := time.Unix(0, 0).Add(time.Hour * 24 * time.Duration(timeTmp)) // 获取带日期的时间

				// 提取年月日部分，格式化为 YYYY-MM-DD
				dateOnly := time.Date(tv.Year(), tv.Month(), tv.Day(), 0, 0, 0, 0, time.UTC)
				dateString := dateOnly.Format("2006-01-02") // 格式化为字符串 "YYYY-MM-DD"

				// 将格式化后的日期字符串存储到目标数组
				dest[j] = dateString

			case fieldType_DATETIME,
				fieldType_DATETIME_TZ:

				if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
					dest[j] = nil
				} else {
					// 假设 timeTmp 是从字节数据中解析出来的时间戳
					timeTmp := binary.BigEndian.Uint64(row.results.Values[j][row.results.rowIdx].Col_Data)

					// 将时间戳转换为 Unix 时间（秒级）
					tv := time.Unix(0, int64(timeTmp)*1000000) // *1000000 是为了将微秒转换为纳秒

					// 格式化为 "YYYY-MM-DD HH:MM:SS" 格式
					formattedTime := tv.Format("2006-01-02 15:04:05") // "2006-01-02 15:04:05" 是 Go 的格式化模板

					// 将格式化后的时间存储到目标数组
					dest[j] = formattedTime
				}

			case fieldType_R4:
				if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
					dest[j] = nil
				} else {
					dest[j] = math.Float32frombits(binary.BigEndian.Uint32(row.results.Values[j][row.results.rowIdx].Col_Data))
				}

			case fieldType_R8:
				if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
					dest[j] = nil
				} else {
					dest[j] = math.Float64frombits(binary.BigEndian.Uint64(row.results.Values[j][row.results.rowIdx].Col_Data))
				}
			case fieldType_NUM:
				dest[j] = row.results.Values[j][row.results.rowIdx].Col_Data
			case fieldType_I1:
				if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
					dest[j] = nil
				} else {
					dest[j] = int8(row.results.Values[j][row.results.rowIdx].Col_Data[0])
				}
			case fieldType_I2:
				if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
					dest[j] = nil
				} else {
					dest[j] = int16(binary.BigEndian.Uint16(row.results.Values[j][row.results.rowIdx].Col_Data))
				}
			case fieldType_I4:
				if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
					dest[j] = nil
				} else {
					dest[j] = int32(binary.BigEndian.Uint32(row.results.Values[j][row.results.rowIdx].Col_Data))
				}
			case fieldType_I8:
				if len(row.results.Values[j][row.results.rowIdx].Col_Data) == 0 {
					dest[j] = nil
				} else {
					dest[j] = int64(binary.BigEndian.Uint64(row.results.Values[j][row.results.rowIdx].Col_Data))
					//dest[j] = row.results.Values[j][row.results.rowIdx].Col_Data
				}
			case fieldType_CHAR, fieldType_NCHAR:
				if row.results.Values[j][row.results.rowIdx].Col_Data == nil {
					dest[j] = string("")
				} else if row.results.Values[j][row.results.rowIdx].Col_Data[0] == 0x00 {
					dest[j] = string("")
				} else {
					dest[j] = string(row.results.Values[j][row.results.rowIdx].Col_Data)
				}
			default:

				//填入一行的数据
				//TODO这里长度改为一行长度
				dest[j] = make([]byte, len(row.results.Values))
				// Values[字段][0]
				dest[j] = row.results.Values[j][row.results.rowIdx].Col_Data

			}
		}
	}
	row.results.rowIdx++

	return nil
}

// Columns返回列的名字集，它的个数是从slice的长度中推断出来的。
// 如果不知道特定的列名，应该为该条目返回一个空的字符串
func (row *xuguRows) Columns() []string {

	var columns []string

	for _, v := range row.results.Fields {
		columns = append(columns, v.FieldName)
	}

	return columns
}

func (row *xuguRows) ColumnTypeScanType(index int) reflect.Type {
	//rintln(">>>>>ColumnTypeScanType ")

	return row.results.Fields[index].scanType()
}

// The driver is at the end of the current result set.
// Test to see if there is another result set after the current one.
// Only close Rows if there is no further result sets to read.
func (row *xuguRows) HasNextResultSet() bool {
	return row.aR.next != nil

}

// NextResultSet prepares the next result set for reading. It reports whether
// there is further result sets, or false if there is no further result set
// or if there is an error advancing to it. The Err method should be consulted
// to distinguish between the two cases.
//
// After calling NextResultSet, the Next method should always be called before
// scanning. If there are further result sets they may not have rows in the result
// set.
func (row *xuguRows) NextResultSet() error {

	if row.aR.next == nil {
		return fmt.Errorf("there are no multiple result sets available")
	}
	switch row.aR.next.rt {
	case errInfo:
		return fmt.Errorf("error: %s", string(row.aR.next.e.ErrStr))

	}

	row.results = row.aR.next.s
	row.aR = row.aR.next

	return nil
}

func (row *xuguRows) Close() error {
	return nil
}
