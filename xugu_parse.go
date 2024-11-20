package xugu

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type xuguValue struct {
	// 布尔值，如果值为 true，表示当前字段的数据类型是大对象数据类型
	islob bool
	//字段名
	paramName []byte
	//paramNameLength [2]byte
	// 字段的实际值
	value []byte
	// 值的长度
	valueLength int
	// 字段的类型
	types fieldType
}

// 判断参数个数
func assertParamCount(query string) int {

	paramCount := strings.Count(query, "?")

	return paramCount
}
func assertParamType(dV driver.Value, values *[]xuguValue) error {
	var dest xuguValue
	switch srcv := dV.(type) {

	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(srcv))
		dest.value = buf
		dest.valueLength = 8
		dest.islob = false
		dest.types = fieldType_I8

	case float64:
		S := strconv.FormatFloat(srcv, 'f', 15, 64)
		dest.value = []byte(S)
		dest.valueLength = len(S)
		dest.islob = false
		dest.types = fieldType_CHAR

	case bool:
		//S := strconv.FormatBool(srcv)
		var tmp []byte
		if srcv {
			tmp = []byte{1}
		} else {
			tmp = []byte{0}
		}

		dest.value = []byte(tmp)
		dest.valueLength = 1
		dest.islob = false
		dest.types = fieldType_BOOL

	case string:
		dest.value = []byte(srcv)
		dest.valueLength = len(srcv)
		dest.islob = false
		dest.types = fieldType_CHAR
		if dest.valueLength == 0 {
			dest.valueLength = 1
			dest.value = []byte{0}
		}

	case time.Time:
		tm := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
			srcv.Year(), int(srcv.Month()), srcv.Day(),
			srcv.Hour(), srcv.Minute(), srcv.Second())
		dest.value = []byte(tm)
		//	dest.valueLength = strings.Count(tm, "") - 1
		dest.valueLength = len(tm)
		dest.islob = false
		//dest.types = fieldType_TIME
		dest.types = fieldType_CHAR
	case []byte:
		dest.value = srcv
		dest.valueLength = len(srcv)
		dest.islob = true
		dest.types = fieldType_BLOB
		if dest.valueLength == 0 {
			dest.valueLength = 1
			dest.value = []byte{0}
		}

	case nil:
		dest.value = nil
		dest.valueLength = 0
		dest.islob = false
		dest.types = fieldType_NULL

	default:
		return errors.New("unknown data type")
	}

	*values = append(*values, dest)
	return nil
}

func parseMsg(readBuf *buffer, pConn *xuguConn) (*allResult, error) {
	var err error
	i := 0
	//aR := allResult{}
	//head node
	aRPoint := &allResult{}
	//move node
	aR := aRPoint
	for {
		char := readBuf.peekChar()
		if i != 0 {
			re := &allResult{}
			//re.effectNum = aR.effectNum
			aR.next = re
			aR = re
		}
		i++
		switch char {

		case 'K':
			readBuf.reset()
			return aRPoint, nil

		case '$':
			readBuf.idx++
			if aR.f, err = parseFormArgDescri(readBuf); err != nil {
				return nil, err
			}
			aR.rt = formArgDescri
			//return &aR, err
		case 'A':
			readBuf.idx++
			if aR.s, err = parseSelectResult(readBuf); err != nil {
				return nil, err
			}
			aR.rt = selectResult
		//	return &aR, err

		case 'I':
			readBuf.idx++
			if aR.i, err = parseInsertResult(readBuf); err != nil {
				return nil, err
			}
			aRPoint.effectNum++
			aRPoint.i.EffectNum = aRPoint.effectNum
			aR.rt = insertResult
			//return &aR, err

		case 'U':
			readBuf.idx++
			if aR.u, err = parseUpdateResult(readBuf); err != nil {
				return nil, err
			}
			aR.rt = updateResult
			//return &aR, err

		case 'D':
			readBuf.idx++
			if aR.d, err = parseDeleteResult(readBuf); err != nil {
				return nil, err
			}
			aR.rt = deleteResult
		//	return &aR, err

		case 'E':
			readBuf.idx++

			if aR.e, err = parseErrInfo(readBuf); err != nil {
				return nil, err
			}
			pConn.errStr = aR.e.ErrStr
			aR.rt = errInfo
			//return &aR, err

		case 'W':
			readBuf.idx++
			if aR.w, err = parseWarnInfo(readBuf); err != nil {
				return nil, err
			}
			aR.rt = warnInfo
			//return &aR, err

		case 'M':
			readBuf.idx++
			if aR.m, err = parseMessage(readBuf); err != nil {
				return nil, err
			}
			aR.rt = message
			//return &aR, err

		default:
			return nil, errors.New("parseMsg: unknown message type")
		}

	}
}
func parseSelectResult(readBuf *buffer) (*SelectResult, error) {
	data := &SelectResult{}

	var char byte

	//Field_Num
	fn, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}

	Field_Num := binary.LittleEndian.Uint32(fn)
	data.Field_Num = Field_Num
	data.rowIdx = 0

	//获取字段信息
	for i := 0; i < int(Field_Num); i++ {

		field := FieldDescri{}

		//Field_Name_Len
		Field_Name_Len, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}
		field.FieldNameLen = int(binary.LittleEndian.Uint32(Field_Name_Len))

		//Field_Name:
		Field_Name, err := readBuf.readNext(field.FieldNameLen, false)
		if err != nil {
			return nil, err
		}
		parts := strings.Split(string(Field_Name), ".")
		field.FieldName = parts[len(parts)-1]

		//Field_DType:
		Field_DType, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}
		field.FieldType = fieldType(binary.LittleEndian.Uint32(Field_DType))

		//Field_Preci_Scale:
		Field_Preci_Scale, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}

		fieldPreciScale := binary.LittleEndian.Uint32(Field_Preci_Scale)
		if int32(fieldPreciScale) <= 0 {
			field.FieldPreciScale = fieldPreciScaleInfo{
				scale:    0,
				accuracy: 0,
			}
		} else {
			field.FieldPreciScale = fieldPreciScaleInfo{
				scale:    uint16(fieldPreciScale >> 16),
				accuracy: uint16(fieldPreciScale & 0xFFFF),
			}
		}

		//Field_Flag:
		Field_Flag, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}
		field.FieldFlag = binary.LittleEndian.Uint32(Field_Flag)
		data.Fields = append(data.Fields, field)
	}

	data.Values = make([][]FieldValue, data.Field_Num)

	//获取字段的行值,并判断类型
	// 使用 Peek 方法检查下一个字节是否为'R'或'K'
	for {
		char = readBuf.peekChar()
		//readBuf.idx++
		if char == 'K' {
			return data, nil
		} else if char == 'R' { //接收字段信息后不是k 那一定是 R

			colIdx := 0
			//typeIdx := 0
			readBuf.idx++
			for i := 0; i < int(Field_Num); i++ {
				col := FieldValue{}
				//获取数据的大小
				Col_len, err := readBuf.readNext(4, true)
				if err != nil {
					return nil, err
				}
				col.Col_len = binary.LittleEndian.Uint32(Col_len)

				//获取数据的值
				col.Col_Data, err = readBuf.readNext(int(col.Col_len), false)
				if err != nil {
					return nil, err
				}

				data.Values[colIdx] = append(data.Values[colIdx], col)
				colIdx++

			} //for end

		} else if char == '$' {
			fad, err := parseFormArgDescri(readBuf)
			if err != nil {
				return nil, err
			}

			char := readBuf.peekChar()
			//既不是R 也不是K 代表该行还有其他字段内容没有读取完成
			if char == 'K' {
				data.fad = fad
				return data, nil
				//break
			}

			return nil, errors.New("select to $ parse failed")
		} else {
			return data, nil
		}
	}

}

func parseInsertResult(readBuf *buffer) (*InsertResult, error) {

	//Rowid_Len
	Rowid_L, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	Rowid_Len := binary.LittleEndian.Uint32(Rowid_L)
	//Rowid_Data
	encoded, err := readBuf.readNext(int(Rowid_Len), false)
	if err != nil {
		return nil, err
	}

	//检测是否结束
	// char := readBuf.peekChar()

	// if char == 'K' {
	// 	return &InsertResult{
	// 		RowidLen:  Rowid_Len,
	// 		RowidData: encoded,
	// 	}, nil
	// }

	return &InsertResult{
		RowidLen:  Rowid_Len,
		RowidData: encoded,
	}, nil
}

func parseUpdateResult(readBuf *buffer) (*UpdateResult, error) {
	updatas, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	updateNum := binary.LittleEndian.Uint32(updatas)

	return &UpdateResult{UpdateNum: updateNum}, nil
}

func parseDeleteResult(readBuf *buffer) (*DeleteResult, error) {
	deletes, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	deleteNum := binary.LittleEndian.Uint32(deletes)

	return &DeleteResult{DeleteNum: deleteNum}, nil
}

func parseProcRet(readBuf *buffer) (*ProcRet, error) {
	retDypes, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	retDType := binary.LittleEndian.Uint32(retDypes)
	retDataLens, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	retDataLen := binary.LittleEndian.Uint32(retDataLens)
	retData, err := readBuf.readNext(int(retDataLen), false)
	if err != nil {
		return nil, err
	}

	return &ProcRet{RetDType: retDType, RetDataLen: retDataLen, RetData: retData}, nil
}

func parseOutParamRet(readBuf *buffer) (*OutParamRet, error) {
	outParamNos, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	outParamNo := binary.LittleEndian.Uint32(outParamNos)

	outParamDTypes, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	outParamDType := binary.LittleEndian.Uint32(outParamDTypes)

	outParamLens, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	outParamLen := binary.LittleEndian.Uint32(outParamLens)

	outParamData, err := readBuf.readNext(int(outParamLen), false)
	if err != nil {
		return nil, err
	}

	return &OutParamRet{
		OutParamNo:    outParamNo,
		OutParamDType: outParamDType,
		OutParamLen:   outParamLen,
		OutParamData:  outParamData,
	}, nil
}

func parseErrInfo(readBuf *buffer) (*ErrInfo, error) {
	errStrLens, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	errStrLen := binary.LittleEndian.Uint32(errStrLens)

	errStr, err := readBuf.readNext(int(errStrLen), false)
	if err != nil {
		return nil, err
	}
	return &ErrInfo{ErrStrLen: errStrLen, ErrStr: errStr}, nil

}

func parseWarnInfo(readBuf *buffer) (*WarnInfo, error) {
	warnStrLens, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	warnStrLen := binary.LittleEndian.Uint32(warnStrLens)

	warnStr, err := readBuf.readNext(int(warnStrLen), false)
	if err != nil {
		return nil, err
	}
	return &WarnInfo{WarnStrLen: warnStrLen, WarnStr: warnStr}, nil
}

func parseMessage(readBuf *buffer) (*Message, error) {
	msgStrLens, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	msgStrLen := binary.LittleEndian.Uint32(msgStrLens)

	msgStr, err := readBuf.readNext(int(msgStrLen), false)
	if err != nil {
		return nil, err
	}

	return &Message{MsgStrLen: msgStrLen, MsgStr: msgStr}, nil
}

func parseFormArgDescri(readBuf *buffer) (*FormArgDescri, error) {
	//	FormArgDescri:   '$' Arg_Num { Arg_Name_Len Arg_Name Arg_No Arg_DType Arg_Preci_Scale }+
	Arg_Nums, err := readBuf.readNext(4, true)
	if err != nil {
		return nil, err
	}
	Arg_Num := binary.LittleEndian.Uint32(Arg_Nums)
	formArgDescri := &FormArgDescri{ArgNum: Arg_Num}
	for i := 0; i < int(Arg_Num); i++ {
		arg := ArgDescri{}
		//Arg_Name_Len
		ArgNameLen, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}
		arg.ArgNameLen = binary.LittleEndian.Uint32(ArgNameLen)
		//Arg_Name
		arg.ArgName, err = readBuf.readNext(int(arg.ArgNameLen), false)
		if err != nil {
			return nil, err
		}
		//Arg_No
		ArgNo, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}
		arg.ArgNo = binary.LittleEndian.Uint32(ArgNo)
		//Argg_DType
		ArgDType, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}
		arg.ArgDType = binary.LittleEndian.Uint32(ArgDType)
		//Arg_Preci_Scale
		ArgPreciScale, err := readBuf.readNext(4, true)
		if err != nil {
			return nil, err
		}
		arg.ArgPreciScale = binary.LittleEndian.Uint32(ArgPreciScale)
		formArgDescri.Args = append(formArgDescri.Args, arg)
	}
	return formArgDescri, nil
}
