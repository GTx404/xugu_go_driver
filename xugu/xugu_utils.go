package xugu

import (
	"encoding/binary"
	"fmt"
	"strings"
)

func parseDSN(dsn string) dsnConfig {
	// Initialize a dsnConfig struct
	var config dsnConfig

	// Split the string by semicolons
	pairs := strings.Split(dsn, ";")

	// Iterate over the pairs and map them to the struct fields
	for _, pair := range pairs {
		// Split each pair by the equals sign
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key, value := strings.TrimSpace(kv[0]), strings.Trim(strings.TrimSpace(kv[1]), "'")
		keyL := strings.ToLower(key)

		// Map the key to the appropriate struct field
		switch keyL {
		case "ip":
			config.IP = value
		case "port":
			config.Port = value
		case "db":
			config.Database = value
		case "user":
			config.User = value
		case "pwd":
			config.Password = value
		case "encryptor":
			config.Encryptor = value
		case "char_set":
			config.CharSet = value
		case "time_zone":
			config.TimeZone = value
		case "iso_level":
			config.IsoLevel = value
		case "lock_timeout":
			config.LockTimeout = value
		case "auto_commit":
			config.AutoCommit = value
		case "strict_commit":
			config.StrictCommit = value
		case "result":
			config.Result = value
		case "return_schema":
			config.ReturnSchema = value
		case "return_cursor_id":
			config.ReturnCursorID = value
		case "lob_ret":
			config.LobRet = value
		case "return_rowid":
			config.ReturnRowid = value
		case "version":
			config.Version = value
		}
	}

	return config
}

func generateLoginString(config dsnConfig) string {
	baseString := "login   database = '%s' user = '%s'  password = '%s' "
	additionalParams := ""

	if config.Encryptor != "" {
		additionalParams += fmt.Sprintf(" encryptor='%s'", config.Encryptor)
	}
	if config.CharSet != "" {
		additionalParams += fmt.Sprintf(" char_set='%s'", config.CharSet)
	}
	if config.TimeZone != "" {
		additionalParams += fmt.Sprintf(" time_zone='%s'", config.TimeZone)
	}
	if config.IsoLevel != "" {
		additionalParams += fmt.Sprintf(" iso_level='%s'", config.IsoLevel)
	}
	if config.LockTimeout != "" {
		additionalParams += fmt.Sprintf(" lock_timeout='%s'", config.LockTimeout)
	}
	if config.AutoCommit != "" {
		additionalParams += fmt.Sprintf(" auto_commit='%s'", config.AutoCommit)
	}
	if config.StrictCommit != "" {
		additionalParams += fmt.Sprintf(" strict_commit='%s'", config.StrictCommit)
	}
	if config.Result != "" {
		additionalParams += fmt.Sprintf(" result='%s'", config.Result)
	}
	if config.ReturnSchema != "" {
		additionalParams += fmt.Sprintf(" return_schema='%s'", config.ReturnSchema)
	}
	if config.ReturnCursorID != "" {
		additionalParams += fmt.Sprintf(" return_cursor_id='%s'", config.ReturnCursorID)
	}
	if config.LobRet != "" {
		additionalParams += fmt.Sprintf(" lob_ret='%s'", config.LobRet)
	}
	if config.ReturnRowid != "" {
		additionalParams += fmt.Sprintf(" return_rowid='%s'", config.ReturnRowid)
	}
	if config.Version != "" {
		additionalParams += fmt.Sprintf(" version='%s'", config.Version)
	} else {
		additionalParams += " version='201'"
	}

	finalString := fmt.Sprintf(baseString, config.Database, config.User, config.Password)
	if additionalParams != "" {
		finalString += additionalParams
	}
	//finalString += " version='201'"
	return finalString
}

// reverseBytes 反转 byte slice 的顺序
func reverseBytes(b []byte) []byte {
	reversed := make([]byte, len(b))
	for i := range b {
		reversed[i] = b[len(b)-1-i]
	}
	return reversed
}

func gt(name string) {
	//rintf("\n=============%s================\n", name)
}

func switchSQLType(sql string) int {
	// 去掉首尾的空格、换行符和回车符
	sql = strings.TrimSpace(sql)
	if len(sql) < 6 {
		return SQL_OTHER
	}

	// 取前6个字符并转为大写
	kstr := strings.ToUpper(sql[:6])

	// 根据SQL语句前缀判断类型
	switch {
	case strings.HasPrefix(kstr, "SELECT"):
		// if strings.Contains(sql, ";") && len(sql[strings.Index(sql, ";"):]) > 5 {
		// 	return SQL_OTHER // 多结果集
		// }
		return SQL_SELECT
	case strings.HasPrefix(kstr, "INSERT"):
		return SQL_INSERT
	case strings.HasPrefix(kstr, "UPDATE"):
		return SQL_UPDATE
	case strings.HasPrefix(kstr, "DELETE"):
		return SQL_DELETE
	case strings.HasPrefix(kstr, "CREATE"):
		return SQL_CREATE
	case strings.HasPrefix(kstr, "ALTER "):
		return SQL_ALTER
	case strings.HasPrefix(kstr, "EXEC "):
		return SQL_PROCEDURE
	case strings.HasPrefix(kstr, "EXECUT"):
		return SQL_PROCEDURE
	case strings.HasPrefix(kstr, "STC"):
		return SQL_SELECT
	default:
		return SQL_OTHER
	}
}

// CheckEndian 判断机器的字节序
func CheckEndian() bool {
	var i int32 = 0x01020304
	b := [4]byte{}

	// 将整数值写入字节切片
	binary.BigEndian.PutUint32(b[:], uint32(i))

	// 判断字节序
	if b[0] == 0x01 {
		return false // 大端字节序
	} else {
		return true // 小端字节序
	}
}
