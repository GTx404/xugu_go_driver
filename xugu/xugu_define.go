package xugu

var GlobalIsBig bool

// 协议消息类型
type msgType byte

const (
	selectResult msgType = iota + 0x01
	insertResult
	updateResult
	deleteResult
	procRet
	outParamRet
	errInfo
	warnInfo
	message
	formArgDescri
)

// SQL类型常量
const (
	SQL_UNKNOWN = iota
	SQL_SELECT
	SQL_INSERT
	SQL_UPDATE
	SQL_DELETE
	SQL_CREATE
	SQL_ALTER
	SQL_PROCEDURE
	SQL_OTHER
)
