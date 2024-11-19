package xugu

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
)

type xuguStmt struct {
	stmt_conn *xuguConn

	prepared bool

	prename []byte

	// 布尔值，用于标识游标是否启用
	curopend bool

	// 游标名称
	curname []byte

	// 执行的 SQL 语句中的参数数量
	paramCount int
	mysql      string

	//需替换的参数字段信息
	//parser *xuguParse
}

func (stmt *xuguStmt) Close() error {
	//关闭 prepare
	err := xuguUnPrepare(stmt.stmt_conn, string(stmt.prename))
	//释放资源

	return err
}

func (stmt *xuguStmt) NumInput() int {

	return assertParamCount(stmt.mysql)
	//return 0
}

func (stmt *xuguStmt) Exec(args []driver.Value) (driver.Result, error) {

	stmt.stmt_conn.mu.Lock()
	defer stmt.stmt_conn.mu.Unlock()
	//send msg
	//如果有参数
	if stmt.paramCount > 0 && len(args) > 0 {
		values := []xuguValue{}
		for _, param := range args {
			assertParamType(param, &values)
		}
		sockSendPutStatement(stmt.stmt_conn, stmt.prename, &values, stmt.paramCount)
		sockSendExecute(stmt.stmt_conn)
		//没有参数
	} else {
		sockSendPutStatement(stmt.stmt_conn, []byte(stmt.mysql), nil, 0)
		sockSendExecute(stmt.stmt_conn)
	}

	//recv msg
	aR, err := xuguSockRecvMsg(stmt.stmt_conn)
	if err != nil {
		return nil, err
	}
	switch aR.rt {
	case selectResult:

		return nil, errors.New("exec is Query error")
	case updateResult:
		return &xuguResult{xgConn: stmt.stmt_conn, affectedRows: int64(aR.u.UpdateNum), insertId: int64(0)}, nil
	case insertResult:
		return &xuguResult{xgConn: stmt.stmt_conn, affectedRows: int64(aR.i.EffectNum), insertId: int64(binary.LittleEndian.Uint64(aR.i.RowidData))}, nil
	case errInfo:

		return nil, errors.New(string(aR.e.ErrStr))
	case warnInfo:

		return nil, errors.New(string(aR.w.WarnStr))
	default:
		return &xuguResult{
			xgConn:       stmt.stmt_conn,
			affectedRows: int64(0),
			insertId:     int64(0),
		}, nil
	}

}

func (stmt *xuguStmt) Query(args []driver.Value) (driver.Rows, error) {

	stmt.stmt_conn.mu.Lock()
	defer stmt.stmt_conn.mu.Unlock()

	//send msg
	//如果有参数
	if stmt.paramCount > 0 && len(args) > 0 {
		values := []xuguValue{}
		for _, param := range args {
			assertParamType(param, &values)
		}
		sockSendPutStatement(stmt.stmt_conn, stmt.prename, &values, stmt.paramCount)
		sockSendExecute(stmt.stmt_conn)
		//没有参数
	} else {
		sockSendPutStatement(stmt.stmt_conn, []byte(stmt.mysql), nil, 0)
		sockSendExecute(stmt.stmt_conn)
	}

	//recv msg
	aR, err := xuguSockRecvMsg(stmt.stmt_conn)
	if err != nil {
		return nil, err
	}

	switch aR.rt {
	case selectResult:

		rows := &xuguRows{
			rows_conn: stmt.stmt_conn,
			results:   aR.s,
			colIdx:    0,
			prepared:  false,
		}

		return rows, nil
	case errInfo:

		return nil, errors.New(string(aR.e.ErrStr))
	case warnInfo:

		return nil, errors.New(string(aR.w.WarnStr))
	default:
	}

	return nil, errors.New("xugu Query error")
}
