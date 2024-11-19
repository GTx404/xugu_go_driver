package xugu

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
)

type xuguConn struct {
	dsnConfig
	conn        net.Conn
	mu          sync.Mutex
	useSSL      bool // 是否使用加密
	havePrepare int  //default 0

	prepareNo   int
	prepareName string
	//presPrepareCata *Result

	errStr []byte

	sendBuff bytes.Buffer
	readBuff buffer
}

type dsnConfig struct {
	IP             string
	Port           string
	Database       string
	User           string
	Password       string
	Encryptor      string //加密库的解密口令
	CharSet        string //客户端使用的字符集名
	TimeZone       string
	IsoLevel       string //事务隔离级别
	LockTimeout    string //加锁超时
	AutoCommit     string
	StrictCommit   string
	Result         string
	ReturnSchema   string
	ReturnCursorID string
	LobRet         string
	ReturnRowid    string
	Version        string
}

func (xgConn *xuguConn) Begin() (driver.Tx, error) {

	_, err := xgConn.exec("Begin;", nil)
	if err != nil {
		return nil, err
	}
	return &xuguTx{tconn: xgConn}, nil

}

func (xgConn *xuguConn) Close() error {

	xgConn.mu.Lock()
	defer xgConn.mu.Unlock()
	err := xgConn.conn.Close()
	if err != nil {
		xgConn.mu.Unlock()
		return err
	}

	return nil
}

func (xgConn *xuguConn) Query(sql string,
	args []driver.Value) (driver.Rows, error) {

	xgConn.mu.Lock()
	defer xgConn.mu.Unlock()

	// 检测sql语句不是查询则报错
	if switchSQLType(sql) != SQL_SELECT {
		return nil, errors.New("The executed SQL statement is not a SELECT")
	}

	// 有传进来的参数
	if len(args) != 0 {
		values := []xuguValue{}
		//判断类型
		for _, param := range args {
			err := assertParamType(param, &values)
			if err != nil {
				return nil, err
			}
		}
		//send msg
		if err := sockSendPutStatement(xgConn, []byte(sql), &values, len(args)); err != nil {
			return nil, err
		}

		if err := sockSendExecute(xgConn); err != nil {
			return nil, err
		}

	} else {

		//send msg
		if err := sockSendPutStatement(xgConn, []byte(sql), nil, len(args)); err != nil {
			return nil, err
		}

		if err := sockSendExecute(xgConn); err != nil {
			return nil, err
		}

	}
	//recv msg
	aR, err := xuguSockRecvMsg(xgConn)
	if err != nil {
		return nil, err
	}

	switch aR.rt {
	case selectResult:

		rows := &xuguRows{
			rows_conn: xgConn,
			aR:        aR,
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

func (xgConn *xuguConn) Ping(ctx context.Context) error {

	//send
	xgConn.mu.Lock()
	defer xgConn.mu.Unlock()
	sockSendPutStatement(xgConn, []byte("select count(*) from dual;"), nil, 0)
	sockSendExecute(xgConn)

	_, err := xuguSockRecvMsg(xgConn)
	if err != nil {
		return err
	}
	xgConn.readBuff.reset()

	return nil
}

func (xgConn *xuguConn) Prepare(sql string) (driver.Stmt, error) {

	xgConn.mu.Lock()
	defer xgConn.mu.Unlock()

	//判断sql类型
	switch switchSQLType(sql) {
	case SQL_PROCEDURE:
		return nil, errors.New("Prepare does not support stored procedures")
	case SQL_UNKNOWN:
		return nil, errors.New("Unknown SQL statement type")
	case SQL_CREATE:
		return nil, errors.New("Prepare does not support DDL.")
	}
	//发送创建prepare
	prepareName := "GTONG"

	err := xuguPrepare(xgConn, sql, prepareName)
	if err != nil {
		return nil, err
	}
	count := assertParamCount(sql)
	stmt := &xuguStmt{
		stmt_conn:  xgConn,
		prepared:   true,
		prename:    make([]byte, 128),
		curopend:   false,
		curname:    make([]byte, 128),
		paramCount: count,
		mysql:      sql,
	}
	stmt.prename = []byte(fmt.Sprintf("? %s", xgConn.prepareName))

	return stmt, nil
}

func xuguPrepare(pConn *xuguConn, cmd_sql string, prepareName string) error {

	prepareName = fmt.Sprintf("%s%d", prepareName, pConn.prepareNo)
	sqlRet := fmt.Sprintf("PREPARE %s AS %s", prepareName, cmd_sql)
	pConn.prepareName = prepareName
	pConn.prepareNo++
	//send msg
	sockSendPutStatement(pConn, []byte(sqlRet), nil, 0)
	sockSendExecute(pConn)
	//recv msg
	aR, err := xuguSockRecvMsg(pConn)
	switch aR.rt {

	case errInfo:

		return errors.New(string(aR.e.ErrStr))
	case warnInfo:

		return errors.New(string(aR.w.WarnStr))
	default:
	}

	if err != nil {
		return err
	}

	return nil
}

func xuguUnPrepare(pConn *xuguConn, prepareName string) error {

	sqlRet := fmt.Sprintf("DEALLOCATE %s ", prepareName)

	//send msg
	sockSendPutStatement(pConn, []byte(sqlRet), nil, 0)
	sockSendExecute(pConn)
	//recv msg
	aR, err := xuguSockRecvMsg(pConn)
	switch aR.rt {

	case 'K':
		return nil
	case errInfo:

		return errors.New(string(aR.e.ErrStr))
	case warnInfo:

		return errors.New(string(aR.w.WarnStr))
	default:
	}

	if err != nil {
		return err
	}

	return nil
}

func (xgConn *xuguConn) Exec(sql string,
	args []driver.Value) (driver.Result, error) {

	xgConn.mu.Lock()
	defer xgConn.mu.Unlock()

	// 检测sql语句是查询则报错
	if switchSQLType(sql) == SQL_SELECT {
		return nil, errors.New("The executed SQL statement is  a SELECT")
	}

	// 有传进来的参数
	if len(args) != 0 {
		values := []xuguValue{}
		//判断类型
		for _, param := range args {
			err := assertParamType(param, &values)
			if err != nil {
				return nil, err
			}
		}
		//send msg
		if err := sockSendPutStatement(xgConn, []byte(sql), &values, len(args)); err != nil {
			return nil, err
		}

		if err := sockSendExecute(xgConn); err != nil {
			return nil, err
		}

	} else {

		//send msg
		if err := sockSendPutStatement(xgConn, []byte(sql), nil, len(args)); err != nil {
			return nil, err
		}

		if err := sockSendExecute(xgConn); err != nil {
			return nil, err
		}

	}
	//recv msg
	aR, err := xuguSockRecvMsg(xgConn)
	if err != nil {
		return nil, err
	}

	switch aR.rt {
	case selectResult:

		return nil, errors.New("exec is Query error")
	case errInfo:

		return nil, errors.New(string(aR.e.ErrStr))
	case warnInfo:

		return nil, errors.New(string(aR.w.WarnStr))
	case updateResult:
		return &xuguResult{xgConn: xgConn, affectedRows: int64(aR.u.UpdateNum), insertId: int64(0)}, nil
	case insertResult:

		return &xuguResult{xgConn: xgConn, affectedRows: int64(aR.i.EffectNum), insertId: int64(binary.LittleEndian.Uint64(aR.i.RowidData))}, nil
	default:
		return &xuguResult{
			xgConn:       xgConn,
			affectedRows: int64(0),
			insertId:     int64(0),
		}, nil
	}

}

func (xgConn *xuguConn) exec(sql string, args []driver.Value) (driver.Result, error) {
	// 有传进来的参数
	if len(args) != 0 {
		values := []xuguValue{}
		//判断类型
		for _, param := range args {
			err := assertParamType(param, &values)
			if err != nil {
				return nil, err
			}
		}
		//send msg
		if err := sockSendPutStatement(xgConn, []byte(sql), &values, len(args)); err != nil {
			return nil, err
		}

		if err := sockSendExecute(xgConn); err != nil {
			return nil, err
		}

	} else {

		//send msg
		if err := sockSendPutStatement(xgConn, []byte(sql), nil, len(args)); err != nil {
			return nil, err
		}

		if err := sockSendExecute(xgConn); err != nil {
			return nil, err
		}

	}

	//recv msg
	aR, err := xuguSockRecvMsg(xgConn)
	if err != nil {
		return nil, err
	}
	switch aR.rt {
	case selectResult:

		return nil, errors.New("exec is Query error")
	case updateResult:
		return &xuguResult{xgConn: xgConn, affectedRows: int64(aR.u.UpdateNum), insertId: int64(0)}, nil
	case insertResult:

		return &xuguResult{xgConn: xgConn, affectedRows: int64(aR.i.EffectNum), insertId: int64(binary.LittleEndian.Uint64(aR.i.RowidData))}, nil
	case errInfo:

		return nil, errors.New(string(aR.e.ErrStr))
	case warnInfo:

		return nil, errors.New(string(aR.w.WarnStr))
	default:
		return &xuguResult{
			xgConn:       xgConn,
			affectedRows: int64(0),
			insertId:     int64(0),
		}, nil
	}

}
