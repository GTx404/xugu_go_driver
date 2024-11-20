package xugu

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net"
	"time"
)

type connector struct {
	dsn string
}

// Driver implements driver.Connector interface.
// Driver returns &XuguDriver{}
func (conntor *connector) Driver() driver.Driver {

	return &XuguDriver{}
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
/*
dsn解析
创建连接
设置为 tcp 长连接（
创建连接缓冲区
设置连接超时配置
接收来自服务端的握手请求
*/
func (conntor *connector) Connect(ctx context.Context) (driver.Conn, error) {

	GlobalIsBig = CheckEndian()

	dsnConfig := parseDSN(conntor.dsn)

	xgConn := &xuguConn{conn: nil}
	xgConn.dsnConfig = dsnConfig

	nd := net.Dialer{Timeout: 10 * time.Millisecond}
	netConn, err := nd.DialContext(ctx, "tcp", fmt.Sprintf("%s:%s", xgConn.IP, xgConn.Port))
	if err != nil {
		return nil, err
	}

	// 启用 TCP 保活
	if tc, ok := netConn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			//c.cfg.Logger.Print(err) // 如果设置保活失败，记录错误但不终止
			return nil, err
		}
	}

	xgConn.conn = netConn
	xgConn.mu.Lock()
	xgConn.readBuff = newBuffer(xgConn.conn)
	err = xgSockOpenConn(ctx, xgConn)
	if err != nil {
		return nil, err
	}
	xgConn.mu.Unlock()
	return xgConn, nil
}
