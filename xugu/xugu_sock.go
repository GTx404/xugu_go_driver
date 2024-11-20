package xugu

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

func xgSockOpenConn(ctx context.Context, pConn *xuguConn) error {
	//发送
	//rintf("login   database = '%s' user = '%s'  password = '%s' version='201' ", pConn.Database, pConn.User, pConn.Password)
	//	message := "login   database = 'SYSTEM' user = 'SYSDBA'  password = 'SYSDBA' version='201' "
	dsnMessage := generateLoginString(pConn.dsnConfig)
	_, err := pConn.conn.Write([]byte(dsnMessage))
	if err != nil {
		return errors.New("向数据库发起连接失败")
	}

	buffer := make([]byte, 1)
	n, err := pConn.conn.Read(buffer)
	if err != nil {
		return errors.New(fmt.Sprintln("接收数据库连接失败: ", err.Error()))
	}

	if !bytes.Equal(buffer[:n], []byte("K")) {
		return errors.New("数据库连接失败")
	} else {
		return nil
	}

}

func sockSendPutStatement(pConn *xuguConn, sql []byte, values *[]xuguValue, paramCount int) error {
	if pConn.sendBuff.Len() > 0 {
		//将缓冲区重置为空
		pConn.sendBuff.Reset()
	}
	// ?
	pConn.sendBuff.Write([]byte("?"))
	// Comand_Len
	sqlLength := uint32(len(sql))
	var networkBytes [4]byte
	binary.BigEndian.PutUint32(networkBytes[:], sqlLength)
	pConn.sendBuff.Write(networkBytes[:])
	//  Comand_str
	pConn.sendBuff.Write(sql)
	//'0' end
	binary.BigEndian.PutUint32(networkBytes[:], 0)
	pConn.sendBuff.Write([]byte{0})
	// Param_num

	var Param_num [4]byte
	binary.BigEndian.PutUint32(Param_num[:], uint32(paramCount))
	pConn.sendBuff.Write(Param_num[:])
	if values != nil {

		//发送后续参数
		//	Param_num   { Param_name_len Param_name Param_INOUT Param_DType Param_Data_Len Param_Data }
		for _, value := range *values {
			//Param_name_len
			if value.paramName == nil {
				var Param_name_len [2]byte
				pConn.sendBuff.Write(Param_name_len[:])
				//Param_name
				// var Param_name []byte
				// pConn.sendBuff.Write(Param_name)

			} else {
				var Param_name_len [2]byte

				binary.BigEndian.PutUint16(Param_name_len[:], uint16(len(value.paramName)))
				pConn.sendBuff.Write(Param_name_len[:])

				//Param_name
				pConn.sendBuff.Write(value.paramName[:])

			}

			//Param_INOUT
			Param_INOUT := [2]byte{0x1}
			pConn.sendBuff.Write(reverseBytes(Param_INOUT[:]))

			//Param_DType
			var Param_DType [2]byte
			binary.BigEndian.PutUint16(Param_DType[:], uint16(value.types))
			pConn.sendBuff.Write(Param_DType[:])
			//Param_Data_Len 根据DType 修改长度
			Param_Data_Len := make([]byte, 4)
			binary.BigEndian.PutUint32(Param_Data_Len[:], uint32(value.valueLength))
			pConn.sendBuff.Write(Param_Data_Len[:])
			//Param_Data 根据DType 修改长度
			//Param_Data := make([]byte, value.valueLength)
			pConn.sendBuff.Write([]byte(value.value))
		}

	}

	return nil
}

func sockSendExecute(pConn *xuguConn) error {
	//	rintln("SockSendExecute msg: ", pConn.sendBuff.String())
	_, err := pConn.conn.Write(pConn.sendBuff.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func xuguSockRecvMsg(pConn *xuguConn) (*allResult, error) {
	n, _ := pConn.conn.Read(pConn.readBuff.buf)
	pConn.readBuff.length += n
	rs, err := parseMsg(&pConn.readBuff, pConn)
	if err != nil {

		return nil, err
	}
	pConn.readBuff.reset()
	return rs, nil
}
