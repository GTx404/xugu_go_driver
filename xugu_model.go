package xugu

type SelectResult struct {
	Field_Num uint32
	Fields    []FieldDescri
	Values    [][]FieldValue //[字段][字段所有值]
	rowIdx    int
	fad       *FormArgDescri
	next      *SelectResult
}
type FormArgDescri struct {
	ArgNum uint32
	Args   []ArgDescri
}

type FieldValue struct {
	Col_len  uint32
	Col_Data []byte
}

type InsertResult struct {
	EffectNum uint32
	RowidLen  uint32
	RowidData []byte
}

type UpdateResult struct {
	UpdateNum uint32
}

type DeleteResult struct {
	DeleteNum uint32
}

type ProcRet struct {
	RetDType   uint32
	RetDataLen uint32
	RetData    []byte
}

type OutParamRet struct {
	OutParamNo    uint32
	OutParamDType uint32
	OutParamLen   uint32
	OutParamData  []byte
}

type ErrInfo struct {
	ErrStrLen uint32
	ErrStr    []byte
}

type WarnInfo struct {
	WarnStrLen uint32
	WarnStr    []byte
}

type Message struct {
	MsgStrLen uint32
	MsgStr    []byte
}

type ArgDescri struct {
	ArgNameLen    uint32
	ArgName       []byte
	ArgNo         uint32
	ArgDType      uint32
	ArgPreciScale uint32
}

type allResult struct {
	rt        msgType
	s         *SelectResult
	i         *InsertResult
	u         *UpdateResult
	d         *DeleteResult
	p         *ProcRet
	o         *OutParamRet
	e         *ErrInfo
	w         *WarnInfo
	m         *Message
	f         *FormArgDescri
	next      *allResult
	effectNum uint32 //sql总影响行数，包括i,u,d
}
