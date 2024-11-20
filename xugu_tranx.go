package xugu

import "errors"

type xuguTx struct {
	tconn *xuguConn
}

func (tx *xuguTx) Commit() error {
	tx.tconn.mu.Lock()
	defer tx.tconn.mu.Unlock()

	if tx.tconn == nil {
		return errors.New("invalid connection")
	}
	_, err := tx.tconn.exec("commit;", nil)
	if err != nil {
		return err
	}
	return nil
}

func (tx *xuguTx) Rollback() error {
	tx.tconn.mu.Lock()
	defer tx.tconn.mu.Unlock()

	if tx.tconn == nil {
		return errors.New("invalid connection")
	}
	_, err := tx.tconn.exec("rollback;", nil)
	if err != nil {
		return err
	}

	return err
}
