package writeaheadlog

import (
	"errors"
	"os"
)

type WriteAheadLog struct {
	filePath string // file path at which the log is persisted
	fd       *os.File
}

func NewWriteAheadLog(filePath string) (*WriteAheadLog, bool, error) {
	w := &WriteAheadLog{filePath: filePath}

	// both read write permissions are required as we need to read from the file
	// existing data on restart
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, false, err
	}

	fileExists := false
	_, err = os.Stat(filePath)
	if os.IsExist(err) {
		fileExists = true
	} else if os.IsNotExist(err) {
		fileExists = false
	} else {
		return nil, false, err
	}

	w.fd = fd
	return w, fileExists, nil
}

func (w *WriteAheadLog) AppendLog(data []byte) error {

	n, err := w.fd.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("Mismatch in data written")
	}
	return nil
}

func (w *WriteAheadLog) ReadFromLog() ([]byte, error) {

	data, err := os.ReadFile(w.filePath)
	if err != nil {
		return nil, err
	}
	return data, nil
}
