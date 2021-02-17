package clickhouse

import (
	"bufio"
	"errors"
	"io"
	"regexp"
	"strings"
)

type External struct {
	Name      string
	Structure string
	Data      []byte
}

type Func struct {
	Name string
	Args interface{}
}

type Query struct {
	Stmt      string
	args      []interface{}
	externals []External
	body      io.Reader
}

func (q *Query) AddExternal(name string, structure string, data []byte) {
	q.externals = append(q.externals, External{Name: name, Structure: structure, Data: data})
}

func (q Query) Iter(conn *Conn) *Iter {
	if conn == nil {
		return &Iter{err: errors.New("Connection pointer is nil")}
	}

	re := regexp.MustCompile("(FORMAT [A-Za-z0-9]+)? *;? *$")
	q.Stmt = re.ReplaceAllString(q.Stmt, " FORMAT TabSeparatedWithNames")

	resp, err := conn.transport.Exec(conn, q, false)
	if err != nil {
		return &Iter{err: err}
	}

	iter := &Iter{
		reader:     bufio.NewReader(resp),
		readCloser: resp,
		err:        nil,
	}

	iter.columns = iter.fetchNext()

	return iter
}

func (q Query) Exec(conn *Conn) (err error) {
	if conn == nil {
		return errors.New("Connection pointer is nil")
	}
	_, err = conn.transport.Exec(conn, q, false)

	return err
}

type Iter struct {
	reader     *bufio.Reader
	readCloser io.ReadCloser
	err        error
	text       string
	columns    []string
}

func (r *Iter) Error() error {
	return r.err
}

func (r *Iter) Columns() []string {
	return r.columns
}

func (r *Iter) Scan(vars ...interface{}) bool {
	row := r.fetchNext()
	if r.err != nil {
		return false
	}

	if len(row) == 0 {
		return false
	}
	if len(row) < len(vars) {
		return false
	}
	for i, v := range vars {
		err := unmarshal(v, row[i])
		if err != nil {
			r.err = err
			return false
		}
	}
	return true
}

func (r *Iter) fetchNext() []string {
	var bytes []byte
	bytes, r.err = r.reader.ReadBytes('\n')

	l := len(bytes)
	if l > 0 {
		bytes = bytes[0 : len(bytes)-1]
	}

	if r.err == io.EOF {
		r.err = r.readCloser.Close()
	}

	return strings.Split(string(bytes), "\t")
}
