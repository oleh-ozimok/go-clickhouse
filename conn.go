package clickhouse

import (
	"fmt"
	"io/ioutil"
	"strings"
)

const (
	successTestResponse = "Ok."
)

type Conn struct {
	Host      string
	transport Transport
}

func (c *Conn) Ping() (err error) {
	body, err := c.transport.Exec(c, Query{Stmt: ""}, true)
	if err != nil {
		return err
	}
	defer body.Close()
	bodyBytes, err := ioutil.ReadAll(body)
	response := string(bodyBytes)
	if err == nil {
		if !strings.Contains(response, successTestResponse) {
			err = fmt.Errorf("Clickhouse host response was '%s', expected '%s'.", response, successTestResponse)
		}
	}

	return err
}
