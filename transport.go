package clickhouse

import (
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	httpTransportBodyType = "text/plain"
)

type Transport interface {
	Exec(conn *Conn, q Query, readOnly bool) (io.ReadCloser, error)
}

type HttpTransport struct {
	Timeout    time.Duration
	BufferPool *BufferPool
}

func (t HttpTransport) Exec(conn *Conn, q Query, readOnly bool) (r io.ReadCloser, err error) {
	var resp *http.Response
	query := prepareHttp(q.Stmt, q.args)
	client := &http.Client{Timeout: t.Timeout}
	if readOnly {
		if len(query) > 0 {
			query = "?query=" + query
		}
		resp, err = client.Get(conn.Host + query)
	} else {
		var req *http.Request
		req, err = t.prepareExecPostRequest(conn.Host, q)
		if err != nil {
			return nil, err
		}

		resp, err = client.Do(req)
	}
	if err != nil {
		return nil, err
	} else if err = t.handleErrStatus(resp); err != nil {
		return nil, err
	}

	return resp.Body, err
}

func (t HttpTransport) handleErrStatus(res *http.Response) error {
	if res.StatusCode != 200 {
		buf := t.BufferPool.Get()
		defer t.BufferPool.Put(buf)

		_, err := buf.ReadFrom(res.Body)
		if err != nil {
			return err
		}

		text := buf.String()

		if text[0] == '<' {
			re := regexp.MustCompile("<title>([^<]+)</title>")
			list := re.FindAllString(text, -1)

			return errors.New(list[0])
		} else {
			return errors.New(text)
		}
	}

	return nil
}

func (t HttpTransport) prepareExecPostRequest(host string, q Query) (*http.Request, error) {
	query := prepareHttp(q.Stmt, q.args)
	var req *http.Request
	var err error

	switch true {
	case len(q.externals) > 0:
		if len(query) > 0 {
			query = "?query=" + url.QueryEscape(query)
		}

		body := t.BufferPool.Get()
		defer t.BufferPool.Put(body)

		writer := multipart.NewWriter(body)

		for _, ext := range q.externals {
			query = query + "&" + ext.Name + "_structure=" + url.QueryEscape(ext.Structure)
			part, err := writer.CreateFormFile(ext.Name, ext.Name)
			if err != nil {
				return nil, err
			}
			_, err = part.Write(ext.Data)
			if err != nil {
				return nil, err
			}
		}

		err = writer.Close()
		if err != nil {
			return nil, err
		}

		req, err = http.NewRequest("POST", host+query, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
	case q.body != nil:
		req, err = http.NewRequest("POST", host+"?query="+url.QueryEscape(query), q.body)
		if err != nil {
			return nil, err
		}
	default:
		req, err = http.NewRequest("POST", host, strings.NewReader(query))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", httpTransportBodyType)
	}

	return req, err
}

func prepareHttp(stmt string, args []interface{}) string {
	var res []byte
	buf := []byte(stmt)
	res = make([]byte, 0)
	k := 0
	for _, ch := range buf {
		if ch == '?' {
			res = append(res, []byte(marshal(args[k]))...)
			k++
		} else {
			res = append(res, ch)
		}
	}

	return string(res)
}
