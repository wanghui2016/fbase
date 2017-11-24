package server

import (
	"model/pkg/kvrpcpb"
	"data-server/client"
)

// TODO timestamp to ensure the order of put
func (p *Proxy) RawPut(dbName, tableName string, key, value []byte) error {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return ErrNotExistTable
	}
	req := &kvrpcpb.KvRawPutRequest{
		Key:   key,
		Value: value,
	}
	proxy := KvProxy{
		cli: p.nodeCli,
		msCli: p.msCli,
		clock: p.clock,
		table: t,
		findRoute: t.FindRoute,
		writeTimeout: client.WriteTimeout,
		readTimeout: client.ReadTimeoutShort,
	}
	_, err := proxy.rawPut(req)
	if err != nil {
		return err
	}
	return nil
}

// get in range split, first get from dst range, if no value, we need get from src range again
func (p *Proxy) RawGet(dbName, tableName string, key []byte) ([]byte, error) {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return nil, ErrNotExistTable
	}
	req := &kvrpcpb.KvRawGetRequest{
		Key: key,
	}
	proxy := KvProxy{
		cli: p.nodeCli,
		msCli: p.msCli,
		clock: p.clock,
		table: t,
		findRoute: t.FindRoute,
		writeTimeout: client.WriteTimeout,
		readTimeout: client.ReadTimeoutShort,
	}
	resp, err := proxy.rawGet(req)
	if err != nil {
		return nil, err
	}

	//code := resp.GetKvRawGetResp().GetCode()
	value := resp.GetValue()
	return value, nil

}

// delete in range split, we need delete key in src range and dst range
func (p *Proxy) RawDelete(dbName, tableName string, key []byte) error {
	t := p.router.FindTable(dbName, tableName)
	if t == nil {
		return ErrNotExistTable
	}
	req := &kvrpcpb.KvRawDeleteRequest{
		Key: key,
	}
	proxy := KvProxy{
		cli: p.nodeCli,
		msCli: p.msCli,
		clock: p.clock,
		table: t,
		findRoute: t.FindRoute,
		writeTimeout: client.WriteTimeout,
		readTimeout: client.ReadTimeoutShort,
	}
	_, err := proxy.rawDelete(req)
	if err != nil {
		return err
	}
	return nil
}

