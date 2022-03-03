/****************************************************************************
 *
 * Copyright (C) Agile Data, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by MFTLABS <code@mftlabs.io>
 *
 ****************************************************************************/
package elastic

import (
	amf "github.com/mft-labs/amfcore"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type ElasticClient struct {
	client *http.Client
	url    string
}

func (ec *ElasticClient) Init(url string) {
	ec.url = url
	ec.client = &http.Client{
		Timeout: time.Duration(60) * time.Second,
	}
}

func (ec *ElasticClient) Send(msg map[string]string, status string) error {
	msg[amf.STATUS] = status
	t, err := amf.ConvertEpochtoTimestamp(msg[amf.TIME_CREATED])
	if err != nil {
		return errors.New("could not format time [" + err.Error() + "]")
	}
	msg["create_time"] = t.Format(time.RFC3339)
	data, err := json.Marshal(msg)
	if err != nil {
		return errors.New("error creating elastic json data [" + err.Error() + "]")
	}
	req, err := http.NewRequest(http.MethodPost, ec.url, bytes.NewBuffer(data))
	if err != nil {
		return errors.New("error creating elastic HTTP request [" + err.Error() + "]")
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := ec.client.Do(req)
	if err != nil {
		return errors.New("error sending elastic HTTP request [" + err.Error() + "]")
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.New("error receiving elastic HTTP response [" + err.Error() + "]")
	}
	if resp.StatusCode > http.StatusCreated {
		return errors.New(fmt.Sprintf("error from elastic, code [%d], reason [%s]", resp.StatusCode, resp.Status))
	}
	return nil
}
