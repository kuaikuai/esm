/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	log "github.com/cihub/seelog"
)

type ESAPI interface {
	ClusterHealth() *ClusterHealth
	ClusterVersion() *ClusterVersion
	GetDefaultSortId() string // 不同版本的 es, 默认的sort Id 不一样: es5: _uid, es6+: _id
	Bulk(data *bytes.Buffer) error
	GetIndexSettings(indexNames string) (*Indexes, error)
	DeleteIndex(name string) error
	CreateIndex(name string, settings map[string]interface{}) error
	GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error)
	UpdateIndexSettings(indexName string, settings map[string]interface{}) error
	UpdateIndexMapping(indexName string, mappings map[string]interface{}) error
	NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, sort string,
		slicedId int, maxSlicedCount int, fields string) (ScrollAPI, error)
	NextScroll(scrollTime string, scrollId string) (ScrollAPI, error)
	DeleteScroll(scrollId string) error
	Refresh(name string) (err error)
}

func GetClusterVersion(host string, auth *Auth, proxy string) (*ClusterVersion, []error) {

	url := fmt.Sprintf("%s", host)
	resp, body, errs := Get(url, auth, proxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return nil, errs
	}

	log.Debugf("version info: %s", body)

	version := &ClusterVersion{}
	err := json.Unmarshal([]byte(body), version)

	if err != nil {
		log.Error(body, errs)
		return nil, errs
	}
	return version, nil
}

func ParseEsApi(isSource bool, host string, authStr string, proxy string, compress bool) ESAPI {
	var auth *Auth = nil
	if len(authStr) > 0 && strings.Contains(authStr, ":") {
		authArray := strings.Split(authStr, ":")
		auth = &Auth{User: authArray[0], Pass: authArray[1]}
	}

	esVersion, errs := GetClusterVersion(host, auth, proxy)
	if errs != nil {
		log.Error(errs)
		return nil
	}

	esInfo := "dest"
	if isSource {
		esInfo = "source"
	}

	log.Infof("%s es version: %s", esInfo, esVersion.Version.Number)
	if strings.HasPrefix(esVersion.Version.Number, "7.") {
		log.Debug("es is V7,", esVersion.Version.Number)
		api := new(ESAPIV7)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "6.") {
		log.Debug("es is V6,", esVersion.Version.Number)
		api := new(ESAPIV6)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else if strings.HasPrefix(esVersion.Version.Number, "5.") {
		log.Debug("es is V5,", esVersion.Version.Number)
		api := new(ESAPIV5)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
		//migrator.SourceESAPI = api
	} else {
		log.Debug("es is not V5,", esVersion.Version.Number)
		api := new(ESAPIV0)
		api.Host = host
		api.Compress = compress
		api.Auth = auth
		api.HttpProxy = proxy
		api.Version = esVersion
		return api
	}
}
