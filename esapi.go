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

type MainVersion uint8

const (
	VersionUnknown MainVersion = iota //
	ES5
	ES6
	ES7
	ES8

	// OpenSearch
	OS1
	OS2
)

type ESAPI interface {
	ClusterHealth() *ClusterHealth
	ClusterVersion() *ClusterVersion
	GetMainVersion() MainVersion
	GetDefaultSortId() string // 不同版本的 es, 默认的sort Id 不一样: es5: _uid, es6+: _id
	Bulk(data *bytes.Buffer) error
	GetIndexSettings(indexNames string) (*Indexes, error)
	DeleteIndex(name string) error
	CreateIndex(name string, settings map[string]interface{}) error
	GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error)
	CanUpdateIndex(indexName string) bool
	UpdateIndexSettings(indexName string, settings map[string]interface{}) error
	UpdateIndexMapping(indexName string, mappings map[string]interface{}) error
	NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, stamp string, sort string,
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

func GetMainVersion(cVersion *ClusterVersion) MainVersion {
	mainVersion := VersionUnknown
	versionNumber := cVersion.Version.Number
	if len(cVersion.Version.Distribution) == 0 {
		if strings.HasPrefix(versionNumber, "7.") {
			mainVersion = ES7
		} else if strings.HasPrefix(versionNumber, "6.") {
			mainVersion = ES6
		} else if strings.HasPrefix(versionNumber, "5.") {
			mainVersion = ES5
		}
	} else if cVersion.Version.Distribution == "opensearch" {
		//OpenSearch
		if strings.HasPrefix(versionNumber, "2.") {
			mainVersion = OS2
		} else if strings.HasPrefix(versionNumber, "1.") {
			mainVersion = OS1
		}
	}

	return mainVersion
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
	mainVersion := GetMainVersion(esVersion)
	var api ESAPI = nil
	switch mainVersion {
	case OS2:
		log.Debug("is open search v2,", esVersion.Version.Number)
		api = NewOpenSearchV2(host, auth, proxy, compress, esVersion)
	case OS1:
		log.Debug("is open search v1,", esVersion.Version.Number)
		api = NewOpenSearchV1(host, auth, proxy, compress, esVersion)
	case ES7:
		log.Debug("es is V7,", esVersion.Version.Number)
		api = NewEsApiV7(host, auth, proxy, compress, esVersion)
	case ES6:
		log.Debug("es is V6,", esVersion.Version.Number)
		api = NewEsApiV6(host, auth, proxy, compress, esVersion)
	case ES5:
		log.Debug("es is V5,", esVersion.Version.Number)
		api = NewEsApiV5(host, auth, proxy, compress, esVersion)
	default:
		api = nil
	}
	return api
}
