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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"

	log "github.com/cihub/seelog"
	//"infini.sh/framework/core/util"
)

type ESAPIV6 struct {
	ESAPIV5
}

func NewEsApiV6(host string, auth *Auth, httpProxy string, compress bool, version *ClusterVersion) ESAPI {
	apiV6 := &ESAPIV6{}

	apiV6.Host = host
	apiV6.Auth = auth
	apiV6.HttpProxy = httpProxy
	apiV6.Compress = compress
	apiV6.Version = version

	return apiV6
}

func (s *ESAPIV6) GetDefaultSortId() string {
	return "_id"
}

func (s *ESAPIV6) GetIndexMappings(copyAllIndexes bool, indexNames string) (string, int, *Indexes, error) {
	url := fmt.Sprintf("%s/%s/_mapping", s.Host, indexNames)
	resp, body, errs := Get(url, s.Auth, s.HttpProxy)

	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if errs != nil {
		log.Error(errs)
		return "", 0, nil, errs[0]
	}

	if resp.StatusCode != 200 {
		return "", 0, nil, errors.New(body)
	}

	idxs := Indexes{}
	er := DecodeJson(body, &idxs)

	if er != nil {
		log.Error(body)
		return "", 0, nil, er
	}

	// if _all indexes limit the list of indexes to only these that we kept
	// after looking at mappings
	if indexNames == "_all" {

		var newIndexes []string
		for name := range idxs {
			newIndexes = append(newIndexes, name)
		}
		indexNames = strings.Join(newIndexes, ",")

	} else if strings.Contains(indexNames, "*") || strings.Contains(indexNames, "?") {

		r, _ := regexp.Compile(indexNames)

		//check index patterns
		var newIndexes []string
		for name := range idxs {
			matched := r.MatchString(name)
			if matched {
				newIndexes = append(newIndexes, name)
			}
		}
		indexNames = strings.Join(newIndexes, ",")

	}

	i := 0
	// wrap in mappings if moving from super old es
	for name, idx := range idxs {
		i++
		if _, ok := idx.(map[string]interface{})["mappings"]; !ok {
			(idxs)[name] = map[string]interface{}{
				"mappings": idx,
			}
		}
	}

	return indexNames, i, &idxs, nil
}

func (s *ESAPIV6) UpdateIndexMapping(indexName string, settings map[string]interface{}) error {

	log.Debug("start update mapping: ", indexName, settings)

	delete(settings, "dynamic_templates")

	for name, _ := range settings {

		log.Debug("start update mapping: ", indexName, ", ", settings)

		url := fmt.Sprintf("%s/%s/%s/_mapping", s.Host, indexName, name)

		body := bytes.Buffer{}
		enc := json.NewEncoder(&body)
		enc.Encode(settings)
		res, err := Request(s.Compress, "POST", url, s.Auth, &body, s.HttpProxy)
		if err != nil {
			log.Error(url)
			log.Error(settings)
			log.Error(err, res)
			panic(err)
		}
	}
	return nil
}
