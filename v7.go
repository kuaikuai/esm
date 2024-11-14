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

	log "github.com/cihub/seelog"
)

type ESAPIV7 struct {
	ESAPIV6
}

func NewEsApiV7(host string, auth *Auth, httpProxy string, compress bool, version *ClusterVersion) ESAPI {
	apiV7 := &ESAPIV7{}

	apiV7.Host = host
	apiV7.Auth = auth
	apiV7.HttpProxy = httpProxy
	apiV7.Compress = compress
	apiV7.Version = version

	return apiV7
}

func (s *ESAPIV7) UpdateIndexMapping(indexName string, settings map[string]interface{}) error {

	log.Debug("start update mapping: ", indexName, settings)

	delete(settings, "dynamic_templates")

	//for name, mapping := range settings {

	log.Debug("start update mapping: ", indexName, ", ", settings)

	url := fmt.Sprintf("%s/%s/_mapping", s.Host, indexName)

	body := bytes.Buffer{}
	enc := json.NewEncoder(&body)
	enc.Encode(settings)
	res, err := Request(s.Compress, "POST", url, s.Auth, &body, s.HttpProxy)
	if err != nil {
		log.Error(url)
		log.Error(body.String())
		log.Error(err, res)
		panic(err)
	}
	//}
	return nil
}
