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

type ESAPIV8 struct {
	ESAPIV7
}

func (s *ESAPIV8) NextScroll(scrollTime string, scrollId string) (ScrollAPI, error) {
	//id := bytes.NewBufferString(scrollId)
	param := make(map[string]string)
	param["scroll"] = scrollTime
	param["scroll_id"] = scrollId
	data, _ := json.Marshal(param)
	reqData := bytes.NewBuffer(data)
	url := fmt.Sprintf("%s/_search/scroll", s.Host)
	body, err := Request(s.Compress, "GET", url, s.Auth, reqData, s.HttpProxy)

	if err != nil {
		//log.Error(errs)
		return nil, err
	}
	// decode elasticsearch scroll response
	scroll := &ScrollV7{}
	err = DecodeJson(body, &scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}
