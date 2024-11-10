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
	"strings"

	log "github.com/cihub/seelog"
)

type ESAPIV5 struct {
	ESAPIV0
}

func (s *ESAPIV5) GetDefaultSortId() string {
	return "_uid"
}

func (s *ESAPIV5) NewScroll(indexNames string, scrollTime string, docBufferCount int, query string, sort string,
	slicedId int, maxSlicedCount int, fields string) (scroll ScrollAPI, err error) {
	url := fmt.Sprintf("%s/%s/_search?scroll=%s&size=%d", s.Host, indexNames, scrollTime, docBufferCount)

	var jsonBody []byte
	if len(query) > 0 || maxSlicedCount > 0 || len(fields) > 0 {
		queryBody := map[string]interface{}{}

		if len(fields) > 0 {
			if !strings.Contains(fields, ",") {
				queryBody["_source"] = fields
			} else {
				queryBody["_source"] = strings.Split(fields, ",")
			}
		}

		if len(query) > 0 {
			queryBody["query"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"] = map[string]interface{}{}
			queryBody["query"].(map[string]interface{})["query_string"].(map[string]interface{})["query"] = query
		}

		if len(sort) > 0 {
			sortFields := make([]string, 0)
			sortFields = append(sortFields, sort)
			queryBody["sort"] = sortFields
		}

		if maxSlicedCount > 1 {
			log.Tracef("sliced scroll, %d of %d", slicedId, maxSlicedCount)
			queryBody["slice"] = map[string]interface{}{}
			queryBody["slice"].(map[string]interface{})["id"] = slicedId
			queryBody["slice"].(map[string]interface{})["max"] = maxSlicedCount
		}

		jsonBody, err = json.Marshal(queryBody)
		if err != nil {
			log.Error(err)
		}
	}

	body, err := Request(s.Compress, "POST", url, s.Auth, bytes.NewBuffer(jsonBody), s.HttpProxy)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	scroll = &Scroll{}
	err = DecodeJson(body, scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, err
}

func (s *ESAPIV5) NextScroll(scrollTime string, scrollId string) (ScrollAPI, error) {
	id := bytes.NewBufferString(scrollId)

	url := fmt.Sprintf("%s/_search/scroll?scroll=%s&scroll_id=%s", s.Host, scrollTime, id)

	body, err := Request(s.Compress, "GET", url, s.Auth, nil, s.HttpProxy)

	// decode elasticsearch scroll response
	scroll := &Scroll{}
	err = DecodeJson(body, &scroll)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return scroll, nil
}
