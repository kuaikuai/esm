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

import "bytes"

type ESAPI interface {
	ClusterHealth() *ClusterHealth
	ClusterVersion() *ClusterVersion
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
	GetIndices(pattern string) (*map[string]IndexInfo, error)
}
