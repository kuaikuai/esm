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
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"

	log "github.com/cihub/seelog"
)

type BulkOperation uint8

const (
	opIndex BulkOperation = iota
	opDelete
)

func (op BulkOperation) String() string {
	switch op {
	case opIndex:
		return "opIndex"
	case opDelete:
		return "opDelete"
	default:
		return fmt.Sprintf("unknown:%d", op)
	}
}

func (m *Migrator) recoveryIndexSettings(sourceIndexRefreshSettings map[string]interface{}) {
	//update replica and refresh_interval
	for name, interval := range sourceIndexRefreshSettings {
		tempIndexSettings := getEmptyIndexSettings()
		tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"] = interval
		//tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"] = 1
		m.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings)
		if m.Config.Refresh {
			m.TargetESAPI.Refresh(name)
		}
	}
}

func (m *Migrator) ClusterReady(api ESAPI) (*ClusterHealth, bool) {
	health := api.ClusterHealth()

	if !m.Config.WaitForGreen {
		return health, true
	}

	if health.Status == "red" {
		return health, false
	}

	if m.Config.WaitForGreen == false && health.Status == "yellow" {
		return health, true
	}

	if health.Status == "green" {
		return health, true
	}

	return health, false
}

func (m *Migrator) NewBulkWorker(docCount *int, pb *pb.ProgressBar, wg *sync.WaitGroup) {

	log.Debug("start es bulk worker")

	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	idleDuration := 5 * time.Second
	idleTimeout := time.NewTimer(idleDuration)
	defer idleTimeout.Stop()

	taskTimeOutDuration := 5 * time.Minute
	taskTimeout := time.NewTimer(taskTimeOutDuration)
	defer taskTimeout.Stop()

READ_DOCS:
	for {
		idleTimeout.Reset(idleDuration)
		taskTimeout.Reset(taskTimeOutDuration)
		select {
		case docI, open := <-m.DocChan:
			var err error
			log.Trace("read doc from channel,", docI)
			// this check is in case the document is an error with scroll stuff
			if status, ok := docI["status"]; ok {
				if status.(int) == 404 {
					log.Error("error: ", docI["response"])
					continue
				}
			}

			// sanity check
			for _, key := range []string{"_index", "_type", "_source", "_id"} {
				if _, ok := docI[key]; !ok {
					break READ_DOCS
				}
			}

			var tempDestIndexName string
			var tempTargetTypeName string
			tempDestIndexName = docI["_index"].(string)
			tempTargetTypeName = docI["_type"].(string)

			if m.Config.TargetIndexName != "" {
				tempDestIndexName = m.Config.TargetIndexName
			}

			if m.Config.OverrideTypeName != "" {
				tempTargetTypeName = m.Config.OverrideTypeName
			}

			doc := Document{
				Index:  tempDestIndexName,
				Type:   tempTargetTypeName,
				source: docI["_source"].(map[string]interface{}),
				Id:     docI["_id"].(string),
			}

			if m.Config.RegenerateID {
				doc.Id = ""
			}

			if m.Config.RenameFields != "" {
				kvs := strings.Split(m.Config.RenameFields, ",")
				for _, i := range kvs {
					fvs := strings.Split(i, ":")
					oldField := strings.TrimSpace(fvs[0])
					newField := strings.TrimSpace(fvs[1])
					if oldField == "_type" {
						doc.source[newField] = docI["_type"].(string)
					} else {
						v := doc.source[oldField]
						doc.source[newField] = v
						delete(doc.source, oldField)
					}
				}
			}

			// add doc "_routing" if exists
			if _, ok := docI["_routing"]; ok {
				str, ok := docI["_routing"].(string)
				if ok && str != "" {
					doc.Routing = str
				}
			}

			// if channel is closed flush and gtfo
			if !open {
				goto WORKER_DONE
			}

			// sanity check
			if len(doc.Index) == 0 || len(doc.Type) == 0 {
				log.Errorf("failed decoding document: %+v", doc)
				continue
			}

			// encode the doc and and the _source field for a bulk request
			post := map[string]Document{
				"index": doc,
			}
			if err = docEnc.Encode(post); err != nil {
				log.Error(err)
			}
			if err = docEnc.Encode(doc.source); err != nil {
				log.Error(err)
			}

			// append the doc to the main buffer
			mainBuf.Write(docBuf.Bytes())
			// reset for next document
			bulkItemSize++
			(*docCount)++
			docBuf.Reset()

			// if we approach the 100mb es limit, flush to es and reset mainBuf
			if mainBuf.Len()+docBuf.Len() > (m.Config.BulkSizeInMB * 1024 * 1024) {
				goto CLEAN_BUFFER
			}

		case <-idleTimeout.C:
			log.Warn("5s no message input")
			goto CLEAN_BUFFER
		case <-taskTimeout.C:
			log.Warn("5m no message input, close worker")
			goto WORKER_DONE
		}

		goto READ_DOCS

	CLEAN_BUFFER:
		m.TargetESAPI.Bulk(&mainBuf)
		log.Trace("clean buffer, and execute bulk insert")
		pb.Add(bulkItemSize)
		bulkItemSize = 0
		if m.Config.SleepSecondsAfterEachBulk > 0 {
			time.Sleep(time.Duration(m.Config.SleepSecondsAfterEachBulk) * time.Second)
		}
	}
WORKER_DONE:
	if docBuf.Len() > 0 {
		mainBuf.Write(docBuf.Bytes())
		bulkItemSize++
	}
	m.TargetESAPI.Bulk(&mainBuf)
	log.Trace("bulk insert")
	pb.Add(bulkItemSize)
	bulkItemSize = 0
	wg.Done()
}

func (m *Migrator) bulkRecords(bulkOp BulkOperation, dstEsApi ESAPI, targetIndex string, targetType string, diffDocMaps map[string]interface{}) error {
	//var err error
	docCount := 0
	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	//var tempDestIndexName string
	//var tempTargetTypeName string
	log.Debugf("bulkRecords bulkOp=%s, len(diffDocMaps)=%d", bulkOp, len(diffDocMaps))

	for docId, docData := range diffDocMaps {
		docI := docData.(map[string]interface{})
		log.Tracef("now will bulk %s docId=%s, docData=%+v", bulkOp, docId, docData)
		//tempDestIndexName = docI["_index"].(string)
		//tempTargetTypeName = docI["_type"].(string)
		var strOperation string
		doc := Document{
			Index: targetIndex,
			Type:  targetType,
			Id:    docId, // docI["_id"].(string),
		}
		if m.TargetESAPI.GetMainVersion() > ES7 {
			//don't support _type after es8, TODO: opensearch?
			doc.Type = ""
		}

		switch bulkOp {
		case opIndex:
			doc.source = docI // docI["_source"].(map[string]interface{}),
			strOperation = "index"
		case opDelete:
			strOperation = "delete"
			//do nothing
		}

		// encode the doc and and the _source field for a bulk request

		post := map[string]Document{
			strOperation: doc,
		}
		_ = Verify(docEnc.Encode(post))
		if bulkOp == opIndex {
			_ = Verify(docEnc.Encode(doc.source))
		}
		// append the doc to the main buffer
		mainBuf.Write(docBuf.Bytes())
		// reset for next document
		bulkItemSize++
		docCount++
		docBuf.Reset()
	}

	if mainBuf.Len() > 0 {
		_ = Verify(dstEsApi.Bulk(&mainBuf))
	}
	return nil
}

func (m *Migrator) SyncBetweenIndex(srcEsApi ESAPI, dstEsApi ESAPI, cfg *Config) (string, error) {
	// _id => value
	srcDocMaps := make(map[string]interface{})
	dstDocMaps := make(map[string]interface{})
	diffDocMaps := make(map[string]interface{})

	srcTotalCount := 0
	srcRecordIndex := 0
	dstRecordIndex := 0
	var err error
	srcType := ""
	dstType := ""
	var srcScroll ScrollAPI = nil
	var dstScroll ScrollAPI = nil
	var emptyScroll = &EmptyScroll{}
	lastSrcId := ""
	lastDestId := ""
	needScrollSrc := true
	needScrollDest := true

	addCount := 0
	updateCount := 0
	deleteCount := 0

	//TODO: 进度计算,分为 [ scroll src/dst + index ] => delete 几个部分
	srcBar := pb.New(1).Prefix("Progress")
	//srcBar := pb.New(1).Prefix("Source")
	//dstBar := pb.New(100).Prefix("Dest")
	//pool, err := pb.StartPool(srcBar, dstBar)

	for {
		if srcScroll == nil {
			srcScroll, err = srcEsApi.NewScroll(cfg.SourceIndexNames, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.StampValue, cfg.SrcSortField, 0, cfg.ScrollSliceSize, cfg.Fields)
			if err != nil {
				log.Infof("can not scroll for source index: %s, reason:%s", cfg.SourceIndexNames, err.Error())
				return "", err
			}
			srcTotalCount = srcScroll.GetHitsTotal()
			log.Infof("src total count=%d", srcTotalCount)
			srcBar.Total = int64(srcTotalCount)
			srcBar.Start()
		} else if needScrollSrc {
			srcScroll = VerifyWithResult(srcEsApi.NextScroll(cfg.ScrollTime, srcScroll.GetScrollId())).(ScrollAPI)
		}

		if dstScroll == nil {
			dstScroll, err = dstEsApi.NewScroll(cfg.TargetIndexName, cfg.ScrollTime, cfg.DocBufferCount, cfg.Query,
				cfg.StampValue, cfg.DstSortField, 0, cfg.ScrollSliceSize, cfg.Fields)
			if err != nil {
				log.Infof("can not scroll for dest index: %s, reason:%s", cfg.TargetIndexName, err.Error())
				//生成一个 empty 的, 相当于直接bulk?
				dstScroll = emptyScroll

				//没有 dest index,以 src 的条数作为总数
				//dstBar.Total = int64(srcScroll.GetHitsTotal()) // = pb.New(srcScroll.GetHitsTotal()).Prefix("Dest")
			} else {
				//有 dest index,
				//dstBar.Total = int64(dstScroll.GetHitsTotal()) // pb.New(dstScroll.GetHitsTotal()).Prefix("Dest")
			}
			//dstBar.Start()
			log.Infof("dst total count=%d", dstScroll.GetHitsTotal())
		} else if needScrollDest {
			dstScroll = VerifyWithResult(dstEsApi.NextScroll(cfg.ScrollTime, dstScroll.GetScrollId())).(ScrollAPI)
		}

		//从目标 index 中查询,并放入 destMap, 如果没有则是空
		if needScrollDest {
			for idx, dstDocI := range dstScroll.GetDocs() {
				destId := dstDocI.(map[string]interface{})["_id"].(string)
				dstSource := dstDocI.(map[string]interface{})["_source"]
				lastDestId = destId
				_ = idx
				//log.Tracef("dst [%d]: dstId=%s", dstRecordIndex+idx, destId)

				if srcSource, found := srcDocMaps[destId]; found {
					delete(srcDocMaps, destId)

					//如果从 src 的 map 中找到匹配地项
					if !reflect.DeepEqual(srcSource, dstSource) {
						//不相等, 则需要更新
						diffDocMaps[destId] = srcSource
						updateCount++
					} else {
						//完全相等, 则不需要处理
					}
				} else {
					dstDocMaps[destId] = dstSource
				}
				//dstBar.Increment()
			}
			dstRecordIndex += len(dstScroll.GetDocs())
		}

		//先将 src 的当前批次查出并放入 map
		if needScrollSrc {
			for idx, srcDocI := range srcScroll.GetDocs() {
				srcId := srcDocI.(map[string]interface{})["_id"].(string)
				srcSource := srcDocI.(map[string]interface{})["_source"]
				srcType = srcDocI.(map[string]interface{})["_type"].(string)
				dstType = srcType
				if m.Config.OverrideTypeName != "" {
					dstType = m.Config.OverrideTypeName
				}
				lastSrcId = srcId
				_ = idx
				//log.Tracef("src [%d]: srcId=%s", srcRecordIndex+idx, srcId)

				if len(lastDestId) == 0 {
					//没有 destId, 表示 目标 index 中没有数据, 直接全部更新
					diffDocMaps[srcId] = srcSource
					addCount++
				} else if dstSource, ok := dstDocMaps[srcId]; ok { //能从 dstDocMaps 中找到相同ID的数据
					if !reflect.DeepEqual(srcSource, dstSource) {
						//不完全相同,需要更新,否则忽略
						diffDocMaps[srcId] = srcSource
						updateCount++
					}
					//从 dst 中删除相同的
					delete(dstDocMaps, srcId)
				} else {
					//找不到相同的 id, 可能是 dst 还没找到, 或者 dst 中不存在
					if srcId < lastDestId {
						//dest 已经超过当前的 srcId, 表示 dst 中不存在
						diffDocMaps[srcId] = srcSource
						addCount++
					} else {
						srcDocMaps[srcId] = srcSource
					}
				}
				srcBar.Increment()
			}
			srcRecordIndex += len(srcScroll.GetDocs())
		}

		if len(diffDocMaps) > 0 {
			log.Tracef("now will bulk index %d records", len(diffDocMaps))
			_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, dstType, diffDocMaps))
			diffDocMaps = make(map[string]interface{})
		}

		log.Tracef("Before needScrollSrc=%t, needScrollDest=%t, lastSrcId=%s, lastDestId=%s,"+
			"len(srcScroll.GetDocs()=%d, len(dstScroll.GetDocs())=%d,"+
			"len(srcDocMaps)=%d,len(dstDocMaps)=%d",
			needScrollSrc, needScrollDest, lastSrcId, lastDestId,
			len(srcScroll.GetDocs()), len(dstScroll.GetDocs()),
			len(srcDocMaps), len(dstDocMaps))

		//计算应该继续查询哪个: src or/and dest
		newNeedScrollSrc := false
		newNeedScrollDest := false

		if needScrollSrc && len(srcScroll.GetDocs()) == cfg.DocBufferCount {
			newNeedScrollSrc = true
		}
		if needScrollDest && len(dstScroll.GetDocs()) == cfg.DocBufferCount {
			newNeedScrollDest = true
		}
		needScrollSrc = newNeedScrollSrc
		needScrollDest = newNeedScrollDest

		//log.Debugf("After needScrollSrc=%t, needScrollDest=%t, lastSrcId=%s, lastDestId=%s,"+
		//	"len(srcScroll.GetDocs()=%d, len(dstScroll.GetDocs())=%d,"+
		//	"len(srcDocMaps)=%d,len(dstDocMaps)=%d",
		//	needScrollSrc, needScrollDest, lastSrcId, lastDestId,
		//	len(srcScroll.GetDocs()), len(dstScroll.GetDocs()),
		//	len(srcDocMaps), len(dstDocMaps))

		if len(srcDocMaps) > 0 && lastSrcId > lastDestId {
			// dst 已经中已经没有更多的记录, 可以直接将所有的 src 都同步到 dst 中了,避免其中保存太多
			addCount += len(srcDocMaps)
			_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, dstType, srcDocMaps))
			srcDocMaps = make(map[string]interface{})
		}

		if len(dstDocMaps) > 0 && lastSrcId < lastDestId {
			//dstDocMaps 中还有记录,而且当前已经检测过所有比 srcId 都大的数据,之后的比较不会再更改结构,避免其中保存太多
			if cfg.EnableDelete {
				deleteCount += len(dstDocMaps)
				_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, dstType, dstDocMaps))
			}
			dstDocMaps = make(map[string]interface{})
		}

		//如果 src 和 dst 都遍历完毕, 才退出
		if !needScrollSrc && !needScrollDest {
			log.Infof("can not find more, will quit, remain index %d, delete %d", len(srcDocMaps), len(dstDocMaps))

			if len(srcDocMaps) > 0 {
				addCount += len(srcDocMaps)
				_ = Verify(m.bulkRecords(opIndex, dstEsApi, cfg.TargetIndexName, dstType, srcDocMaps))
				srcDocMaps = make(map[string]interface{})
			}
			if len(dstDocMaps) > 0 {
				//最后在 dst 中还有遗留的,表示 dst 中多的.需要删除
				if cfg.EnableDelete {
					deleteCount += len(dstDocMaps)
					_ = Verify(m.bulkRecords(opDelete, dstEsApi, cfg.TargetIndexName, dstType, dstDocMaps))
				}
				dstDocMaps = make(map[string]interface{})
			}

			break
		}

		//目标不存在 或 src 还没有查询到和 dest 一样的地方
		if cfg.SleepSecondsAfterEachBulk > 0 {
			time.Sleep(time.Duration(cfg.SleepSecondsAfterEachBulk) * time.Second)
		}
	}
	_ = Verify(srcEsApi.DeleteScroll(srcScroll.GetScrollId()))
	_ = Verify(dstEsApi.DeleteScroll(dstScroll.GetScrollId()))

	srcBar.FinishPrint("Source End")
	//dstBar.FinishPrint("Dest End")
	//pool.Stop()

	log.Infof("sync %s(%d/%d) to %s(%d), add=%d, update=%d, delete=%d",
		cfg.SourceIndexNames, srcRecordIndex, srcTotalCount, cfg.TargetIndexName, dstRecordIndex,
		addCount, updateCount, deleteCount)

	//log.Infof("diffDocMaps=%+v", diffDocMaps)

	return "", err
}
