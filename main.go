package main

import (
	"bufio"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	_ "runtime/pprof"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	log "github.com/cihub/seelog"
	goflags "github.com/jessevdk/go-flags"
	"github.com/mattn/go-isatty"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {
		//log.Infof("pprof listen at: http://%s/debug/pprof/", app.httpprof)
		mux := http.NewServeMux()

		// register pprof handler
		mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
			http.DefaultServeMux.ServeHTTP(w, r)
		})

		// register metrics handler
		//mux.HandleFunc("/debug/vars", app.metricsHandler)

		endpoint := http.ListenAndServe("0.0.0.0:6060", mux)
		log.Debug("stop pprof server: %v", endpoint)
	}()

	var err error
	c := &Config{}
	migrator := Migrator{}
	migrator.Config = c

	// parse args
	_, err = VerifyWithResultEx(goflags.Parse(c))
	if err != nil {
		return
	}

	setInitLogging(c.LogLevel)

	if len(c.SourceEs) == 0 && len(c.DumpInputFile) == 0 {
		log.Error("no input, type --help for more details")
		return
	}
	if len(c.TargetEs) == 0 && len(c.DumpOutFile) == 0 {
		log.Error("no output, type --help for more details")
		return
	}

	if c.SourceEs == c.TargetEs && c.SourceIndexNames == c.TargetIndexName {
		log.Error("migration output is the same as the output")
		return
	}

	var showBar bool = false
	if isatty.IsTerminal(os.Stdout.Fd()) {
		showBar = true
	} else if isatty.IsCygwinTerminal(os.Stdout.Fd()) {
		showBar = true
	} else {
		showBar = false
	}

	if c.Sync {
		//sync 功能时,只支持一个 index:
		if len(c.SourceIndexNames) == 0 || len(c.TargetIndexName) == 0 {
			log.Error("migration sync only support source 1 index to 1 target index")
			return
		}

		migrator.SourceESAPI = ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr, c.SourceProxy, c.Compress)
		if migrator.SourceESAPI == nil {
			log.Error("can not parse source es api")
			return
		}
		migrator.TargetESAPI = ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr, c.TargetProxy, false)
		if migrator.TargetESAPI == nil {
			log.Error("can not parse target es api")
			return
		}

		// 检查设置缺省的 sort Id,
		if len(c.SrcSortField) == 0 {
			c.SrcSortField = migrator.SourceESAPI.GetDefaultSortId()
		}
		if len(c.DstSortField) == 0 {
			c.DstSortField = migrator.TargetESAPI.GetDefaultSortId()
		}

		log.Infof("src sort id=%s, dest sort id=%s", c.SrcSortField, c.DstSortField)

		_, indexCount, sourceIndexMappings, err := migrator.SourceESAPI.GetIndexMappings(c.CopyAllIndexes, c.SourceIndexNames)
		sourceIndexRefreshSettings := map[string]interface{}{}
		err = copyIndexSettings(c, migrator, indexCount, sourceIndexRefreshSettings, sourceIndexMappings)
		if err != nil {
			log.Error(err)
			return
		}
		migrator.SyncBetweenIndex(migrator.SourceESAPI, migrator.TargetESAPI, c)
		return
	}

	//至少输出一次
	if c.RepeatOutputTimes < 1 {
		c.RepeatOutputTimes = 1
	} else {
		log.Info("source data will repeat send to target: ", c.RepeatOutputTimes, " times, the document id will be regenerated.")
	}

	if c.RepeatOutputTimes > 0 {

		for i := 0; i < c.RepeatOutputTimes; i++ {

			if c.RepeatOutputTimes > 1 {
				log.Info("repeat round: ", i+1)
			}

			// enough of a buffer to hold all the search results across all workers
			migrator.DocChan = make(chan map[string]interface{}, c.BufferCount)

			//var srcESVersion *ClusterVersion
			// create a progressbar and start a docCount
			var outputBar *pb.ProgressBar = pb.New(1).Prefix("Output ")

			var fetchBar = pb.New(1).Prefix("Scroll")

			wg := sync.WaitGroup{}

			//dealing with input
			if len(c.SourceEs) > 0 {
				//dealing with basic auth

				migrator.SourceESAPI = ParseEsApi(true, c.SourceEs, c.SourceEsAuthStr,
					migrator.Config.SourceProxy, c.Compress)
				if migrator.SourceESAPI == nil {
					log.Error("can not parse source es api")
					return
				}

				if c.ScrollSliceSize < 1 {
					c.ScrollSliceSize = 1
				}

				totalSize := 0
				finishedSlice := 0
				for slice := 0; slice < c.ScrollSliceSize; slice++ {
					scroll, err := migrator.SourceESAPI.NewScroll(c.SourceIndexNames, c.ScrollTime, c.DocBufferCount,
						c.Query, c.StampValue, c.SrcSortField, slice, c.ScrollSliceSize, c.Fields)
					if err != nil {
						log.Error(err)
						return
					}

					totalSize += scroll.GetHitsTotal()

					if scroll.GetDocs() != nil {

						if scroll.GetHitsTotal() == 0 {
							log.Error("can't find documents from source.")
							return
						}

						go func() {
							wg.Add(1)
							//process input
							// start scroll
							scroll.ProcessScrollResult(&migrator, fetchBar)

							// loop scrolling until done
							for scroll.Next(&migrator, fetchBar) == false {
							}

							if showBar {
								fetchBar.Finish()
							}

							// finished, close doc chan and wait for goroutines to be done
							wg.Done()
							finishedSlice++

							//clean up final results
							if finishedSlice == c.ScrollSliceSize {
								log.Debug("closing doc chan")
								close(migrator.DocChan)
							}
						}()
					}
				}

				if totalSize > 0 {
					fetchBar.Total = int64(totalSize)
					outputBar.Total = int64(totalSize)
				}

			} else if len(c.DumpInputFile) > 0 {
				//read file stream
				wg.Add(1)
				f, err := os.Open(c.DumpInputFile)
				if err != nil {
					log.Error(err)
					return
				}
				//get file lines
				lineCount := 0
				defer f.Close()
				r := bufio.NewReader(f)
				for {
					_, err := r.ReadString('\n')
					if io.EOF == err || nil != err {
						break
					}
					lineCount += 1
				}
				log.Trace("file line,", lineCount)

				fetchBar := pb.New(lineCount).Prefix("Read")
				outputBar = pb.New(lineCount).Prefix("Output ")

				f.Close()

				go migrator.NewFileReadWorker(fetchBar, &wg)

			}

			var pool *pb.Pool
			if showBar {

				// start pool
				pool, err = pb.StartPool(fetchBar, outputBar)
				if err != nil {
					panic(err)
				}
			}

			//dealing with output
			if len(c.TargetEs) > 0 {

				//get target es api
				migrator.TargetESAPI = ParseEsApi(false, c.TargetEs, c.TargetEsAuthStr,
					migrator.Config.TargetProxy, false)
				if migrator.TargetESAPI == nil {
					log.Error("can not parse target es api")
					return
				}

				log.Debug("start process with mappings")
				if c.CopyIndexMappings &&
					migrator.TargetESAPI.ClusterVersion().Version.Number[0] != migrator.SourceESAPI.ClusterVersion().Version.Number[0] {
					log.Error(migrator.SourceESAPI.ClusterVersion().Version, "=>",
						migrator.TargetESAPI.ClusterVersion().Version,
						",cross-big-version mapping migration not available, please update mapping manually :(")
					return
				}

				// wait for cluster state to be okay before moving
				idleDuration := 3 * time.Second
				timer := time.NewTimer(idleDuration)
				defer timer.Stop()
				for {
					timer.Reset(idleDuration)

					if len(c.SourceEs) > 0 {
						if status, ready := migrator.ClusterReady(migrator.SourceESAPI); !ready {
							log.Infof("%s at %s is %s, delaying migration ", status.Name, c.SourceEs, status.Status)
							<-timer.C
							continue
						}
					}

					if len(c.TargetEs) > 0 {
						if status, ready := migrator.ClusterReady(migrator.TargetESAPI); !ready {
							log.Infof("%s at %s is %s, delaying migration ", status.Name, c.TargetEs, status.Status)
							<-timer.C
							continue
						}
					}
					break
				}

				if len(c.SourceEs) > 0 {
					// get all indexes from source
					indexNames, indexCount, sourceIndexMappings, err := migrator.SourceESAPI.GetIndexMappings(c.CopyAllIndexes, c.SourceIndexNames)

					if err != nil {
						log.Error(err)
						return
					}

					sourceIndexRefreshSettings := map[string]interface{}{}

					log.Debugf("indexCount: %d", indexCount)

					if indexCount > 0 {
						//override indexnames to be copy
						c.SourceIndexNames = indexNames

						err = copyIndexSettings(c, migrator, indexCount, sourceIndexRefreshSettings, sourceIndexMappings)
						if err != nil {
							log.Error(err)
							return
						}

					} else {
						log.Error("index not exists,", c.SourceIndexNames)
						return
					}

					defer migrator.recoveryIndexSettings(sourceIndexRefreshSettings)
				} else if len(c.DumpInputFile) > 0 {
					//check shard settings
					//TODO support shard config
				}

			}

			log.Info("start data migration..")

			//start es bulk thread
			if len(c.TargetEs) > 0 {
				log.Debug("start es bulk workers")
				outputBar.Prefix("Bulk")
				var docCount int
				wg.Add(c.Workers)
				for i := 0; i < c.Workers; i++ {
					go migrator.NewBulkWorker(&docCount, outputBar, &wg)
				}
			} else if len(c.DumpOutFile) > 0 {
				// start file write
				outputBar.Prefix("Write")
				wg.Add(1)
				go migrator.NewFileDumpWorker(outputBar, &wg)
			}

			wg.Wait()

			if showBar {

				outputBar.Finish()
				// close pool
				pool.Stop()

			}
		}

	}

	log.Info("data migration finished.")
}

func copyIndexSettings(c *Config,
	migrator Migrator,
	indexCount int,
	sourceIndexRefreshSettings map[string]interface{},
	sourceIndexMappings *Indexes) (err error) {
	// copy index settings if user asked
	if c.CopyIndexSettings || c.ShardsCount > 0 {
		log.Info("start settings/mappings migration..")

		//get source index settings
		var sourceIndexSettings *Indexes
		sourceIndexSettings, err = migrator.SourceESAPI.GetIndexSettings(c.SourceIndexNames)
		log.Info("source index settings:", sourceIndexSettings)
		if err != nil {
			log.Error(err)
			return err
		}

		//get target index settings
		var targetIndexSettings *Indexes
		targetIndexSettings, err = migrator.TargetESAPI.GetIndexSettings(c.TargetIndexName)
		if err != nil {
			//ignore target es settings error
			log.Info(err)
		}
		log.Info("target IndexSettings", targetIndexSettings)

		//if there is only one index and we specify the dest indexname
		if c.SourceIndexNames != c.TargetIndexName && (len(c.TargetIndexName) > 0) && indexCount == 1 {
			log.Debugf("only one index,so we can rewrite indexname, src:%v, dest:%v ,indexCount:%d", c.SourceIndexNames, c.TargetIndexName, indexCount)
			(*sourceIndexSettings)[c.TargetIndexName] = (*sourceIndexSettings)[c.SourceIndexNames]
			delete(*sourceIndexSettings, c.SourceIndexNames)
			log.Info(sourceIndexSettings)
		}

		// dealing with indices settings
		for name, idx := range *sourceIndexSettings {
			log.Info("dealing with index,name:", name, ",settings:", idx)
			tempIndexSettings := getEmptyIndexSettings()

			targetIndexExist := false
			//if target index settings is exist and we don't copy settings, we use target settings
			if targetIndexSettings != nil {
				//if target es have this index and we dont copy index settings
				if val, ok := (*targetIndexSettings)[name]; ok {
					targetIndexExist = true
					tempIndexSettings = val.(map[string]interface{})
				}

				if c.RecreateIndex {
					migrator.TargetESAPI.DeleteIndex(name)
					targetIndexExist = false
				}
			}

			//copy index settings
			if c.CopyIndexSettings {
				tempIndexSettings = ((*sourceIndexSettings)[name]).(map[string]interface{})
			}

			//check map elements
			if _, ok := tempIndexSettings["settings"]; !ok {
				tempIndexSettings["settings"] = map[string]interface{}{}
			}

			if _, ok := tempIndexSettings["settings"].(map[string]interface{})["index"]; !ok {
				tempIndexSettings["settings"].(map[string]interface{})["index"] = map[string]interface{}{}
			}

			sourceIndexRefreshSettings[name] = ((*sourceIndexSettings)[name].(map[string]interface{}))["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"]

			//set refresh_interval
			//tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["refresh_interval"] = -1
			//tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"] = 0

			//clean up settings
			//delete(tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{}), "number_of_shards")

			//copy indexsettings and mappings
			if targetIndexExist {
				if migrator.TargetESAPI.CanUpdateIndex(name) {
					log.Debug("update index with settings,", name, tempIndexSettings)
					//override shard settings
					if c.ShardsCount > 0 {
						tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"] = c.ShardsCount
					}
					err = migrator.TargetESAPI.UpdateIndexSettings(name, tempIndexSettings)
					if err != nil {
						log.Error(err)
					}
				} else {
					log.Warnf("can not dynamic update index %s, just ignore", name)
				}
			} else {
				//override shard settings
				if c.ShardsCount > 0 {
					tempIndexSettings["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_shards"] = c.ShardsCount
				}

				log.Debug("create index with settings,", name, tempIndexSettings)
				err = migrator.TargetESAPI.CreateIndex(name, tempIndexSettings)
				if err != nil {
					log.Error(err)
				}
			}
		}

		if c.CopyIndexMappings {

			//if there is only one index and we specify the dest indexname
			if c.SourceIndexNames != c.TargetIndexName && (len(c.TargetIndexName) > 0) && indexCount == 1 {
				log.Debugf("only one index,so we can rewrite indexname, src:%v, dest:%v ,indexCount:%d", c.SourceIndexNames, c.TargetIndexName, indexCount)
				(*sourceIndexMappings)[c.TargetIndexName] = (*sourceIndexMappings)[c.SourceIndexNames]
				delete(*sourceIndexMappings, c.SourceIndexNames)
				log.Debug(sourceIndexMappings)
			}

			for name, mapping := range *sourceIndexMappings {
				err = migrator.TargetESAPI.UpdateIndexMapping(name, mapping.(map[string]interface{})["mappings"].(map[string]interface{}))
				if err != nil {
					log.Error(err)
				}
			}
		}

		log.Info("settings/mappings migration finished.")
	}
	return
}
