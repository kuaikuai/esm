package main

import (
	"flag"
	"os"
	"testing"

	log "github.com/cihub/seelog"
	"github.com/stretchr/testify/assert"
)

const (
	srcIndexName = "account"
	dstIndexName = "account"
)

func TestMain(m *testing.M) {
	flag.Parse()

	setInitLogging("debug")

	exitCode := m.Run()
	os.Exit(exitCode)
}

type EsTestFunc = func(t *testing.T, srcEsApi ESAPI, dstEsApi ESAPI)

func TestEsApiInfo(t *testing.T) {

	assert.True(t, true, "断言")

	srcEsApi := ParseEsApi(true, "http://localhost:9201", "", "http://localhost:8888", false)
	dstEsApi := ParseEsApi(false, "http://localhost:19201", "", "http://localhost:8888", false)

	testCases := []struct {
		name     string
		testFunc EsTestFunc
		skip     bool //对于一些比较特别的case(比如删除操作), 不要随便进行, 可设置其为 true 跳过(不设置的话,默认为 false,表示会执行)
	}{
		{name: "version", testFunc: getClusterVersion},
		{name: "health", testFunc: getClusterHealth},
		{name: "settings", testFunc: indexSettings},
		{name: "scroll", testFunc: scroll},
	}

	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			if tCase.skip {
				log.Infof("skip %q", tCase.name)
			} else {
				tCase.testFunc(t, srcEsApi, dstEsApi)
			}
		})
	}

}

func getClusterVersion(t *testing.T, srcEsApi ESAPI, dstEsApi ESAPI) {
	log.Info("version is %+v", srcEsApi.ClusterVersion())
}

func getClusterHealth(t *testing.T, srcEsApi ESAPI, dstEsApi ESAPI) {
	log.Info("health is %+v", srcEsApi.ClusterHealth())
}

func indexSettings(t *testing.T, srcEsApi ESAPI, dstEsApi ESAPI) {
	srcSettings, err := VerifyWithResultEx(srcEsApi.GetIndexSettings(srcIndexName))
	log.Infof("srcSettings=%+v, err=%+v", srcSettings, err)

	dstSettings, err := VerifyWithResultEx(dstEsApi.GetIndexSettings(dstIndexName))
	log.Infof("dstSettings=%+v, err=%+v", dstSettings, err)

	settings := (*srcSettings)[srcIndexName].(map[string]interface{})
	_ = Verify(dstEsApi.UpdateIndexSettings("test_index", settings))
}

func scroll(t *testing.T, srcEsApi ESAPI, dstEsApi ESAPI) {
	var scrollApi ScrollAPI
	var err error
	totalDocs := 0
	iterIndex := 0
	scrollApi, err = VerifyWithResultEx(srcEsApi.NewScroll(srcIndexName,
		"10m", 33, `age:40`, "account_number:>=990",
		"", 0, 1, ""))

	assert.NoError(t, err)
	log.Infof("hitsTotal=%d, scrollId=%s, len(docs)=%d",
		scrollApi.GetHitsTotal(), scrollApi.GetScrollId(), len(scrollApi.GetDocs()))
	for {
		iterIndex++
		totalDocs += len(scrollApi.GetDocs())

		log.Infof("iterIndex=%d, totalDocs=%d, hitsTotal=%d, len(docs)=%d",
			iterIndex, totalDocs, scrollApi.GetHitsTotal(), len(scrollApi.GetDocs()))

		scrollApi, err = VerifyWithResultEx(srcEsApi.NextScroll("10m", scrollApi.GetScrollId()))
		if err != nil {
			// 全部结束, 可以删除
			_ = Verify(srcEsApi.DeleteScroll(scrollApi.GetScrollId()))
			break
		}

		if scrollApi.GetDocs() == nil || len(scrollApi.GetDocs()) == 0 {
			_ = Verify(srcEsApi.DeleteScroll(scrollApi.GetScrollId()))
			break
		}
	}

	log.Infof("total documents is %d", totalDocs)
}
