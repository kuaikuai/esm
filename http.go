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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"github.com/parnurzeal/gorequest"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func BasicAuth(req *fasthttp.Request, user, pass string) {
	msg := fmt.Sprintf("%s:%s", user, pass)
	encoded := base64.StdEncoding.EncodeToString([]byte(msg))
	req.Header.Add("Authorization", "Basic "+encoded)
}

func Get(url string, auth *Auth, proxy string) (*http.Response, string, []error) {

	request := gorequest.New()

	tr := &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: false,
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
	}
	request.Transport = tr

	if auth != nil {
		request.SetBasicAuth(auth.User, auth.Pass)
	}

	//request.Type("application/json")

	if len(proxy) > 0 {
		request.Proxy(proxy)
	}

	resp, body, errs := request.Get(url).End()
	return resp, body, errs

}

func Post(url string, auth *Auth, body string, proxy string) (*http.Response, string, []error) {
	request := gorequest.New()
	tr := &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: false,
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
	}
	request.Transport = tr

	if auth != nil {
		request.SetBasicAuth(auth.User, auth.Pass)
	}

	//request.Type("application/json")

	if len(proxy) > 0 {
		request.Proxy(proxy)
	}

	request.Post(url)

	if len(body) > 0 {
		request.Send(body)
	}

	return request.End()
}

func newDeleteRequest(client *http.Client, method, urlStr string) (*http.Request, error) {
	if method == "" {
		// We document that "" means "GET" for Request.Method, and people have
		// relied on that from NewRequest, so keep that working.
		// We still enforce validMethod for non-empty methods.
		method = "GET"
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	req := &http.Request{
		Method:     method,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       u.Host,
	}
	return req, nil
}

var client = &http.Client{
	Transport: &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: false,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}
var fastHttpClient = &fasthttp.Client{
	TLSConfig: &tls.Config{InsecureSkipVerify: true},
}

func DoRequest(compress bool, method string, loadUrl string, auth *Auth, body []byte, proxy string) (string, error) {

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	//defer fasthttp.ReleaseRequest(req)   // <- do not forget to release
	//defer fasthttp.ReleaseResponse(resp) // <- do not forget to release

	req.SetRequestURI(loadUrl)
	req.Header.SetMethod(method)

	//req.Header.Set("Content-Type", "application/json")

	if compress {
		req.Header.Set("Accept-Encoding", "gzip")
		req.Header.Set("content-encoding", "gzip")
	}

	if auth != nil {
		req.URI().SetUsername(auth.User)
		req.URI().SetPassword(auth.Pass)
	}

	if len(body) > 0 {
		if compress {
			_, err := fasthttp.WriteGzipLevel(req.BodyWriter(), body, fasthttp.CompressBestSpeed)
			if err != nil {
				panic(err)
			}
		} else {
			req.SetBody(body)
		}
	}

	err := fastHttpClient.Do(req, resp)

	if err != nil {
		panic(err)
	}
	if resp == nil {
		panic("empty response")
	}

	log.Debug("received status code", resp.StatusCode, "from", string(resp.Header.Header()), "content",
		SubString(string(resp.Body()), 0, 500), req)

	if resp.StatusCode() == http.StatusOK || resp.StatusCode() == http.StatusCreated {

	} else {
		//log.Error("received status code", resp.StatusCode, "from", string(resp.Header.Header()), "content", string(resp.Body()), req)
	}

	//if compress{
	//	data,err:= resp.BodyGunzip()
	//	return string(data),err
	//}

	return string(resp.Body()), nil
}

func Request(compress bool, method string, loadUrl string, auth *Auth, body *bytes.Buffer, proxy string) (string, error) {

	var err error
	var reqest *http.Request
	if body != nil {
		reqest, err = http.NewRequest(method, loadUrl, body)
	} else {
		reqest, err = newDeleteRequest(client, method, loadUrl)
	}

	if err != nil {
		panic(err)
	}

	if auth != nil {
		reqest.SetBasicAuth(auth.User, auth.Pass)
	}

	oldTransport := client.Transport.(*http.Transport)
	if len(proxy) > 0 {
		proxyUrl := VerifyWithResult(url.Parse(proxy))
		proxyFunc := http.ProxyURL(proxyUrl)
		oldTransport.Proxy = proxyFunc
	} else {
		oldTransport.Proxy = nil
	}
	reqest.Header.Set("Content-Type", "application/json")

	//enable gzip
	//reqest.Header.Set("Content-Encoding", "gzip")
	//GzipHandler(reqest)
	//

	resp, errs := client.Do(reqest)
	if errs != nil {
		log.Error(SubString(errs.Error(), 0, 500))
		return "", errs
	}

	if resp != nil && resp.Body != nil {
		//io.Copy(ioutil.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if resp.StatusCode != 200 {
		b := VerifyWithResult(io.ReadAll(resp.Body))
		return "", errors.New(
			fmt.Sprintf("server error: code=%d, length=%d, info=", resp.StatusCode, resp.ContentLength) + string(b))
	}

	respBody, err := io.ReadAll(resp.Body)

	//log.Error(SubString(string(respBody), 0, 500))

	if err != nil {
		log.Error(SubString(string(err.Error()), 0, 500))
		return string(respBody), err
	}

	if err != nil {
		return string(respBody), err
	}
	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	return string(respBody), nil
}

func DecodeJson(jsonStream string, o interface{}) error {

	decoder := json.NewDecoder(strings.NewReader(jsonStream))
	// UseNumber causes the Decoder to unmarshal a number into an interface{} as a Number instead of as a float64.
	decoder.UseNumber()
	//decoder.

	if err := decoder.Decode(o); err != nil {
		fmt.Println("error:", err)
		return err
	}
	return nil
}

func DecodeJsonBytes(jsonStream []byte, o interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(jsonStream))
	// UseNumber causes the Decoder to unmarshal a number into an interface{} as a Number instead of as a float64.
	decoder.UseNumber()

	if err := decoder.Decode(o); err != nil {
		fmt.Println("error:", err)
		return err
	}
	return nil
}
