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

type OPENSEARCHV2 struct {
	OPENSEARCHV1
}

func NewOpenSearchV2(host string, auth *Auth, httpProxy string, compress bool, version *ClusterVersion) ESAPI {
	apiOsV2 := &OPENSEARCHV2{}

	apiOsV2.Host = host
	apiOsV2.Auth = auth
	apiOsV2.HttpProxy = httpProxy
	apiOsV2.Compress = compress
	apiOsV2.Version = version

	return apiOsV2
}
