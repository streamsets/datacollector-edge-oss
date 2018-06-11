// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package httpcommon

import (
	"crypto/tls"
	"crypto/x509"
	oauth1 "github.com/mrjones/oauth"
	dac "github.com/xinsnake/go-http-digest-auth-client"
	"io/ioutil"
	"net/http"
)

const (
	None      = "NONE"
	Basic     = "BASIC"
	Digest    = "DIGEST"
	Universal = "UNIVERSAL"
	OAuth     = "OAUTH"
)

type HttpCommon struct {
	HttpClient      *http.Client
	clientConfig    *ClientConfigBean
	digestTransport dac.DigestTransport
}

type ClientConfigBean struct {
	HttpCompression string                 `ConfigDef:"type=STRING,required=true"`
	TlsConfig       TlsConfigBean          `ConfigDefBean:"tlsConfig"`
	AuthType        string                 `ConfigDef:"type=STRING,required=true"`
	Oauth           OAuthConfigBean        `ConfigDefBean:"oauth"`
	BasicAuth       PasswordAuthConfigBean `ConfigDefBean:"basicAuth"`
}

type TlsConfigBean struct {
	TlsEnabled         bool   `ConfigDef:"type=BOOLEAN,required=true"`
	TrustStoreFilePath string `ConfigDef:"type=STRING,required=true"`
}

type OAuthConfigBean struct {
	ConsumerKey    string `ConfigDef:"type=STRING,required=true"`
	ConsumerSecret string `ConfigDef:"type=STRING,required=true"`
	Token          string `ConfigDef:"type=STRING,required=true"`
	TokenSecret    string `ConfigDef:"type=STRING,required=true"`
}

type PasswordAuthConfigBean struct {
	Username string `ConfigDef:"type=STRING,required=true"`
	Password string `ConfigDef:"type=STRING,required=true"`
}

func (h *HttpCommon) InitializeClient(clientConfig ClientConfigBean) error {
	var err error
	if clientConfig.TlsConfig.TlsEnabled {
		caCert, err := ioutil.ReadFile(clientConfig.TlsConfig.TrustStoreFilePath)
		if err != nil {
			return err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		h.HttpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:            caCertPool,
					InsecureSkipVerify: true,
				},
			},
		}
	} else {
		h.HttpClient = &http.Client{}
	}
	h.clientConfig = &clientConfig

	if h.clientConfig.AuthType == OAuth {
		c := oauth1.NewConsumer(
			clientConfig.Oauth.ConsumerKey,
			clientConfig.Oauth.ConsumerSecret,
			oauth1.ServiceProvider{},
		)
		c.Debug(true)

		t := oauth1.AccessToken{
			Token:  clientConfig.Oauth.Token,
			Secret: clientConfig.Oauth.TokenSecret,
		}
		h.HttpClient, err = c.MakeHttpClient(&t)
	}

	switch h.clientConfig.AuthType {
	case Digest:
		h.digestTransport = dac.NewTransport(h.clientConfig.BasicAuth.Username, h.clientConfig.BasicAuth.Password)
	case OAuth:
		c := oauth1.NewConsumer(
			h.clientConfig.Oauth.ConsumerKey,
			h.clientConfig.Oauth.ConsumerSecret,
			oauth1.ServiceProvider{},
		)
		t := oauth1.AccessToken{
			Token:  h.clientConfig.Oauth.Token,
			Secret: h.clientConfig.Oauth.TokenSecret,
		}
		h.HttpClient, err = c.MakeHttpClient(&t)
	}

	return err
}

func (h *HttpCommon) Execute(req *http.Request) (resp *http.Response, err error) {
	switch h.clientConfig.AuthType {
	case Basic:
		fallthrough
	case Universal:
		req.SetBasicAuth(h.clientConfig.BasicAuth.Username, h.clientConfig.BasicAuth.Password)
	case Digest:
		return h.digestTransport.RoundTrip(req)
	}
	return h.HttpClient.Do(req)
}
