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
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/hashicorp/go-cleanhttp"
	oauth1 "github.com/mrjones/oauth"
	dac "github.com/xinsnake/go-http-digest-auth-client"
)

const (
	None      = "NONE"
	Basic     = "BASIC"
	Digest    = "DIGEST"
	Universal = "UNIVERSAL"
	OAuth     = "OAUTH"
)

type HttpCommon struct {
	HttpClient   *http.Client
	clientConfig *ClientConfigBean
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
	TrustStoreType     string `ConfigDef:"type=STRING,required=true"`
	TrustStorePassword string `ConfigDef:"type=STRING,required=true"`
	KeyStoreFilePath   string `ConfigDef:"type=STRING,required=true"`
	KeyStoreType       string `ConfigDef:"type=STRING,required=true"`
	KeyStorePassword   string `ConfigDef:"type=STRING,required=true"`
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

// InitializeClient configures an http.Client from a config bean
func (h *HttpCommon) InitializeClient(clientConfig ClientConfigBean) error {
	h.clientConfig = &clientConfig

	var err error
	var caCertPool *x509.CertPool // nil CertPool will use system CA certs

	// If the user specified a custom certs file, try to load it
	if clientConfig.TlsConfig.TrustStoreFilePath != "" {
		caCert, err := ioutil.ReadFile(clientConfig.TlsConfig.TrustStoreFilePath)
		if err != nil {
			return err
		}

		// appending to the system cert pool rather than replacing it
		caCertPool, err := x509.SystemCertPool()
		if err != nil {
			return err
		}
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("failed to add TrustStoreFile to CA Cert pool")
		}
	}

	// creates a clean transport using sane defaults, but one
	// that won't be shared with multiple client instances
	httpTransport := cleanhttp.DefaultTransport()

	// set our own TLS client configuration
	httpTransport.TLSClientConfig = &tls.Config{
		RootCAs: caCertPool,
	}

	// Our customized default client
	h.HttpClient = &http.Client{
		Transport: httpTransport,
	}

	// TODO: The transport is replaced by the digest auth library
	// provided transport. This means our custom TLS settings will
	// be lost. It doesn't appear this library allows us to chain
	// transports or provide an http client. This will likely
	// need to be revisited in the future with our own implementation
	switch h.clientConfig.AuthType {
	case Digest:
		// Use digest library's transport
		digestTransport := dac.NewTransport(h.clientConfig.BasicAuth.Username, h.clientConfig.BasicAuth.Password)
		h.HttpClient.Transport = &digestTransport
	case OAuth:
		consumer := oauth1.NewConsumer(
			h.clientConfig.Oauth.ConsumerKey,
			h.clientConfig.Oauth.ConsumerSecret,
			oauth1.ServiceProvider{},
		)
		token := oauth1.AccessToken{
			Token:  h.clientConfig.Oauth.Token,
			Secret: h.clientConfig.Oauth.TokenSecret,
		}
		h.HttpClient, err = consumer.MakeHttpClient(&token)
	}

	return err
}

func (h *HttpCommon) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	switch h.clientConfig.AuthType {
	case Basic:
		fallthrough
	case Universal:
		req.SetBasicAuth(h.clientConfig.BasicAuth.Username, h.clientConfig.BasicAuth.Password)
	case Digest:
		// NO OP
	}
	return h.HttpClient.Do(req)
}
