package parallel

import (
	"testing"

	"net/http"
	"time"

	"crypto/tls"

	"github.com/redsift/go-parallel/reducers"
)

// List of top domains
var testUrls = []string{
	"facebook.com/",
	"twitter.com/",
	"google.com/",
	"youtube.com/",
	"instagram.com/",
	"linkedin.com/",
	"wordpress.org/",
	"pinterest.com/",
	"wikipedia.org/",
	"wordpress.com/",
	"blogspot.com/",
	"apple.com/",
	"adobe.com/",
	"tumblr.com/",
	"youtu.be/",
	"amazon.com/",
	"goo.gl/",
	"vimeo.com/",
	"microsoft.com/",
	"flickr.com/",
	"yahoo.com/",
	"bit.ly/",
	"godaddy.com/",
	"vk.com/",
	"reddit.com/",
	"buydomains.com/",
	"w3.org/",
	"nytimes.com/",
	"t.co/",
	"europa.eu/",
	"www.baidu.com/",
	"statcounter.com/",
	"wp.com/",
	"weebly.com/",
	"jimdo.com/",
	"blogger.com/",
	"github.com/",
	"bbc.co.uk/",
	"soundcloud.com/",
}

func cipherSuite(state *tls.ConnectionState) string {
	if state == nil {
		return "none"
	}
	switch state.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA256:
		return "TLS_RSA_WITH_AES_128_CBC_SHA256"
	case tls.TLS_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_RSA_WITH_AES_256_GCM_SHA384:
		return "TLS_RSA_WITH_AES_256_GCM_SHA384"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:
		return "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
	case tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305:
		return "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305"
	case tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305:
		return "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305"
	default:
		return "unknown"
	}
}

// perform the network request and return the TLS protocol
func perform(client *http.Client, url string) string {
	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	return url + " = " + cipherSuite(res.TLS)
}

// TestNetworkRequests checks top websites for their TLS cipher suites sequentially
func TestNetworkRequestsSerially(t *testing.T) {

	all := make([]string, 0, len(testUrls))

	for _, v := range testUrls {
		all = append(all, perform(http.DefaultClient, "https://"+v))
	}

	if l := len(all); len(testUrls) != l {
		t.Error("Unexpected length", l)
	}

	t.Log(all)
}

// TestNetworkRequests demonstrates a simple scattering of HTTPS network requests
// by checking top websites for their their TLS cipher suites
func TestNetworkRequestsInParallel(t *testing.T) {

	const inParallel = 10 // do inParallel requests at a time
	m, cleanup := OptMappers(inParallel, func(i int) interface{} {
		return &http.Client{
			Timeout: time.Second * 10,
		}
	}, nil)
	defer cleanup()

	list := reducers.NewStringList(len(testUrls))

	q, err := Parallel(list.Value(), func(client interface{}, url interface{}) interface{} {
		return perform(client.(*http.Client), url.(string))
	}, list.Reducer(), list.Then(), m)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range testUrls {
		q <- "https://" + v // turn into HTTPS requests
	}
	close(q)

	all, err := list.Get()
	if err != nil {
		t.Fatal(err)
	}

	if l := len(all); len(testUrls) != l {
		t.Error("Unexpected length", l)
	}

	t.Log(all)
}
