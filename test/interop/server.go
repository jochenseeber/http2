// An HTTP/2 server for the client interoperability tests: h2c with prior
// knowledge and TLS with ALPN h2 (self-signed certificate made at start).
//
//	go run . -h2c :9080 -tls :9443
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"time"
)

func main() {
	h2cAddr := flag.String("h2c", ":9080", "h2c (prior knowledge) address")
	tlsAddr := flag.String("tls", ":9443", "TLS address")
	flag.Parse()

	mux := http.NewServeMux()

	// echoes the request body
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	})

	// 40 lines, one every 50 ms, each flushed
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/plain")
		for i := 0; i < 40; i++ {
			fmt.Fprintf(w, "chunk %02d\n", i)
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	})

	// ?size= bytes (32 MiB by default), byte i of each 64 KiB block is i % 251
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		size := 32 << 20
		if s := r.URL.Query().Get("size"); s != "" {
			size, _ = strconv.Atoi(s)
		}
		w.Header().Set("content-length", strconv.Itoa(size))
		if r.Method == http.MethodHead {
			return
		}
		block := make([]byte, 64<<10)
		for i := range block {
			block[i] = byte(i % 251)
		}
		for left := size; left > 0; {
			n := min(left, len(block))
			if _, err := w.Write(block[:n]); err != nil {
				return
			}
			left -= n
		}
	})

	// the request as JSON, then an x-trailer trailer
	mux.HandleFunc("/headers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Header().Set("trailer", "x-trailer")
		json.NewEncoder(w).Encode(map[string]any{
			"method": r.Method, "proto": r.Proto, "host": r.Host, "uri": r.RequestURI,
			"headers": r.Header, "content_length": r.ContentLength,
		})
		w.Header().Set("x-trailer", "done")
	})

	// partial content, then RST_STREAM(INTERNAL_ERROR)
	mux.HandleFunc("/abort", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})

	// answers without reading the request body
	mux.HandleFunc("/noread", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	})

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	h2c := &http.Server{Addr: *h2cAddr, Handler: mux, Protocols: protocols}

	tlsServer := &http.Server{Addr: *tlsAddr, Handler: mux, TLSConfig: &tls.Config{Certificates: []tls.Certificate{selfSigned()}}}
	go func() { log.Fatal(tlsServer.ListenAndServeTLS("", "")) }()

	log.Printf("listening: h2c %s, tls %s", *h2cAddr, *tlsAddr)
	log.Fatal(h2c.ListenAndServe())
}

func selfSigned() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
