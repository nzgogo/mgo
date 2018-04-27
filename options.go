package mgo

import (
	"crypto/tls"
)

type Options struct {
	sslMgo bool
	Protocol        string
	TLS *tls.Config
}

type Option func(*Options)

func SSLMgo(ssl bool) Option {
	return func(o *Options) {
		o.sslMgo = ssl
	}
}

func Protocol(p string) Option {
	return func(o *Options) {
		o.Protocol = p
	}
}