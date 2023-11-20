CONFIG_PATH = $(shell pwd)/certs/

.PHONY: gencert
gencert:
	cfssl gencert \
		-initca certs-config/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs-config/ca-config.json \
		-profile=client \
		certs-config/client-csr.json | cfssljson -bare client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs-config/ca-config.json \
		-profile=server \
		certs-config/server-csr.json | cfssljson -bare server
	mv *.pem *.csr ${CONFIG_PATH}
	cfssl gencert \
		-initca certs-config/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs-config/ca-config.json \
		-profile=server \
		certs-config/server-csr.json | cfssljson -bare server
	mv *.pem *.csr ${CONFIG_PATH}

