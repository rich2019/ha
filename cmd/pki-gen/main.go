package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	out := flag.String("out", "", "output directory for private PKI material")
	agents := flag.String("agents", "", "comma-separated MySQL agent IPs")
	controllers := flag.String("controllers", "", "comma-separated controller IDs")
	flag.Parse()
	if *out == "" {
		fail("-out is required")
	}
	agentIPs := split(*agents)
	controllerIDs := split(*controllers)
	if len(agentIPs) == 0 || len(controllerIDs) == 0 {
		fail("-agents and -controllers are required")
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		fail(err.Error())
	}
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "mysql-ha-agent-ca"}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(5, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		fail(err.Error())
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		fail(err.Error())
	}
	write(filepath.Join(*out, "ca.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0644)
	writeKey(filepath.Join(*out, "ca.key"), caKey)
	for _, rawIP := range agentIPs {
		ip := net.ParseIP(rawIP)
		ips, dnsNames := []net.IP{}, []string{}
		if ip != nil {
			ips = append(ips, ip)
		} else if validDNSName(rawIP) {
			dnsNames = append(dnsNames, rawIP)
		} else {
			fail("invalid agent IP or DNS name: " + rawIP)
		}
		fileName := "agent-" + strings.NewReplacer(".", "-", ":", "-").Replace(rawIP)
		if err := leaf(*out, caCert, caKey, fileName, ips, dnsNames, x509.ExtKeyUsageServerAuth); err != nil {
			fail(err.Error())
		}
	}
	for _, id := range controllerIDs {
		if strings.ContainsAny(id, `/\\`) {
			fail("controller ID may not contain path separators")
		}
		if err := leaf(*out, caCert, caKey, id, nil, nil, x509.ExtKeyUsageClientAuth); err != nil {
			fail(err.Error())
		}
	}
	fmt.Printf("generated CA and %d agent server plus %d controller client certificates in %s\n", len(agentIPs), len(controllerIDs), *out)
}

func leaf(out string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, name string, ips []net.IP, dnsNames []string, usage x509.ExtKeyUsage) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	template := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: name}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(2, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: ips, DNSNames: dnsNames}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, name+".crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		return err
	}
	return writeKey(filepath.Join(out, name+".key"), key)
}

func writeKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0600)
}

func write(path string, data []byte, mode os.FileMode) {
	if err := os.WriteFile(path, data, mode); err != nil {
		fail(err.Error())
	}
}
func split(value string) []string {
	out := []string{}
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func validDNSName(value string) bool {
	if value == "" || strings.Contains(value, "..") {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}
func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		fail(err.Error())
	}
	return n
}
func fail(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
