package emarsysconfig

import (
	"bytes"
	"crypto/sha1"
	b64 "encoding/base64"
	"encoding/hex"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	yaml "gopkg.in/yaml.v2"
)

// EmarsysConf struct defines how config.yaml should be built
type EmarsysConfig struct {
	User   string `yaml:"user"`
	Secret string `yaml:"secret"`
}

// ReadYamlEmarsysConf reads the configuration of the email to send from emarsys_config.yaml
func (emarsysConfig *EmarsysConfig) ReadYamlEmarsysConfig() *EmarsysConfig {

	yamlFile, err := ioutil.ReadFile("emarsys_config.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, emarsysConfig)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return emarsysConfig
}

// Send an HTTP request to the Emarsys API
func (emarsysConfig EmarsysConfig) Send(method string, path string, body string) (string, string) {
	url := "https://api.emarsys.net/api/v2/" + path
	var timestamp = time.Now().Format(time.RFC3339)
	nonce := generateRandString(36)
	text := (nonce + timestamp + emarsysConfig.Secret)
	h := sha1.New()
	h.Write([]byte(text))
	sha1 := hex.EncodeToString(h.Sum(nil))
	passwordDigest := b64.StdEncoding.EncodeToString([]byte(sha1))

	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	header := string(` UsernameToken Username="` + emarsysConfig.User + `",PasswordDigest="` + passwordDigest + `",Nonce="` + nonce + `",Created="` + timestamp + `"`)

	req.Header.Set("X-WSSE", header)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	status := resp.Status
	responseBody, _ := ioutil.ReadAll(resp.Body)
	return status, string(responseBody)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func generateRandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
