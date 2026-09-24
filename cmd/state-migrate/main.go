package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	name := os.Getenv("HA_CLUSTER_NAME")
	if name == "" {
		name = "mysql-ha"
	}
	source, err := clientv3.New(clientv3.Config{Endpoints: split(os.Getenv("HA_MIGRATE_SOURCE_ENDPOINTS")), DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer source.Close()
	destination, err := clientv3.New(clientv3.Config{Endpoints: split(os.Getenv("HA_MIGRATE_DEST_ENDPOINTS")), DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer destination.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := "/ha/" + name
	sourceResponse, err := source.Get(ctx, prefix+"/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("read source: %v", err)
	}
	var selected []*mvccpb.KeyValue
	for _, kv := range sourceResponse.Kvs {
		key := string(kv.Key)
		if key == prefix+"/cluster" || strings.HasPrefix(key, prefix+"/tasks/") {
			selected = append(selected, kv)
		}
	}
	if len(selected) == 0 {
		log.Fatal("source has no cluster state or task history")
	}
	destinationResponse, err := destination.Get(ctx, prefix+"/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("read destination: %v", err)
	}
	for _, kv := range destinationResponse.Kvs {
		key := string(kv.Key)
		if key == prefix+"/cluster" || strings.HasPrefix(key, prefix+"/tasks/") {
			log.Fatalf("destination already contains HA state at %s", key)
		}
	}
	hasCluster := false
	for _, kv := range selected {
		if string(kv.Key) == prefix+"/cluster" {
			hasCluster = true
		}
	}
	if !hasCluster {
		log.Fatal("source cluster state key is missing")
	}
	for _, kv := range selected {
		if _, err := destination.Put(ctx, string(kv.Key), string(kv.Value)); err != nil {
			log.Fatalf("write destination key %s: %v", kv.Key, err)
		}
	}
	verify, err := destination.Get(ctx, prefix+"/", clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("verify destination: %v", err)
	}
	actual := make(map[string]string, len(verify.Kvs))
	for _, kv := range verify.Kvs {
		actual[string(kv.Key)] = string(kv.Value)
	}
	for _, kv := range selected {
		if actual[string(kv.Key)] != string(kv.Value) {
			log.Fatalf("destination verification failed at %s", kv.Key)
		}
	}
	fmt.Printf("migrated and verified %d HA state/task keys for cluster %s\n", len(selected), name)
}

func split(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	if len(result) == 0 {
		log.Fatal(errors.New("migration endpoints are required"))
	}
	return result
}
