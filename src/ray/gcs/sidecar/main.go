package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var (
	isLeader int32 // 0 for false, 1 for true
)

func main() {
	var leaseName string
	var leaseNamespace string
	var port int

	flag.StringVar(&leaseName, "lease-name", "ray-gcs-leader-lock", "The name of the lease for leader election.")
	flag.StringVar(&leaseNamespace, "lease-namespace", "default", "The namespace of the lease.")
	flag.IntVar(&port, "port", 4040, "The port to expose HTTP status endpoint.")
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	if hostname == "" {
		hostname = "localhost"
	}

	log.Printf("Starting sidecar with identity: %s", hostname)

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("Failed to get in-cluster config: %v", err)
		// For local testing, we might want to fallback to flags or kubeconfig,
		// but in production it must be in-cluster.
		log.Fatalf("Failed to get in-cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create clientset: %v", err)
	}

	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			leaderStatus := false
			if atomic.LoadInt32(&isLeader) == 1 {
				leaderStatus = true
			}
			json.NewEncoder(w).Encode(map[string]bool{"is_leader": leaderStatus})
		})
		log.Printf("Starting HTTP server on port %d", port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: leaseNamespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: hostname,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		failures := 0
		const maxFailures = 3

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				conn, err := net.DialTimeout("tcp", "localhost:6379", 1*time.Second)
				if err != nil {
					log.Printf("GCS health check failed: %v", err)
					// Only fail if we are currently the leader!
					if atomic.LoadInt32(&isLeader) == 1 {
						failures++
						if failures >= maxFailures {
							log.Printf("GCS failed health check %d times, releasing lease!", failures)
							cancel()
							return
						}
					}
				} else {
					conn.Close()
					failures = 0 // Reset failures on success
				}
			}
		}
	}()

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Println("Became leader")
				atomic.StoreInt32(&isLeader, 1)
			},
			OnStoppedLeading: func() {
				log.Println("Lost leadership")
				atomic.StoreInt32(&isLeader, 0)
			},
			OnNewLeader: func(identity string) {
				log.Printf("New leader elected: %s", identity)
			},
		},
	})
}
