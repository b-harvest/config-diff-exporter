package main

import (
    "bytes"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "time"
    "sync"
    "strings"
    
    "gopkg.in/yaml.v3"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
    PrometheusListenPort int      `yaml:"prometheusListenPort"`
    Paths                []string `yaml:"paths"`
    S3Bucket             string   `yaml:"s3Bucket"`
    ChainDir             string   `yaml:"chainDir"`
    AWSRegion            string   `yaml:"awsRegion"`
}

var (
    uploadTS = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "config_exporter",
            Name:      "s3_lastmodified_timestamp",
            Help:      "S3 object LastModified timestamp (Unix seconds) via GetObject",
        },
        []string{"bucket", "key"},
    )
    sinceUpload= prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "config_exporter",
            Name:      "time_since_upload",
            Help:      "successful upload event",
        },
        []string{"bucket", "key"},
    )
    lastUploadMu sync.Mutex
    lastUpload   = map[string]time.Time{} // map["bucket|key"] = time

)

func init() {
    prometheus.MustRegister(uploadTS)
    prometheus.MustRegister(sinceUpload)

    go func() {
        ticker := time.NewTicker(time.Second)
        for now := range ticker.C {
            lastUploadMu.Lock()
            for bk, t0 := range lastUpload {
                parts := strings.SplitN(bk, "|", 2)
                bucket, key := parts[0], parts[1]
                age := now.Sub(t0).Seconds()
                sinceUpload.WithLabelValues(bucket, key).Set(age)
            }
            lastUploadMu.Unlock()
        }
    }()
}

func main() {
    // 1) Load config.yaml
    raw, err := os.ReadFile("config.yaml")
    if err != nil {
        log.Fatalf("cannot read config.yaml: %v", err)
    }
    var cfg Config
    if err := yaml.Unmarshal(raw, &cfg); err != nil {
        log.Fatalf("cannot parse config.yaml: %v", err)
    }

    // 2) AWS session (SDK v1)
    sess, err := session.NewSessionWithOptions(session.Options{
		Profile: "config-diff",             
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(cfg.AWSRegion),
		},
	})

    if err != nil {
        log.Fatalf("failed to create AWS session: %v", err)
    }
    s3cli := s3.New(sess)

    // 4) Scheduler: next midnight or noon
    go func() {
        for {
            runAllUploads(s3cli, &cfg)

            next := computeNextRun(time.Now())
            log.Printf("next upload at %s", next.Format(time.RFC3339))
            time.Sleep(time.Until(next))
        }
    }()

    // 5) Prometheus endpoint
    http.Handle("/metrics", promhttp.Handler())
    addr := fmt.Sprintf(":%d", cfg.PrometheusListenPort)
    log.Printf("listening on %s", addr)
    log.Fatal(http.ListenAndServe(addr, nil))
}

// runAllUploads uploads all tomlPaths and servicePaths
// func runAllUploads(cli *s3.S3, cfg *Config) {
//     for _, localPath := range cfg.Paths {
//         uploadSingle(cli, cfg, localPath)
//     }
// }
func runAllUploads(cli *s3.S3, cfg *Config) {
    for _, p := range cfg.Paths {
        fi, err := os.Stat(p)
        if err != nil {
            log.Printf("[ERROR] stat %s: %v", p, err)
            continue
        }
        if fi.IsDir() {
            entries, err := os.ReadDir(p)
            if err != nil {
                log.Printf("[ERROR] read dir %s: %v", p, err)
                continue
            }
            for _, ent := range entries {
                local := filepath.Join(p, ent.Name())
                uploadSingle(cli, cfg, local)
            }
        } else {
            uploadSingle(cli, cfg, p)
        }
    }
}



// uploadSingle does PutObject then GetObject(range=0-0) to get LastModified
func uploadSingle(cli *s3.S3, cfg *Config, localPath string) {

    data, err := os.ReadFile(localPath)
    if err != nil {
        log.Printf("[ERROR] read %s: %v", localPath, err)
        return
    }

    bucket := cfg.S3Bucket

    key := filepath.Join(cfg.ChainDir, filepath.Base(localPath))
    s3uri := fmt.Sprintf("s3://%s/%s", cfg.S3Bucket, key)
    log.Printf("uploading %s → %s", localPath, s3uri)

    now := time.Now()
    lastUploadMu.Lock()
    lastUpload[bucket+"|"+key] = now
    lastUploadMu.Unlock()
    // 즉시 age 를 0으로 찍어준다
    sinceUpload.WithLabelValues(bucket, key).Set(0)

    // PutObject
    if _, err := cli.PutObject(&s3.PutObjectInput{
        Bucket: aws.String(cfg.S3Bucket),
        Key:    aws.String(key),
        Body:   bytes.NewReader(data),
    }); err != nil {
        log.Printf("[ERROR] PutObject %s: %v", key, err)
        return
    }

    // GetObject with Range=0-0 to fetch LastModified header
    resp, err := cli.GetObject(&s3.GetObjectInput{
        Bucket: aws.String(cfg.S3Bucket),
        Key:    aws.String(key),
        Range:  aws.String("bytes=0-0"),
    })
    if err != nil {
        log.Printf("[WARN] GetObject %s: %v", key, err)
        return
    }
    // discard body
    io.Copy(io.Discard, resp.Body)
    resp.Body.Close()

    if resp.LastModified != nil {
        ts := float64(resp.LastModified.Unix())
        uploadTS.WithLabelValues(cfg.S3Bucket, key).Set(ts)
        log.Printf("Recorded LastModified for %s: %s",
            key, resp.LastModified.UTC().Format(time.RFC3339))
    } 
    // else {
    //     log.Printf("[WARN] no LastModified for %s", key)
    // }
}

// computeNextRun finds next 00:00 or 12:00 after now
func computeNextRun(now time.Time) time.Time {
    y, m, d := now.Date()
    loc := now.Location()
    midnight := time.Date(y, m, d, 0, 0, 0, 0, loc)
    noon := time.Date(y, m, d, 12, 0, 0, 0, loc)

    var candidates []time.Time
    if midnight.After(now) {
        candidates = append(candidates, midnight)
    }
    if noon.After(now) {
        candidates = append(candidates, noon)
    }
    if len(candidates) == 0 {
        candidates = append(candidates, midnight.Add(24*time.Hour))
    }
    next := candidates[0]
    for _, t := range candidates {
        if t.Before(next) {
            next = t
        }
    }
    return next
}