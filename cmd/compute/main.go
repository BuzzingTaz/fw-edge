package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pbcompute "github.com/BuzzingTaz/fw-edge-apps/cmd/compute/edge-compute-yolo"
	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"google.golang.org/grpc"
)

var (
	port        = flag.Int("port", 9997, "The server port")
	udpPort     = flag.Int("udp-port", 5000, "The UDP port to dial")
	trackerPort = flag.Int("tracker-port", 5005, "The gRPC tracker server port")
)

var (
	metricsMu       sync.RWMutex
	metricQueue     float64
	metricLat       float64
	completionTimes []time.Time
	receiveTimes    = make(map[uint64]time.Time)
)

// Prevent multiple simultaneous StreamVideo calls fighting for TCP port
var (
	streamVideoMu     sync.Mutex
	streamVideoActive bool
)

func getTemperature() float64 {
	data, err := os.ReadFile("/sys/class/thermal/thermal_zone0/temp")
	if err != nil {
		return 42.0
	}
	tempStr := strings.TrimSpace(string(data))
	tempVal, err := strconv.ParseFloat(tempStr, 64)
	if err != nil {
		return 42.0
	}
	return tempVal / 1000.0
}

func getMemory() float64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 30.0
	}
	var memTotal, memAvailable float64
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			memTotal, _ = strconv.ParseFloat(fields[1], 64)
		}
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			memAvailable, _ = strconv.ParseFloat(fields[1], 64)
		}
	}
	if memTotal == 0 {
		return 30.0
	}
	return ((memTotal - memAvailable) / memTotal) * 100.0
}

func getCPU() float64 {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 15.0
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Scan()
	fields := strings.Fields(scanner.Text())
	var total float64
	for i := 1; i < len(fields); i++ {
		val, _ := strconv.ParseFloat(fields[i], 64)
		total += val
	}
	idle, _ := strconv.ParseFloat(fields[4], 64)
	return (1.0 - (idle / total)) * 100.0
}

func readSysfsFloat(path string) (float64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	val, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func getPower() float64 {
	const (
		ina3221VoltagePath = "/sys/devices/platform/bus@0/c240000.i2c/i2c-1/1-0040/hwmon/hwmon1/in1_input"
		ina3221CurrentPath = "/sys/devices/platform/bus@0/c240000.i2c/i2c-1/1-0040/hwmon/hwmon1/curr1_input"
	)

	voltageMV, vOK := readSysfsFloat(ina3221VoltagePath)
	currentMA, iOK := readSysfsFloat(ina3221CurrentPath)

	if vOK && iOK {
		return (voltageMV * currentMA) / 1_000_000.0
	}

	cpu := getCPU()
	return 5.0 + 5.0*(cpu/100.0)
}

func startMetricsServer(grpcPort int) {
	metricsPort := grpcPort - 1
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metricsMu.Lock()
		q := metricQueue
		lat := metricLat

		// FIXED: 5-second window for more stable throughput
		cutoff := time.Now().Add(-5 * time.Second)
		for len(completionTimes) > 0 && completionTimes[0].Before(cutoff) {
			completionTimes = completionTimes[1:]
		}
		tp := float64(len(completionTimes)) / 5.0  // Average FPS over 5 seconds
		metricsMu.Unlock()

		metrics := map[string]float64{
			"cpu":         getCPU(),
			"memory":      getMemory(),
			"temperature": getTemperature(),
			"queue":       q,
			"latency":     lat,
			"throughput":  tp,
			"power":       getPower(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})
	log.Printf("Metrics HTTP server starting on :%d", metricsPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), nil); err != nil {
		log.Fatalf("Failed to start metrics server: %v", err)
	}
}

type InferenceCache struct {
	mu    sync.RWMutex
	Boxes []*pb.BoundingBox
}

var sharedInferenceCache = &InferenceCache{}

type inferenceServer struct {
	pbcompute.UnimplementedInferenceTrackerServer
}

func (s *inferenceServer) StreamResults(stream pbcompute.InferenceTracker_StreamResultsServer) error {
	log.Println("Python inference client connected to gRPC stream!")

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			log.Println("Python client cleanly closed the gRPC stream.")
			return stream.SendAndClose(&pbcompute.Ack{Received: true})
		}
		if err != nil {
			log.Printf("gRPC stream error (Python likely disconnected): %v", err)
			return err
		}

		var newDetections []*pb.BoundingBox
		log.Printf("Received %d bounding boxes at timestamp %d", len(res.Boxes), res.Timestamp)

		for _, box := range res.Boxes {
			newDetections = append(newDetections, &pb.BoundingBox{
				X:          uint64(box.X),
				Y:          uint64(box.Y),
				Dx:         uint64(box.W),
				Dy:         uint64(box.H),
				Label:      box.ClassLabel,
				Confidence: box.Confidence,
			})
		}

		sharedInferenceCache.mu.Lock()
		sharedInferenceCache.Boxes = newDetections
		sharedInferenceCache.mu.Unlock()
	}
}

type computeStreamServer struct {
	pb.UnimplementedComputeStreamServer
}

func (*computeStreamServer) StreamVideo(stream grpc.BidiStreamingServer[pb.RTPPacket, pb.InferenceResult]) error {
	streamVideoMu.Lock()
	if streamVideoActive {
		streamVideoMu.Unlock()
		return fmt.Errorf("another StreamVideo already active")
	}
	streamVideoActive = true
	streamVideoMu.Unlock()

	defer func() {
		streamVideoMu.Lock()
		streamVideoActive = false
		streamVideoMu.Unlock()
	}()

	log.Println("========== StreamVideo ENTERED ==========")

	// Connect to Python TCP server with retries
	var conn net.Conn
	var err error
	for attempts := 0; attempts < 30; attempts++ {
		conn, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", *udpPort))
		if err == nil {
			break
		}
		log.Printf("Waiting for Python TCP server on port %d... (attempt %d/30)", *udpPort, attempts+1)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Printf("Failed to connect to Python TCP server after 30 attempts: %v", err)
		return fmt.Errorf("python not available: %w", err)
	}
	defer conn.Close()
	log.Printf("TCP connection established to localhost:%d", *udpPort)

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	errChan := make(chan error, 2)
	var wg sync.WaitGroup

	frameCount := 0
	var latestTimestamp uint64
	var tsMutex sync.RWMutex

// Receiver goroutine: receives JPEG from scheduler, forwards to Python
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			log.Println("Waiting for frame from scheduler...")
			frame, err := stream.Recv()

			if err == io.EOF {
				log.Println("Scheduler closed stream")
				errChan <- nil
				return
			}
			if err != nil {
				log.Printf("Recv error: %v", err)
				errChan <- err
				return
			}

			log.Printf("Received frame from scheduler: %d bytes", len(frame.Data))

			// Send JPEG size header (4 bytes, big-endian) then data
			sizeBuf := make([]byte, 4)
			sizeBuf[0] = byte(len(frame.Data) >> 24)
			sizeBuf[1] = byte(len(frame.Data) >> 16)
			sizeBuf[2] = byte(len(frame.Data) >> 8)
			sizeBuf[3] = byte(len(frame.Data))

			_, err = conn.Write(sizeBuf)
			if err != nil {
				log.Printf("TCP size write failed: %v", err)
				errChan <- err
				return
			}

			_, err = conn.Write(frame.Data)
			if err != nil {
				log.Printf("TCP data write failed: %v", err)
				errChan <- err
				return
			}

			log.Printf("Forwarded JPEG frame: %d bytes", len(frame.Data))

			tsMutex.Lock()
			frameCount++
			latestTimestamp = uint64(time.Now().UnixMilli())
			tsMutex.Unlock()

			metricsMu.Lock()
			metricQueue++
			receiveTimes[latestTimestamp] = time.Now()
			metricsMu.Unlock()
		}
	}()

// Sender goroutine: sends inference results back to scheduler
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()

		log.Println("=== SENDER GOROUTINE STARTED ===")
		ticker := time.NewTicker(33 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case <-ticker.C:
				log.Println("TICK")

				// FIXED: Only clear boxes if we actually read them
				sharedInferenceCache.mu.Lock()
				currentBoxes := sharedInferenceCache.Boxes
				if currentBoxes != nil {
					sharedInferenceCache.Boxes = nil
				}
				sharedInferenceCache.mu.Unlock()

				if currentBoxes == nil {
					continue
				}
				log.Println("BEFORE SEND")

				tsMutex.RLock()
				ts := latestTimestamp
				tsMutex.RUnlock()

				err := stream.Send(&pb.InferenceResult{
					Timestamp:        ts,
					ProcessingStatus: 0,
					Detections:       currentBoxes,
				})
				log.Println("AFTER SEND", ts)

				if err != nil {
					log.Printf("Error sending inference data to client: %v", err)
					errChan <- err
					return
				}

				metricsMu.Lock()
				if recvTime, ok := receiveTimes[ts]; ok {
					lat := float64(time.Since(recvTime).Milliseconds())
					if metricLat == 0 {
						metricLat = lat
					} else {
						metricLat = 0.9*metricLat + 0.1*lat
					}
					delete(receiveTimes, ts)
				}
				if metricQueue > 0 {
					metricQueue--
				}
				log.Println("UPDATED METRICS")
				completionTimes = append(completionTimes, time.Now())
				metricsMu.Unlock()
			}
		}
	}()

	err = <-errChan
	cancel()
	wg.Wait()
	return err
}

func startComputeGRPCServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *trackerPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %d: %v", *trackerPort, err)
	}

	s := grpc.NewServer()
	pbcompute.RegisterInferenceTrackerServer(s, &inferenceServer{})

	log.Printf("Go compute gRPC server listening on :%d", *trackerPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

func startSchedulerGRPCServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterComputeStreamServer(s, &computeStreamServer{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {
	flag.Parse()

	go startComputeGRPCServer()
	go startSchedulerGRPCServer()
	go startMetricsServer(*port)

	select {}
}