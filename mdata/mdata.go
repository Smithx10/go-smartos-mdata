package mdata

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/tarm/serial"
)

// transportType defines the connection type for the metadata client
type transportType string

const (
	transportSerial transportType = "serial"
	transportTCP    transportType = "tcp"
	transportUnix   transportType = "unix"
)

// SocketConfig holds configuration for socket connections
type SocketConfig struct {
	Network string        // Network type ("tcp" or "unix")
	Address string        // Address (e.g., "localhost:12345" for TCP, "/var/run/mdata.sock" for Unix)
	Timeout time.Duration // Dial and read timeout (e.g., 5s)
}

// ClientConfig holds configuration for the metadata client
type ClientConfig struct {
	Transport    transportType  // Connection type (serial, tcp, unix)
	SerialConfig *serial.Config // Serial configuration (if Transport == TransportSerial)
	SocketConfig *SocketConfig  // Socket configuration (if Transport == TransportTCP or TransportUnix)
}

// DefaultClientConfig returns a ClientConfig with defaults based on the environment
func DefaultClientConfig() ClientConfig {
	config := ClientConfig{}

	// Check for SmartOS zone Unix sockets
	if _, err := os.Stat("/native/.zonecontrol/metadata.sock"); err == nil {
		config.Transport = transportUnix
		config.SocketConfig = &SocketConfig{
			Network: "unix",
			Address: "/native/.zonecontrol/metadata.sock", // LX-branded zone
			Timeout: 5 * time.Second,
		}
		// fmt.Printf("Detected LX-branded zone socket: %s\n", config.SocketConfig.Address)
		return config
	}
	if _, err := os.Stat("/.zonecontrol/metadata.sock"); err == nil {
		config.Transport = transportUnix
		config.SocketConfig = &SocketConfig{
			Network: "unix",
			Address: "/.zonecontrol/metadata.sock", // Native SmartOS zone
			Timeout: 5 * time.Second,
		}
		// fmt.Printf("Detected native SmartOS zone socket: %s\n", config.SocketConfig.Address)
		return config
	}

	// Fallback to serial for VM guests (e.g., KVM)
	config.Transport = transportSerial
	config.SerialConfig = &serial.Config{
		Baud:        115200,
		ReadTimeout: 60 * time.Second,
		Size:        8,
		Parity:      serial.ParityNone,
		StopBits:    serial.Stop1,
	}
	// Set default port based on guest OS
	switch runtime.GOOS {
	case "linux":
		config.SerialConfig.Name = "/dev/ttyS1" // Common for SmartOS metadata
	case "windows":
		config.SerialConfig.Name = "COM1" // Typical for Windows
	case "solaris":
		config.SerialConfig.Name = "/dev/ttyb" // Common for SmartOS/Solaris
	default:
		fmt.Printf("Warning: unsupported OS %s, Port field left empty\n", runtime.GOOS)
	}

	// fmt.Printf("Detected VM guest, using serial port: %s\n", config.SerialConfig.Name)
	return config
}

// Conn abstracts the connection interface for serial and socket
type Conn interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	Close() error
	SetReadTimeout(time.Duration) error
}

// netConnWrapper wraps net.Conn to implement SetReadTimeout
type netConnWrapper struct {
	net.Conn
}

// SetReadTimeout implements Conn.SetReadTimeout using SetReadDeadline
func (w *netConnWrapper) SetReadTimeout(timeout time.Duration) error {
	if timeout == 0 {
		return w.SetReadDeadline(time.Time{})
	}
	return w.SetReadDeadline(time.Now().Add(timeout))
}

// MetadataClientImpl implements MetadataClient for serial or socket communication
type MetadataClientImpl struct {
	conn Conn
	rw   *bufio.ReadWriter
}

type MetadataClient interface {
	Get(payload string) (string, error)
	Keys() (string, error)
	Delete(payload string) error
	Put(key, value string) error
	Close() error
}

type serialConnWrapper struct {
	*serial.Port
}

// SetReadTimeout sets the read timeout for the serial port
func (w *serialConnWrapper) SetReadTimeout(timeout time.Duration) error {
	// tarm/serial handles timeout via Config.ReadTimeout, set during OpenPort
	// No-op here as timeout is already set
	return nil
}

// NewMetadataClient creates a new MetadataClient based on the config
func NewMetadataClient(config ClientConfig) (MetadataClient, error) {
	var conn Conn
	var err error

	switch config.Transport {
	case transportSerial:
		if config.SerialConfig == nil {
			return nil, fmt.Errorf("serial config required for serial transport")
		}
		if config.SerialConfig.Name == "" {
			return nil, fmt.Errorf("serial port not specified in config")
		}
		port, err := serial.OpenPort(config.SerialConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to open serial port %s: %w", config.SerialConfig.Name, err)
		}
		conn = &serialConnWrapper{Port: port}
	case transportTCP, transportUnix:
		if config.SocketConfig == nil {
			return nil, fmt.Errorf("socket config required for %s transport", config.Transport)
		}
		dialer := &net.Dialer{Timeout: config.SocketConfig.Timeout}
		var netConn net.Conn
		netConn, err = dialer.Dial(config.SocketConfig.Network, config.SocketConfig.Address)
		if err != nil {
			return nil, fmt.Errorf("failed to dial %s %s: %w", config.SocketConfig.Network, config.SocketConfig.Address, err)
		}
		conn = &netConnWrapper{Conn: netConn}
		if err := conn.SetReadTimeout(config.SocketConfig.Timeout); err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to set read timeout: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported transport: %s", config.Transport)
	}

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	if supported, err := Negotiate(rw); err != nil || !supported {
		conn.Close()
		if err != nil {
			return nil, fmt.Errorf("protocol negotiation failed: %w", err)
		}
		return nil, fmt.Errorf("server does not support Version 2 protocol")
	}
	return &MetadataClientImpl{conn: conn, rw: rw}, nil
}

// Get sends a GET request with the given payload
func (c *MetadataClientImpl) Get(payload string) (string, error) {
	return c.sendRequest("GET", payload)
}

// Keys sends a KEYS request with the given payload
func (c *MetadataClientImpl) Keys() (string, error) {
	return c.sendRequest("KEYS", "")
}

// Delete sends a DELETE request with the given payload
func (c *MetadataClientImpl) Delete(payload string) error {
	if _, err := c.sendRequest("DELETE", payload); err != nil {
		return err
	}
	return nil
}

func (c *MetadataClientImpl) Put(key, value string) error {
	if strings.HasPrefix(key, "sdc:") {
		return fmt.Errorf("cannot update keys in the read-only sdc: namespace")
	}
	// Encode key and value separately with BASE64
	encodedKey := base64.StdEncoding.EncodeToString([]byte(key))
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
	// Concatenate with a space
	concatenated := encodedKey + " " + encodedValue
	if _, err := c.sendRequest("PUT", concatenated); err != nil {
		return err
	}
	return nil
}

// sendRequest sends a request with the given code and payload
func (c *MetadataClientImpl) sendRequest(code, payload string) (string, error) {
	frame, err := newFrameWithString(code, payload)
	if err != nil {
		return "", fmt.Errorf("failed to create frame: %w", err)
	}
	if _, err := c.rw.WriteString(frame.Encode()); err != nil {
		return "", fmt.Errorf("failed to send frame: %w", err)
	}
	if err := c.rw.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush frame: %w", err)
	}
	response, err := c.rw.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}
	respFrame, err := ParseFrame(response)
	if err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}
	if respFrame.Code != "SUCCESS" {
		return "", fmt.Errorf("request failed with code: %s", respFrame.Code)
	}
	return string(respFrame.Payload), nil
}

// Close closes the serial connection
func (c *MetadataClientImpl) Close() error {
	return c.conn.Close()
}

// frame represents a Version 2 protocol frame
type frame struct {
	RequestID    string
	Code         string
	Payload      []byte // Raw payload bytes (not BASE64 encoded)
	BodyLength   int
	BodyChecksum string
}

// Protocol constants
const (
	ProtocolPrefix  = "V2 "
	NegotiationReq  = "NEGOTIATE V2\n"
	NegotiationResp = "V2_OK\n"
	CRCPolynomial   = 0xEDB88320
)

// newFrameWithString creates a new protocol frame with a string payload
func newFrameWithString(code, payload string) (*frame, error) {
	return newFrame(code, []byte(payload))
}

// newFrame creates a new protocol frame
func newFrame(code string, payload []byte) (*frame, error) {
	// Generate random request ID
	randBytes := make([]byte, 4)
	_, err := rand.Read(randBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to generate request ID: %w", err)
	}

	// Format request ID as 8-char zero-padded lowercase hex
	requestID := hex.EncodeToString(randBytes)

	// Create frame
	f := &frame{
		RequestID: requestID,
		Code:      strings.ToUpper(code),
		Payload:   payload,
	}

	// Calculate body length and checksum
	f.updateBodyMetadata()

	return f, nil
}

// updateBodyMetadata calculates body length and checksum
func (f *frame) updateBodyMetadata() {
	body := f.buildBodyString()
	f.BodyLength = len(body)
	f.BodyChecksum = fmt.Sprintf("%08x", crc32.Checksum([]byte(body), crc32.MakeTable(CRCPolynomial)))
}

// buildBodyString constructs the body string for checksum calculation
func (f *frame) buildBodyString() string {
	parts := []string{f.RequestID, f.Code}
	if len(f.Payload) > 0 {
		parts = append(parts, base64.StdEncoding.EncodeToString(f.Payload))
	}
	return strings.Join(parts, " ")
}

// Encode converts frame to wire format
func (f *frame) Encode() string {
	return fmt.Sprintf("%s%d %s %s\n",
		ProtocolPrefix,
		f.BodyLength,
		f.BodyChecksum,
		f.buildBodyString())
}

// Negotiate performs V2 protocol negotiation
func Negotiate(conn *bufio.ReadWriter) (bool, error) {
	// Send negotiation request
	if _, err := conn.WriteString(NegotiationReq); err != nil {
		return false, fmt.Errorf("failed to send negotiation: %w", err)
	}
	if err := conn.Flush(); err != nil {
		return false, fmt.Errorf("failed to flush negotiation: %w", err)
	}

	// Read response
	resp, err := conn.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("failed to read negotiation response: %w", err)
	}

	return resp == NegotiationResp, nil
}

// ParseFrame parses a wire format frame
func ParseFrame(data string) (*frame, error) {
	if !strings.HasPrefix(data, ProtocolPrefix) {
		return nil, fmt.Errorf("invalid frame prefix")
	}

	// Remove prefix and trailing newline
	trimmed := strings.TrimPrefix(strings.TrimSuffix(data, "\n"), ProtocolPrefix)

	// Split into fields
	parts := strings.Split(trimmed, " ")
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid frame format")
	}

	// Parse body length
	var bodyLength int
	if _, err := fmt.Sscanf(parts[0], "%d", &bodyLength); err != nil {
		return nil, fmt.Errorf("invalid body length: %w", err)
	}

	// Validate checksum format
	checksum := parts[1]
	if len(checksum) != 8 {
		return nil, fmt.Errorf("invalid checksum format")
	}

	// Parse body fields
	bodyParts := parts[2:]
	if len(bodyParts) < 2 {
		return nil, fmt.Errorf("invalid body format")
	}

	f := &frame{
		BodyLength:   bodyLength,
		BodyChecksum: checksum,
		RequestID:    bodyParts[0],
		Code:         bodyParts[1],
	}

	// Parse payload if present
	if len(bodyParts) > 2 {
		payload, err := base64.StdEncoding.DecodeString(bodyParts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid payload encoding: %w", err)
		}
		f.Payload = payload
	}

	// Verify checksum
	if actualChecksum := fmt.Sprintf("%08x", crc32.Checksum([]byte(f.buildBodyString()), crc32.MakeTable(CRCPolynomial))); actualChecksum != checksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return f, nil
}
