// Package rrd provides a client which can talk to rrdtool's rrdcached
// It supports all known commands and uses native golang types where
// appropriate.
package rrd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// DefaultPort is the default rrdcached port.
	DefaultPort = 42217
)

var (
	respRe = regexp.MustCompile(`^(-?\d+)\s+(.*)$`)

	// DefaultTimeout is the default read / write / dial timeout for Clients.
	DefaultTimeout = time.Second * 10
)

// Client is a rrdcached client.
type Client struct {
	conn    net.Conn
	addr    string
	network string
	timeout time.Duration
	scanner *bufio.Scanner

	m sync.Mutex
}

// Timeout sets read / write / dial timeout for a rrdcached Client.
func Timeout(timeout time.Duration) func(*Client) error {
	return func(c *Client) error {
		c.timeout = timeout
		return nil
	}
}

// Unix sets the client to use a unix socket.
func Unix(c *Client) error {
	c.network = "unix"
	return nil
}

// NewClient returns a new rrdcached client connected to addr.
// By default addr is treated as a TCP address to use UNIX sockets pass Unix as an option.
// If addr for a TCP address doesn't include a port the DefaultPort will be used.
func NewClient(addr string, options ...func(c *Client) error) (*Client, error) {
	c := &Client{timeout: DefaultTimeout, network: "tcp", addr: addr}
	for _, f := range options {
		if f == nil {
			return nil, ErrNilOption
		}
		if err := f(c); err != nil {
			return nil, err
		}
	}
	if c.network == "tcp" {
		if !strings.Contains(c.addr, ":") {
			c.addr = fmt.Sprintf("%v:%v", c.addr, DefaultPort)
		}
	}
	err := c.initConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to establish initial connection: %w", err)
	}
	return c, nil
}

func (c *Client) initConnection() error {
	var err error
	if c.conn, err = net.DialTimeout(c.network, c.addr, c.timeout); err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	c.scanner = bufio.NewScanner(bufio.NewReader(c.conn))
	c.scanner.Split(bufio.ScanLines)

	return nil
}

// setDeadline updates the deadline on the connection based on the clients configured timeout.
func (c *Client) setDeadline() error {
	return c.conn.SetDeadline(time.Now().Add(c.timeout))
}

// Exec executes cmd on the server and returns the response.
func (c *Client) Exec(cmd string) ([]string, error) {
	return c.ExecCmd(NewCmd(cmd))
}

// ExecCmd executes cmd on the server and returns the response.
func (c *Client) ExecCmd(cmd *Cmd) ([]string, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if err := c.setDeadline(); err != nil {
		return nil, err
	}

	for {
		if _, err := c.conn.Write([]byte(cmd.String())); err != nil {
			if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
				fmt.Printf("write to connection caused [%v]; trying to reestablish connection...\n", err)
				err2 := c.initConnection()
				if err2 != nil {
					return nil, fmt.Errorf("failed to write (%s) and failed to reestablish: %w", err.Error(), err2)
				}
				continue
			}
			return nil, fmt.Errorf("failed to write: %w", err)
		}
		break
	}
	fmt.Printf("rrdcached command: [%s]\n", strings.TrimSpace(cmd.String()))

	if err := c.setDeadline(); err != nil {
		return nil, err
	}

	if !c.scanner.Scan() {
		return nil, fmt.Errorf("scan error: %w", c.scanErr())
	}

	l := c.scanner.Text()
	matches := respRe.FindStringSubmatch(l)
	if len(matches) != 3 {
		return nil, fmt.Errorf("not 3 matches: '%s'", l)
	}

	cnt, err := strconv.Atoi(matches[1])
	if err != nil {
		// This should be impossible given the regexp matched.
		return nil, fmt.Errorf("failed to convert to int '%s': %w", matches[1], err)
	}

	switch {
	case cnt < 0:
		// rrdcached reported an error.
		return nil, NewError(cnt, matches[2])
	case cnt == 0:
		// message is the line e.g. first.
		return []string{matches[2]}, nil
	}

	if err := c.setDeadline(); err != nil {
		return nil, err
	}
	lines := make([]string, 0, cnt)
	for len(lines) < cnt && c.scanner.Scan() {
		lines = append(lines, c.scanner.Text())
		if err := c.setDeadline(); err != nil {
			return nil, err
		}
	}

	if len(lines) != cnt {
		// Short response.
		return nil, c.scanErr()
	}

	return lines, nil
}

// Close closes the connection to the server.
func (c *Client) Close() error {
	errD := c.setDeadline()
	_, errW := c.conn.Write([]byte("quit"))
	err := c.conn.Close()
	if err != nil {
		return err
	} else if errD != nil {
		return errD
	}

	return errW
}

// scanError returns the error from the scanner if non-nil,
// io.ErrUnexpectedEOF otherwise.
func (c *Client) scanErr() error {
	if err := c.scanner.Err(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}
