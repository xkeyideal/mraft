package utils

import (
	"fmt"
	"net"
	"strconv"

	"github.com/gin-gonic/gin"
)

func Addr2RaftNodeID(addr string) (uint64, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, fmt.Errorf("invalid raft address %q: %w", addr, err)
	}

	ip := net.ParseIP(host)
	if ip == nil || ip.To4() == nil {
		return 0, fmt.Errorf("raft address %q must be IPv4", addr)
	}

	ipv4 := ip.To4()
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return 0, fmt.Errorf("invalid port in raft address %q", addr)
	}

	var sum uint64
	sum += uint64(ipv4[0]) << 24
	sum += uint64(ipv4[1]) << 16
	sum += uint64(ipv4[2]) << 8
	sum += uint64(ipv4[3])
	sum = sum<<16 + uint64(port)

	return sum, nil
}

func SetStrResp(httpCode, code int, msg string, result interface{}, c *gin.Context) {
	m := msg

	if code == 0 {
		c.JSON(httpCode, gin.H{
			"code":   code,
			"msg":    m,
			"result": result,
		})
	} else {
		c.JSON(httpCode, gin.H{
			"code": code,
			"msg":  m,
		})
	}
}
