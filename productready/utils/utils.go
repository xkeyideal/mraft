package utils

import (
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func Addr2RaftNodeID(addr string) uint64 {
	s := strings.Split(addr, ":")
	bits := strings.Split(s[0], ".")

	b0, _ := strconv.Atoi(bits[0])
	b1, _ := strconv.Atoi(bits[1])
	b2, _ := strconv.Atoi(bits[2])
	b3, _ := strconv.Atoi(bits[3])

	var sum uint64

	sum += uint64(b0) << 24
	sum += uint64(b1) << 16
	sum += uint64(b2) << 8
	sum += uint64(b3)

	port, _ := strconv.Atoi(s[1])

	sum = sum<<16 + uint64(port)

	return sum
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
