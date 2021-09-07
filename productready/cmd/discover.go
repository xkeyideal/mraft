package cmd

import (
	"errors"
	"net"
	"sort"
)

func getIP(str string) (string, error) {
	if str == "auto" {
		return getDiscoverIPAuto()
	} else if ip := net.ParseIP(str); ip != nil {
		return ip.String(), nil
	} else if isInterfaceName(str) {
		return getAddressFromInterface(str)
	} else {
		return checkHost(str)
	}
}

func getDiscoverIPAuto() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil {
			continue
		}
		if ip.IsGlobalUnicast() {
			return ip.String(), nil
		}
	}
	return "", errors.New("cannot find self IP")
}

func getAddressFromInterface(interfaceName string) (string, error) {
	iface, err := net.InterfaceByName(interfaceName)
	if err != nil {
		return "", err
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}
	sort.Slice(addrs, func(i, j int) bool {
		return getIPVersion(addrs[i], 7) < getIPVersion(addrs[j], 7)
	})
	for _, item := range addrs {
		addr, ok := item.(*net.IPNet)
		if !ok {
			continue
		}
		if addr.IP.IsGlobalUnicast() {
			return addr.IP.String(), nil
		}
	}
	return "", errors.New("cannot get IP from interface for discover")
}

// ipv4 return 4
// ipv6 return 6
// other return Default
func getIPVersion(addr net.Addr, Default uint) uint {
	if v, ok := addr.(*net.IPNet); ok {
		if v.IP.To4() == nil {
			return 6
		} else {
			return 4
		}
	}
	return Default
}

func isInterfaceName(ifName string) bool {
	ifs, err := net.Interfaces()
	if err != nil {
		panic("cannot read interface list")
	}
	for _, name := range ifs {
		if ifName == name.Name {
			return true
		}
	}
	return false
}
func getInterfaceNames() []string {
	ifs, err := net.Interfaces()
	result := make([]string, 0, len(ifs))
	if err != nil {
		panic("cannot read interface list")
	}
	for _, name := range ifs {
		result = append(result, name.Name)
	}
	return result
}

func checkHost(str string) (string, error) {
	address, err := net.ResolveIPAddr("ip", str)
	if err != nil {
		return "", err
	}
	if !address.IP.IsGlobalUnicast() {
		return "", errors.New("must use global unicast address")
	}
	return str, nil
}
