## 启动方式

程序启动入口: productready/cmd/main/main.go

```
# CMD ./main -c ${NODES} -i 192.168.1.1 -p 13890 -d /data/raft init
# CMD ./main  -i 192.168.1.1 -p 13890  -d /data/raft join

# Usage:
#  /main [flags] <init|join>
#
#Flags:
#  -c, --cluster-item stringArray   add cluster node <ip:port> | <ip:port,ip:port...>
#  -d, --data-dir string            set data dir (default "./data")
#  -i, --discover-address string    address exported to others (default "auto")
#  -h, --help                       help
#  -s, --silent                     set log level to error.
#  -t, --try-run                    dump config but not serve.
#  -v, --verbose count              set log level [ 0=warn | 1=info | 2=debug ].
#  -V, --version                    show version and build info.

# Examples:
# CMD /main -c "1.2.3.4:13890,1.2.3.5:13890" -i "1.2.3.4" -p 13890 init
# CMD /main -c "1.2.3.4:13890" -c '1.2.3.5'-i "1.2.3.4" -p 13890 init
# CMD /main -i "1.2.3.6" -p 13890 join
```