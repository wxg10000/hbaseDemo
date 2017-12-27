Thrift介绍与应用
—hbase的thrift接口

要使用Hbase的thrift接口，必须将它的服务启动，命令行为：
hbase-deamon.sh start thrift2

thrift默认的监听端口是9090，可以用netstat -nl | grep 9090看看该端口是否有服务。