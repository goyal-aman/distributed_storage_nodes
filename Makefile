seed:
	go run nodes/main.go -port=7770 -eokr=18446744073709551615 -host=http://0.0.0.0:7770 -nodeid=a -replicacount=2
node1:
	go run nodes/main.go -port=7771 -eokr=14757395258967642112 -host=http://0.0.0.0:7771 -nodeid=b -seed=http://0.0.0.0:7770
node2:
	go run nodes/main.go -port=7772 -eokr=11068046444225732608 -host=http://0.0.0.0:7772 -nodeid=c -seed=http://0.0.0.0:7770
node3:
	go run nodes/main.go -port=7773 -eokr=7378697629483821056 -host=http://0.0.0.0:7773 -nodeid=d -seed=http://0.0.0.0:7770
node4:
	go run nodes/main.go -port=7774 -eokr=3689348814741910528 -host=http://0.0.0.0:7774 -nodeid=e -seed=http://0.0.0.0:7770
test:
	go test -v ./...
resetlog:
	rm -rf ./commitlog_logpath/*.log
# no longer needed, cluster is removed
# node1:
# 	go run nodes/main.go -port=7771
# node2:
# 	go run nodes/main.go -port=7772
# node3:
# 	go run nodes/main.go -port=7773
# start-cluster:
# 	go run cluster/main.go -ihost=http://0.0.0.0:7771