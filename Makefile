seed:
	go run nodes/main.go -port=7770 -eokr=18446744073709551615 -host=http://0.0.0.0:7770 -replicacount=3
node1:
	go run nodes/main.go -port=7771 -eokr=13835058055282163712 -host=http://0.0.0.0:7771 -seed=http://0.0.0.0:7770
node2:
	go run nodes/main.go -port=7772 -eokr=9223372036854775808 -host=http://0.0.0.0:7772 -seed=http://0.0.0.0:7770
node3:
	go run nodes/main.go -port=7773 -eokr=4611686018427387904 -host=http://0.0.0.0:7773 -seed=http://0.0.0.0:7770

# no longer needed, cluster is removed
# node1:
# 	go run nodes/main.go -port=7771
# node2:
# 	go run nodes/main.go -port=7772
# node3:
# 	go run nodes/main.go -port=7773
# start-cluster:
# 	go run cluster/main.go -ihost=http://0.0.0.0:7771