node1:
	go run nodes/main.go -port=7771
node2:
	go run nodes/main.go -port=7772
node3:
	go run nodes/main.go -port=7773
start-cluster:
	go run cluster/main.go -ihost=http://0.0.0.0:7771