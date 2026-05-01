install:
	go get go.opentelemetry.io/otel
	go get go.opentelemetry.io/otel/exporters/otlp/otlptrace
	go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp
	go get go.opentelemetry.io/otel/sdk
	go get go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin
jaeger:
	docker run -d --rm --name jaeger -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one:1.56
	echo "Open localhost:16686"