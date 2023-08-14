module implementPackage

go 1.20

replace github.com/confluentinc/confluent-kafka-go/v2 => ./confluent-kafka-go

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.3
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/jhump/protoreflect v1.14.1 // indirect
	google.golang.org/genproto v0.0.0-20230331144136-dcfb400f0633 // indirect
)
