// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.21.12
// source: api/v1/proto/Message.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Message struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key  string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Text string `protobuf:"bytes,2,opt,name=text,proto3" json:"text,omitempty"`
}

func (x *Message) Reset() {
	*x = Message{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_v1_proto_Message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message) ProtoMessage() {}

func (x *Message) ProtoReflect() protoreflect.Message {
	mi := &file_api_v1_proto_Message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message.ProtoReflect.Descriptor instead.
func (*Message) Descriptor() ([]byte, []int) {
	return file_api_v1_proto_Message_proto_rawDescGZIP(), []int{0}
}

func (x *Message) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Message) GetText() string {
	if x != nil {
		return x.Text
	}
	return ""
}

var File_api_v1_proto_Message_proto protoreflect.FileDescriptor

var file_api_v1_proto_Message_proto_rawDesc = []byte{
	0x0a, 0x1a, 0x61, 0x70, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x4d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1f, 0x69, 0x6f,
	0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x6c, 0x75, 0x65, 0x6e, 0x74, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64,
	0x2e, 0x64, 0x65, 0x6d, 0x6f, 0x2e, 0x64, 0x6f, 0x6d, 0x61, 0x69, 0x6e, 0x31, 0x22, 0x2f, 0x0a,
	0x07, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65,
	0x78, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x65, 0x78, 0x74, 0x42, 0x31,
	0x5a, 0x2f, 0x67, 0x65, 0x74, 0x74, 0x69, 0x6e, 0x67, 0x2d, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65,
	0x64, 0x2d, 0x77, 0x69, 0x74, 0x68, 0x2d, 0x63, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2d, 0x67, 0x6f,
	0x6c, 0x61, 0x6e, 0x67, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_v1_proto_Message_proto_rawDescOnce sync.Once
	file_api_v1_proto_Message_proto_rawDescData = file_api_v1_proto_Message_proto_rawDesc
)

func file_api_v1_proto_Message_proto_rawDescGZIP() []byte {
	file_api_v1_proto_Message_proto_rawDescOnce.Do(func() {
		file_api_v1_proto_Message_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_v1_proto_Message_proto_rawDescData)
	})
	return file_api_v1_proto_Message_proto_rawDescData
}

var file_api_v1_proto_Message_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_api_v1_proto_Message_proto_goTypes = []interface{}{
	(*Message)(nil), // 0: io.confluent.cloud.demo.domain1.Message
}
var file_api_v1_proto_Message_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_api_v1_proto_Message_proto_init() }
func file_api_v1_proto_Message_proto_init() {
	if File_api_v1_proto_Message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_v1_proto_Message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Message); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_v1_proto_Message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_api_v1_proto_Message_proto_goTypes,
		DependencyIndexes: file_api_v1_proto_Message_proto_depIdxs,
		MessageInfos:      file_api_v1_proto_Message_proto_msgTypes,
	}.Build()
	File_api_v1_proto_Message_proto = out.File
	file_api_v1_proto_Message_proto_rawDesc = nil
	file_api_v1_proto_Message_proto_goTypes = nil
	file_api_v1_proto_Message_proto_depIdxs = nil
}
