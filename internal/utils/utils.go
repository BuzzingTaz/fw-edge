package utils

import (
	"encoding/json"
	"time"

	pb "github.com/BuzzingTaz/fw-edge-apps/proto"
	"github.com/pion/webrtc/v4/pkg/media"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func MarshalMediaSample(sample *media.Sample) ([]byte, error) {
	msg, err := mediaSampleToProto(sample)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(msg)
}

func UnmarshalMediaSample(data []byte) (*media.Sample, error) {
	var msg pb.MediaSample
	if err := proto.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return protoToMediaSample(&msg)
}

func mediaSampleToProto(sample *media.Sample) (*pb.MediaSample, error) {
	msg := &pb.MediaSample{
		Data:               sample.Data,
		DurationNs:         int64(sample.Duration),
		PacketTimestamp:    sample.PacketTimestamp,
		PrevDroppedPackets: uint32(sample.PrevDroppedPackets),
	}

	if !sample.Timestamp.IsZero() {
		msg.Timestamp = timestamppb.New(sample.Timestamp)
	}

	if sample.Metadata != nil {
		metadata, err := json.Marshal(sample.Metadata)
		if err != nil {
			return nil, err
		}
		msg.Metadata = metadata
	}

	return msg, nil
}

func protoToMediaSample(msg *pb.MediaSample) (*media.Sample, error) {
	sample := &media.Sample{
		Data:               msg.GetData(),
		Duration:           time.Duration(msg.GetDurationNs()),
		PacketTimestamp:    msg.GetPacketTimestamp(),
		PrevDroppedPackets: uint16(msg.GetPrevDroppedPackets()),
	}

	if ts := msg.GetTimestamp(); ts != nil {
		sample.Timestamp = ts.AsTime()
	}

	if len(msg.GetMetadata()) > 0 {
		sample.Metadata = msg.GetMetadata()
	}

	return sample, nil
}
