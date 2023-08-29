package platform_client

import (
	"context"
	model2 "github.com/AliyunContainerService/scaler/go/pkg/model"
	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
	"math/rand"
	"time"
)

type FakeClient struct {
	createDurationMs int
}

func NewFakeClient(createDurationMs int) Client {
	return &FakeClient{createDurationMs}
}

func (f *FakeClient) Close() error {
	return nil
}

func (f *FakeClient) CreateSlot(_ context.Context, _ string, req *model2.SlotResourceConfig) (*model2.Slot, error) {
	time.Sleep(time.Duration(f.createDurationMs) * time.Millisecond)
	return &model2.Slot{
		Slot: pb.Slot{
			Id:                 uuid.New().String(),
			ResourceConfig:     &req.ResourceConfig,
			CreateTime:         uint64(time.Now().UnixMilli()),
			CreateDurationInMs: uint64(f.createDurationMs),
		},
	}, nil
}

func (f *FakeClient) DestroySLot(ctx context.Context, requestId, slotId, reason string) error {
	return nil
}

func (f *FakeClient) Init(ctx context.Context, requestId, instanceId string, slot *model2.Slot, meta *model2.Meta) (*model2.Instance, error) {
	return &model2.Instance{
		Id:               instanceId,
		Slot:             slot,
		Meta:             meta,
		CreateTimeInMs:   time.Now().UnixMilli(),
		InitDurationInMs: int64(rand.Intn(1000)),
		Busy:             false,
		LastIdleTime:     time.Now(),
	}, nil
}
