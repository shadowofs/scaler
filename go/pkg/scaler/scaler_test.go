package scaler_test

import (
	"context"
	"github.com/AliyunContainerService/scaler/go/pkg/config"
	"github.com/AliyunContainerService/scaler/go/pkg/model"
	"github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	scaler2 "github.com/AliyunContainerService/scaler/go/pkg/scaler"
	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Assign instance", Ordered, func() {
	var cfg *config.Config
	var scaler scaler2.Scaler
	var createDurationMs = 10

	BeforeAll(func() {
		cfg = &config.DefaultConfig
		metaData := &model.Meta{
			Meta: pb.Meta{
				Key:           "test",
				Runtime:       "go",
				TimeoutInSecs: 60,
				MemoryInMb:    128,
			},
		}
		scaler = scaler2.NewWithClient(metaData, cfg, platform_client.NewFakeClient(createDurationMs))
	})

	When("no idle instance", func() {
		It("should create idle instance", func(ctx context.Context) {
			stats := scaler.Stats()
			Expect(stats.TotalIdleInstance).To(Equal(0))

			req := &pb.AssignRequest{
				RequestId: uuid.New().String(),
			}
			ins, err := scaler.Assign(ctx, req)
			Expect(err).To(BeNil())
			Expect(ins).NotTo(BeNil())
			time.Sleep(time.Millisecond * time.Duration(createDurationMs))
			stats = scaler.Stats()
			Expect(stats.TotalIdleInstance).To(Equal(cfg.BufferSize))
		}, SpecTimeout(time.Second))
	})

	When("idle instance available", func() {
		BeforeEach(func() {
			stats := scaler.Stats()
			Expect(stats.TotalIdleInstance).To(Equal(cfg.BufferSize))
		})

		It("should use idle instance", func(ctx context.Context) {
			req := &pb.AssignRequest{
				RequestId: uuid.New().String(),
			}
			ins, err := scaler.Assign(ctx, req)
			Expect(err).To(BeNil())
			Expect(ins).NotTo(BeNil())
			stats := scaler.Stats()
			Expect(stats.TotalIdleInstance).To(Equal(cfg.BufferSize - 1))
		}, SpecTimeout(time.Second))
	})
})

var _ = Describe("Idle instance", func() {
	var scaler scaler2.Scaler
	var createDurationMs = 10
	cfg := config.DefaultConfig

	BeforeEach(func() {
		cfg.BufferSize = 0
		metaData := &model.Meta{
			Meta: pb.Meta{
				Key:           "test",
				Runtime:       "go",
				TimeoutInSecs: 60,
				MemoryInMb:    128,
			},
		}
		scaler = scaler2.NewWithClient(metaData, &cfg, platform_client.NewFakeClient(createDurationMs))
	})

	When("no idle instance", func() {
		var assign *pb.Assignment
		BeforeEach(func() {
			req := &pb.AssignRequest{
				RequestId: uuid.New().String(),
			}
			res, _ := scaler.Assign(context.Background(), req)
			Expect(res).NotTo(BeNil())
			assign = res.Assigment
			stats := scaler.Stats()
			Expect(stats.TotalIdleInstance).To(Equal(0))
			Expect(stats.TotalInstance).To(Equal(1))
		})

		It("should add instance to idle list", func(ctx context.Context) {
			req := &pb.IdleRequest{
				Assigment: assign,
			}
			res, err := scaler.Idle(ctx, req)
			Expect(err).To(BeNil())
			Expect(res).NotTo(BeNil())
			stats := scaler.Stats()
			Expect(stats.TotalIdleInstance).To(Equal(1))
			Expect(stats.TotalInstance).To(Equal(1))
		}, SpecTimeout(time.Second))
	})
})
