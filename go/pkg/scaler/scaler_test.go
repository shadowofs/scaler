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

var _ = Describe("Adaptive", func() {
	var scaler scaler2.Scaler
	var createDurationMs = 10

	BeforeEach(func() {
		metaData := &model.Meta{
			Meta: pb.Meta{
				Key:           "test",
				Runtime:       "go",
				TimeoutInSecs: 60,
				MemoryInMb:    128,
			},
		}

		scaler = scaler2.NewWithClient(metaData, &config.DefaultConfig, platform_client.NewFakeClient(createDurationMs))
	})

	When("Assign", func() {
		It("should create additional instance", func(ctx context.Context) {
			req := &pb.AssignRequest{
				RequestId: uuid.New().String(),
			}
			ins, err := scaler.Assign(ctx, req)
			Expect(err).To(BeNil())
			Expect(ins).NotTo(BeNil())
			time.Sleep(time.Millisecond * time.Duration(createDurationMs))
			stats := scaler.Stats()
			Expect(stats.TotalIdleInstance).To(Equal(1))
			Expect(stats.TotalInstance).To(Equal(2))
		}, SpecTimeout(time.Second))
	})
})
