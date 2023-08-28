package scaler

import (
	"container/list"
	"context"
	"fmt"
	"github.com/AliyunContainerService/scaler/go/pkg/config"
	model2 "github.com/AliyunContainerService/scaler/go/pkg/model"
	platform_client2 "github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

type Adaptive struct {
	config         *config.Config
	metaData       *model2.Meta
	platformClient platform_client2.Client
	mu             *sync.Mutex
	instances      map[string]*model2.Instance
	idleInstance   *list.List
	insAvailable   *sync.Cond
	waitedReq      chan *pb.AssignRequest
	runningIns     atomic.Uint64
	logger         *log.Logger
}

func New(metaData *model2.Meta, config *config.Config) Scaler {
	client, err := platform_client2.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	lock := sync.Mutex{}
	scheduler := &Adaptive{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		mu:             &lock,
		instances:      make(map[string]*model2.Instance),
		idleInstance:   list.New(),
		insAvailable:   sync.NewCond(&lock),
		waitedReq:      make(chan *pb.AssignRequest, 1),
		runningIns:     atomic.Uint64{},
		logger:         log.New(os.Stdout, fmt.Sprintf("app=%s", metaData.Key), log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	go func() {
		scheduler.instanceAdaptLoop()
		scheduler.logger.Printf("gc loop for app: %s is stoped", metaData.Key)
	}()

	return scheduler
}

func (s *Adaptive) PopIdleInstance(request *pb.AssignRequest, blocking bool) *model2.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	var element *list.Element
	for element = s.idleInstance.Front(); element == nil; {
		if !blocking {
			return nil
		} else {
			s.logger.Printf("requestId=%s, wait for available instance...", request.RequestId)
			s.waitedReq <- request
			s.insAvailable.Wait()
		}
	}

	instance := element.Value.(*model2.Instance)
	instance.Busy = true
	s.idleInstance.Remove(element)
	return instance
}

func (s *Adaptive) AddNewInstance(instance *model2.Instance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[instance.Id] = instance
	instance.Busy = false
	instance.LastIdleTime = time.Now()
	s.idleInstance.PushFront(instance)
	s.insAvailable.Signal()
}

func (s *Adaptive) CreateNewInstance(ctx context.Context, request *pb.AssignRequest) (ins *model2.Instance, err error) {
	defer func() {
		if ins != nil {
			s.logger.Printf("instance %s created, init latency: %dms", ins.Id, ins.InitDurationInMs)
		}
	}()
	resourceConfig := model2.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, &resourceConfig)
	if err != nil {
		return nil, fmt.Errorf("create slot failed with: %s", err.Error())
	}

	meta := &model2.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
		},
	}
	instance, err := s.platformClient.Init(ctx, request.RequestId, uuid.New().String(), slot, meta)
	if err != nil {
		return nil, fmt.Errorf("create instance failed with: %s", err.Error())
	}

	return instance, nil
}

func (s *Adaptive) Assign(_ context.Context, request *pb.AssignRequest) (rst *pb.AssignReply, err error) {
	start := time.Now()
	defer func() {
		instanceId := ""
		if rst != nil {
			instanceId = rst.Assigment.InstanceId
		}
		s.logger.Printf("Finish Assign, requestId=%s, instanceId=%s, cost=%dms", request.RequestId, instanceId, time.Since(start).Milliseconds())
	}()
	s.logger.Printf("Start Assign, requestId=%s,idleInstance=%d", request.RequestId, s.idleInstance.Len())
	s.runningIns.Add(1)
	if idle := s.PopIdleInstance(request, true); idle != nil {
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    idle.Meta.Key,
				InstanceId: idle.Id,
			},
			ErrorMessage: nil,
		}, nil
	} else {
		return nil, status.Errorf(codes.Internal, fmt.Sprintf("can't get available instance"))
	}
}

func (s *Adaptive) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	s.runningIns.Add(-1)
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("assignment is nil"))
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	needDestroy := false
	var instance *model2.Instance

	defer func() {
		s.logger.Printf("Idle, request id: %s, instance: %s, destroy: %v, cost %dus", request.Assigment.RequestId, instanceId, needDestroy, time.Since(start).Microseconds())
		if needDestroy && instance != nil {
			s.deleteSlot(ctx, request.Assigment.RequestId, instance.Slot.Id, instanceId, "bad instance")
		}
	}()

	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		instance = s.instances[instanceId]
		if instance == nil {
			return status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
		}

		maxIdle := int(math.Ceil(float64(s.runningIns.Load()) * s.config.IdlePct))
		needDestroy = s.idleInstance.Len() >= maxIdle

		if needDestroy {
			delete(s.instances, instance.Id)
			return nil
		}

		if !instance.Busy {
			s.logger.Printf("requestId=%s, instance %s already freed", request.Assigment.RequestId, instanceId)
			return nil
		}

		instance.LastIdleTime = time.Now()
		instance.Busy = false
		s.idleInstance.PushFront(instance)
		s.insAvailable.Signal()
		return nil
	}(); err != nil {
		return nil, err
	}

	return reply, nil
}

func (s *Adaptive) deleteSlot(ctx context.Context, requestId, slotId, instanceId, reason string) {
	s.logger.Printf("start delete Instance %s (Slot: %s)", instanceId, slotId)
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		s.logger.Printf("delete Instance %s (Slot: %s) failed with: %s", instanceId, slotId, err.Error())
	}
}

func (s *Adaptive) gcInstance() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.idleInstance.Back(); element != nil {
		instance := element.Value.(*model2.Instance)
		idleDuration := time.Now().Sub(instance.LastIdleTime)
		if idleDuration > s.config.IdleDurationBeforeGC {
			//need GC
			s.idleInstance.Remove(element)
			delete(s.instances, instance.Id)
			s.mu.Unlock()
			go func() {
				reason := fmt.Sprintf("Idle duration: %fs, excceed configured duration: %fs", idleDuration.Seconds(), s.config.IdleDurationBeforeGC.Seconds())
				ctx := context.Background()
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, reason)
			}()
		}
	}
}

func (s *Adaptive) instanceAdaptLoop() {
	s.logger.Printf("instance adapt loop started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	for {
		select {
		case <-ticker.C:
			s.gcInstance()
		case req := <-s.waitedReq:
			s.mu.Lock()
			targetIdleIns := int(math.Ceil(float64(s.runningIns.Load()) * s.config.IdlePct))
			needCreate := targetIdleIns - s.idleInstance.Len()
			s.mu.Unlock()
			for i := 0; i < needCreate; i++ {
				go func() {
					instance, err := s.CreateNewInstance(context.Background(), req)
					if err != nil {
						s.logger.Printf("requestId=%s, create instance failed, err=%s", req.RequestId, err.Error())
						return
					}

					s.AddNewInstance(instance)
				}()
			}
		}
	}
}

func (s *Adaptive) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
}
