package scaler

import (
	"container/list"
	"context"
	"fmt"
	"github.com/AliyunContainerService/scaler/go/pkg/config"
	model2 "github.com/AliyunContainerService/scaler/go/pkg/model"
	platform_client2 "github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	"github.com/AliyunContainerService/scaler/go/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"math"
	"os"
	"sync"
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
	waitedReq      chan string
	runningIns     util.AtomicInt32
	logger         *log.Logger
}

func New(metaData *model2.Meta, config *config.Config) Scaler {
	client, err := platform_client2.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}

	return NewWithClient(metaData, config, client)
}

func NewWithClient(metaData *model2.Meta, config *config.Config, client platform_client2.Client) Scaler {
	lock := sync.Mutex{}
	scheduler := &Adaptive{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		mu:             &lock,
		instances:      make(map[string]*model2.Instance),
		idleInstance:   list.New(),
		insAvailable:   sync.NewCond(&lock),
		waitedReq:      make(chan string, 1),
		logger:         log.New(os.Stdout, fmt.Sprintf("app=%s, ", metaData.Key), log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	go func() {
		scheduler.insAdaptLoop()
		scheduler.logger.Printf("gc loop for app: %s is stopped", metaData.Key)
	}()

	return scheduler
}

func (s *Adaptive) PopIdleInstance(ctx context.Context, reqId string) *model2.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.idleInstance.Front()
	if element == nil {
		s.logger.Printf("requestId=%s, wait for available instance...", reqId)
		s.waitedReq <- reqId
		insReady := make(chan int)

		go func() {
			for element == nil {
				s.insAvailable.Wait()
				element = s.idleInstance.Front()
			}
			insReady <- 0
		}()

		select {
		case <-ctx.Done():
			return nil
		case <-insReady:
		}
	}

	instance := element.Value.(*model2.Instance)
	s.logger.Printf("requestId=%s, assign instance=%s", reqId, instance.Id)
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
	s.insAvailable.Broadcast()
}

func (s *Adaptive) CreateNewInstance(ctx context.Context, reqId string) (ins *model2.Instance, err error) {
	defer func() {
		if ins != nil {
			s.logger.Printf("instance %s created, init latency: %dms", ins.Id, ins.InitDurationInMs)
		}
	}()
	resourceConfig := model2.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: s.metaData.MemoryInMb,
		},
	}
	slot, err := s.platformClient.CreateSlot(ctx, reqId, &resourceConfig)
	if err != nil {
		return nil, fmt.Errorf("create slot failed with: %s", err.Error())
	}

	meta := &model2.Meta{
		Meta: pb.Meta{
			Key:           s.metaData.Key,
			Runtime:       s.metaData.Runtime,
			TimeoutInSecs: s.metaData.TimeoutInSecs,
		},
	}
	instance, err := s.platformClient.Init(ctx, reqId, uuid.New().String(), slot, meta)
	if err != nil {
		return nil, fmt.Errorf("create instance failed with: %s", err.Error())
	}

	return instance, nil
}

func (s *Adaptive) Assign(ctx context.Context, request *pb.AssignRequest) (rst *pb.AssignReply, err error) {
	start := time.Now()
	defer func() {
		instanceId := ""
		if rst != nil {
			instanceId = rst.Assigment.InstanceId
		}
		s.logger.Printf("Finish Assign, requestId=%s, instanceId=%s, cost=%dms", request.RequestId, instanceId, time.Since(start).Milliseconds())
	}()
	s.logger.Printf("Start Assign, requestId=%s, idleInstance=%d", request.RequestId, s.idleInstance.Len())
	s.runningIns.Add(1)
	if ins := s.PopIdleInstance(ctx, request.RequestId); ins != nil {
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    ins.Meta.Key,
				InstanceId: ins.Id,
			},
			ErrorMessage: nil,
		}, nil
	} else {
		return nil, status.Errorf(codes.Canceled, "context canceled")
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
		s.insAvailable.Broadcast()
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

func (s *Adaptive) insGC() {
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

func (s *Adaptive) insAdaptLoop() {
	s.logger.Printf("instance adapt loop started")
	ticker := time.NewTicker(s.config.GcInterval)
	for {
		select {
		case <-ticker.C:
			s.insGC()
		case req := <-s.waitedReq:
			pending := 0
			resChan := make(chan *model2.Instance, 1)
			adjustInstance := func() int {
				s.mu.Lock()
				defer s.mu.Unlock()
				targetIns := int(math.Ceil(float64(s.runningIns.Load()) * (1 + s.config.IdlePct)))
				needCreate := targetIns - len(s.instances) - pending
				if needCreate <= 0 {
					return 0
				}
				s.logger.Printf("Start CreateNewInstance, requestId=%s, targetIns=%d, pending=%d, needCreate=%d", req, targetIns, pending, needCreate)
				for i := 0; i < needCreate; i++ {
					go func() {
						ins, err := s.CreateNewInstance(context.Background(), req)
						if err != nil {
							resChan <- nil
							s.logger.Printf("requestId=%s, create instance failed, err=%s", req, err.Error())
							return
						}

						resChan <- ins
					}()
				}

				return needCreate
			}
			pending += adjustInstance()

			for pending > 0 {
				select {
				case req = <-s.waitedReq:
					pending += adjustInstance()
				case ins := <-resChan:
					pending--
					s.AddNewInstance(ins)
					s.logger.Printf("Add new instance %s(slot=%s)", ins.Id, ins.Slot.Id)
				}
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
