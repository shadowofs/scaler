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
	waitedReq      chan *reqContext
	waitedNum      util.AtomicInt32
	logger         *log.Logger
}

type reqContext struct {
	id      string
	resChan chan<- *model2.Instance
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
		waitedReq:      make(chan *reqContext, 1),
		logger:         log.New(os.Stdout, fmt.Sprintf("app=%s, ", metaData.Key), log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	go func() {
		scheduler.adapterLoop()
		scheduler.logger.Printf("gc loop for app: %s is stopped", metaData.Key)
	}()

	return scheduler
}

func (s *Adaptive) WaitNewInstance(ctx context.Context, reqId string) *model2.Instance {
	s.logger.Printf("requestId=%s, wait for available instance...", reqId)
	resChan := make(chan *model2.Instance)
	defer close(resChan)
	reqCtx := reqContext{reqId, resChan}
	s.waitedNum.Add(1)
	defer s.waitedNum.Add(-1)
	s.waitedReq <- &reqCtx

	select {
	case <-ctx.Done():
		return nil
	case ins := <-resChan:
		ins.Busy = true
		return ins
	}
}

func (s *Adaptive) PopIdleInstance(reqId string) *model2.Instance {
	s.mu.Lock()
	element := s.idleInstance.Front()
	s.mu.Unlock()

	if element == nil {
		return nil
	}

	instance := element.Value.(*model2.Instance)
	s.logger.Printf("requestId=%s, assign instance=%s", reqId, instance.Id)
	instance.Busy = true
	s.idleInstance.Remove(element)
	return instance
}

func (s *Adaptive) AddIdleInstance(instance *model2.Instance) (destroy bool) {
	defer func() {
		if destroy {
			s.deleteSlot(context.Background(), uuid.New().String(), instance.Slot.Id, instance.Id, "too many idle instance")
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	destroy = s.idleInstance.Len() >= s.config.MaxIdleIns
	if destroy {
		delete(s.instances, instance.Id)
		return
	}

	s.logger.Printf("Add new idle instance %s(slot=%s)", instance.Id, instance.Slot.Id)
	s.instances[instance.Id] = instance
	instance.Busy = false
	instance.LastIdleTime = time.Now()
	s.idleInstance.PushFront(instance)
	return
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
	var ins *model2.Instance
	if ins = s.PopIdleInstance(request.RequestId); ins == nil {
		if ins = s.WaitNewInstance(ctx, request.RequestId); ins == nil {
			return nil, status.Errorf(codes.Canceled, "context canceled")
		}
	}

	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId:  request.RequestId,
			MetaKey:    ins.Meta.Key,
			InstanceId: ins.Id,
		},
		ErrorMessage: nil,
	}, nil
}

func (s *Adaptive) Idle(_ context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("assignment is nil"))
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	destroyed := false
	var instance *model2.Instance

	defer func() {
		s.logger.Printf("Idle, request id: %s, instance: %s, destroy: %v, cost %dus", request.Assigment.RequestId, instanceId, destroyed, time.Since(start).Microseconds())
	}()

	instance = s.getInstance(instanceId)
	if instance == nil {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}
	destroyed = s.AddIdleInstance(instance)

	return &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}, nil
}

func (s *Adaptive) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
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

func (s *Adaptive) adapterLoop() {
	s.logger.Printf("instance adapt loop started")
	ticker := time.NewTicker(s.config.GcInterval)
	pending := 0
	resChan := make(chan int, 0)
	defer close(resChan)

	for {
		select {
		case <-ticker.C:
			s.insGC()
		case reqCtx := <-s.waitedReq:
			adjustInstanceNum := func() int {
				createNum := int(s.waitedNum.Load()) + s.config.BufferSize - pending
				if createNum <= 0 {
					return 0
				}
				s.logger.Printf("Start CreateNewInstance, requestId=%s, pending=%d, createNum=%d", reqCtx.id, pending, createNum)
				for i := 0; i < createNum; i++ {
					go func(addIdle bool) {
						var ins *model2.Instance
						var err error
						defer func() {
							if r := recover(); r != nil {
								// this happened when request ctx canceled and closed channel
								if ins != nil {
									s.AddIdleInstance(ins)
								}
							}
						}()

						ins, err = s.CreateNewInstance(context.Background(), reqCtx.id)
						if err != nil {
							s.logger.Printf("requestId=%s, create instance failed, err=%s", reqCtx.id, err.Error())
							resChan <- 0
							return
						}

						if addIdle {
							s.AddIdleInstance(ins)
						} else {
							s.addInstance(ins)
							reqCtx.resChan <- ins
						}
						resChan <- 0
					}(i != 0)
				}

				return createNum
			}

			pending += adjustInstanceNum()
			for pending > 0 {
				select {
				case reqCtx = <-s.waitedReq:
					pending += adjustInstanceNum()
				case <-resChan:
					pending--
				}
			}
		}
	}
}

func (s *Adaptive) getInstance(insId string) *model2.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.instances[insId]
}

func (s *Adaptive) addInstance(ins *model2.Instance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[ins.Id] = ins
}
