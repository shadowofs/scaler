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
	insAvailable   *sync.Cond
	idleInstance   *list.List
	waitedReq      chan string
	waitedReqNum   util.AtomicInt32
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
		insAvailable:   sync.NewCond(&lock),
		idleInstance:   list.New(),
		waitedReq:      make(chan string, 1),
		logger:         log.New(os.Stdout, fmt.Sprintf("app=%s, ", metaData.Key), log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	go func() {
		scheduler.adapterLoop()
		scheduler.logger.Printf("gc loop for app: %s is stopped", metaData.Key)
	}()

	return scheduler
}

func (s *Adaptive) PopIdleInstance(ctx context.Context, reqId string) *model2.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.idleInstance.Front()

	if element == nil {
		s.waitedReqNum.Add(1)
		defer s.waitedReqNum.Add(-1)
		s.waitedReq <- reqId
		insReady := make(chan int, 0)

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
			// move on
		}
	}

	s.idleInstance.Remove(element)
	instance := element.Value.(*model2.Instance)
	instance.Busy = true
	//s.logger.Printf("requestId=%s, assign instance=%s", reqId, instance.Id)
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
			s.logger.Printf("instance %s created, init latency=%dms, reqId=%s", ins.Id, ins.InitDurationInMs, reqId)
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
		if err != nil {
			s.logger.Printf("Assign Error, requestId=%s, error=%s", request.RequestId, err)
		} else {
			s.logger.Printf("Assign, requestId=%s, instanceId=%s, cost=%dms", request.RequestId, rst.Assigment.InstanceId, time.Since(start).Milliseconds())
		}
	}()
	//s.logger.Printf("Start Assign, requestId=%s, idleInstance=%d", request.RequestId, s.idleInstance.Len())
	var ins *model2.Instance
	if ins = s.PopIdleInstance(ctx, request.RequestId); ins == nil {
		return nil, status.Errorf(codes.Canceled, "context canceled")
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

func (s *Adaptive) Idle(ctx context.Context, request *pb.IdleRequest) (reply *pb.IdleReply, err error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("assignment is nil"))
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	destroyed := false
	reason := ""
	var instance *model2.Instance

	defer func() {
		if err != nil {
			s.logger.Printf("Idle Error, requestId: %s, err=%v", request.Assigment.RequestId, err)
		} else {
			s.logger.Printf("Idle, requestId: %s, instanceId=%s, destroy=%v, reason=%s, cost=%dus",
				request.Assigment.RequestId, instanceId, destroyed, reason, time.Since(start).Microseconds())
		}
	}()

	defer func() {
		if destroyed {
			s.deleteSlot(ctx, uuid.New().String(), instance.Slot.Id, instance.Id, reason)
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	instance = s.instances[instanceId]
	if instance == nil {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}

	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		reason = "bad instance"
		destroyed = true
	} else if s.waitedReqNum.Load() == 0 &&
		s.idleInstance.Len() >= util.Max(s.config.ColdStartBufferSize, int(s.waitedReqNum.Max())) {
		reason = "too many idle instance"
		destroyed = true
	}

	if destroyed {
		delete(s.instances, instance.Id)
	} else {
		instance.Busy = true
		instance.LastIdleTime = time.Now()
		s.idleInstance.PushFront(instance)
		s.insAvailable.Broadcast()
	}

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
	//s.logger.Printf("start delete Instance %s (Slot: %s), reason: %s", instanceId, slotId, reason)
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
	//s.logger.Printf("instance adapt loop started")
	ticker := time.NewTicker(s.config.GcInterval)
	pending := 0
	resChan := make(chan int, 0)
	defer close(resChan)

	for {
		select {
		case <-ticker.C:
			s.insGC()
		case reqId := <-s.waitedReq:
			adjustInstanceNum := func(reqId string) int {
				createNum := int(s.waitedReqNum.Load()) + s.config.ColdStartBufferSize - pending
				if createNum <= 0 {
					return 0
				}
				//s.logger.Printf("Start CreateNewInstance, requestId=%s, pending=%d, createNum=%d", reqId, pending, createNum)
				for i := 0; i < createNum; i++ {
					go func() {
						var ins *model2.Instance
						var err error

						ins, err = s.CreateNewInstance(context.Background(), reqId)
						if err != nil {
							s.logger.Printf("requestId=%s, create instance failed, err=%s", reqId, err.Error())
							resChan <- 0
							return
						}

						s.AddNewInstance(ins)
						resChan <- 0
					}()
				}

				return createNum
			}

			pending += adjustInstanceNum(reqId)
			for pending > 0 {
				select {
				case reqId = <-s.waitedReq:
					pending += adjustInstanceNum(reqId)
				case <-resChan:
					pending--
				}
			}
		}
	}
}
