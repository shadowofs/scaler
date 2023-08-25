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
	"sync"
	"time"

	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

type Batch struct {
	config         *config.Config
	metaData       *model2.Meta
	platformClient platform_client2.Client
	mu             sync.Mutex
	wg             sync.WaitGroup
	instances      map[string]*model2.Instance
	idleInstance   *list.List
}

func New(metaData *model2.Meta, config *config.Config) Scaler {
	client, err := platform_client2.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Batch{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		mu:             sync.Mutex{},
		wg:             sync.WaitGroup{},
		instances:      make(map[string]*model2.Instance),
		idleInstance:   list.New(),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		scheduler.gcLoop()
		log.Printf("gc loop for app: %s is stoped", metaData.Key)
	}()

	return scheduler
}

func (s *Batch) PopIdleInstance() *model2.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.idleInstance.Front()
	if element == nil {
		return nil
	}
	instance := element.Value.(*model2.Instance)
	instance.Busy = true
	s.idleInstance.Remove(element)
	return instance
}

func (s *Batch) AddNewInstance(instance *model2.Instance, running bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances[instance.Id] = instance
	if running {
		instance.Busy = true
	} else {
		instance.Busy = false
		instance.LastIdleTime = time.Now()
		s.idleInstance.PushFront(instance)
	}
}

func (s *Batch) CreateNewInstance(ctx context.Context, request *pb.AssignRequest) (ins *model2.Instance, err error) {
	defer func() {
		if ins != nil {
			log.Printf("Assign, requestId=%s, instance %s for app %s is created, init latency: %dms", request.RequestId, ins.Id, ins.Meta.Key, ins.InitDurationInMs)
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

func (s *Batch) Assign(ctx context.Context, request *pb.AssignRequest) (rst *pb.AssignReply, err error) {
	start := time.Now()
	var instances []string
	defer func() {
		log.Printf("Assign, requestId=%s, instances: %s, cost %dms", request.RequestId, instances, time.Since(start).Milliseconds())
	}()
	log.Printf("Assign, request id: %s", request.RequestId)
	if idle := s.PopIdleInstance(); idle != nil {
		instances = append(instances, idle.Id)
		log.Printf("Assign, requestId=%s, instance %s reused", request.RequestId, idle.Id)
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    idle.Meta.Key,
				InstanceId: idle.Id,
			},
			ErrorMessage: nil,
		}, nil
	}

	resChan := make(chan string, 1)
	if s.config.BatchSize > 1 {
		for i := 0; i < s.config.BatchSize-1; i++ {
			go func() {
				instance, err := s.CreateNewInstance(ctx, request)
				if err != nil {
					resChan <- ""
					log.Printf("Assign, requestId=%s, err=%s, batch create instance failed", request.RequestId, err.Error())
					return
				}

				s.AddNewInstance(instance, false)
				resChan <- instance.Id
			}()
		}
	}

	instance, err := s.CreateNewInstance(ctx, request)
	if err != nil {
		log.Printf("Assign, requestId=%s, err=%s, batch create instance failed", request.RequestId, err.Error())
		return
	}

	//add new instance
	s.AddNewInstance(instance, true)
	instances = append(instances, instance.Id)

	if s.config.BatchSize > 1 {
		for i := 0; i < s.config.BatchSize-1; i++ {
			insId := <-resChan
			if insId != "" {
				instances = append(instances, insId)
			}
		}
	}

	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId:  request.RequestId,
			MetaKey:    instance.Meta.Key,
			InstanceId: instance.Id,
		},
		ErrorMessage: nil,
	}, nil
}

func (s *Batch) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("assignment is nil"))
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	defer func() {
		log.Printf("Idle, request id: %s, instance: %s, cost %dus", request.Assigment.RequestId, instanceId, time.Since(start).Microseconds())
	}()
	//log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	needDestroy := false
	var instance *model2.Instance
	defer func() {
		if needDestroy && instance != nil {
			s.deleteSlot(ctx, request.Assigment.RequestId, instance.Slot.Id, instanceId, request.Assigment.MetaKey, "bad instance")
		}
	}()

	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		instance = s.instances[instanceId]
		if instance == nil {
			return status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
		}

		maxIdle := int(float64(len(s.instances)) * s.config.MaxIdlePct)
		needDestroy = s.idleInstance.Len() >= maxIdle

		instance.LastIdleTime = time.Now()

		if needDestroy {
			return nil
		}

		if !instance.Busy {
			log.Printf("request id %s, instance %s already freed", request.Assigment.RequestId, instanceId)
			return nil
		}

		instance.Busy = false
		s.idleInstance.PushFront(instance)
		return nil
	}(); err != nil {
		return nil, err
	}

	return reply, nil
}

func (s *Batch) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string) {
	log.Printf("start delete Instance %s (Slot: %s) of app: %s", instanceId, slotId, metaKey)
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		log.Printf("delete Instance %s (Slot: %s) of app: %s failed with: %s", instanceId, slotId, metaKey, err.Error())
	}
}

func (s *Batch) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		for {
			s.mu.Lock()
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
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()

					continue
				}
			}
			s.mu.Unlock()
			break
		}
	}
}

func (s *Batch) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
}
