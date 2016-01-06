// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Handler for Hyper containers.
package hyper

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/google/cadvisor/container"
	"github.com/google/cadvisor/fs"
	info "github.com/google/cadvisor/info/v1"
)

const WatchInterval = 3 * time.Second

type hyperContainerHandler struct {
	name               string
	id                 string
	alias              []string
	isPod              bool
	client             *HyperClient
	fsInfo             fs.FsInfo
	machineInfoFactory info.MachineInfoFactory

	// container watcher
	containers  map[string]string
	stopWatcher chan error

	// Time at which this container was created.
	creationTime time.Time
}

func newHyperContainerHandler(
	client *HyperClient,
	name string,
	machineInfoFactory info.MachineInfoFactory,
	fsInfo fs.FsInfo) (container.ContainerHandler, error) {
	handler := &hyperContainerHandler{
		client:             client,
		fsInfo:             fsInfo,
		alias:              []string{name},
		machineInfoFactory: machineInfoFactory,
		stopWatcher:        make(chan error),
		containers:         make(map[string]string),
	}

	// Process hyper containers
	if strings.HasPrefix(name, "/hyper") {
		containerId := strings.Split(name, "/")[2]
		container, err := client.GetContainer(containerId)
		if err != nil {
			return handler, err
		}

		handler.id = containerId
		handler.name = name
		handler.alias = append(handler.alias, container.Name,
			containerId, "/"+containerId)
		return handler, nil
	}

	// Process hyper pods
	vmName, err := isHyperVirtualMachine(name)
	if err != nil {
		return handler, err
	}

	glog.V(3).Infof("Got hyper vm: %s", vmName)
	pods, err := client.ListPodsByVM(vmName)
	if err != nil {
		return handler, err
	}
	if len(pods) == 0 {
		return handler, fmt.Errorf("Can't find any pod in vm %s", vmName)
	}

	pod := pods[0]
	handler.name = name
	handler.id = pod.PodID

	alias := []string{pod.PodName, vmName, pod.PodID}
	for _, a := range alias {
		handler.alias = append(handler.alias, a, "/"+a)
	}

	return handler, nil
}

func (self *hyperContainerHandler) Cleanup() {
}

func (self *hyperContainerHandler) ContainerReference() (info.ContainerReference, error) {
	// We only know the container by its one name.
	return info.ContainerReference{
		Name:      self.name,
		Namespace: HyperNamespace,
		Aliases:   self.alias,
	}, nil
}

func (self *hyperContainerHandler) GetSpec() (info.ContainerSpec, error) {
	var spec info.ContainerSpec

	// Get machine info.
	// mi, err := self.machineInfoFactory.GetMachineInfo()
	// if err != nil {
	// 	return spec, err
	// }

	// TODO: CPU, Memory, Fs, Network, DiskIo
	spec.CreationTime = time.Now().Add(-time.Hour)
	spec.Cpu = info.CpuSpec{Limit: 1024}
	spec.HasCpu = true

	spec.Memory = info.MemorySpec{Limit: 18446744073709551615, SwapLimit: 18446744073709551615}
	spec.HasMemory = true
	spec.HasDiskIo = true
	spec.HasNetwork = true

	return spec, nil
}

func (self *hyperContainerHandler) GetStats() (*info.ContainerStats, error) {
	// TODO: get stats
	stats := info.ContainerStats{Timestamp: time.Now().Add(-15 * time.Minute)}
	// stats, err := containerlibcontainer.GetStats(self.cgroupManager, self.rootFs, self.pid)
	// if err != nil {
	// 	return stats, err
	// }

	// Get filesystem stats.
	// err = self.getFsStats(stats)
	// if err != nil {
	// 	return stats, err
	// }

	// TODO: replace demo stats
	stats.Cpu = info.CpuStats{
		Usage: info.CpuUsage{
			Total:  24750780,
			PerCpu: []uint64{18354559, 6396221},
			User:   0,
			System: 10000000,
		},
		LoadAverage: 0,
	}

	stats.DiskIo = info.DiskIoStats{
		IoServiceBytes: []info.PerDiskStats{
			{
				Major: 253,
				Minor: 8,
				Stats: map[string]uint64{"Async": 5353472, "Read": 5353472, "Sync": 0, "Total": 5353472, "Write": 0},
			},
		},
	}

	stats.Memory = info.MemoryStats{
		Usage:      5763072,
		WorkingSet: 1871872,
		ContainerData: info.MemoryStatsMemoryData{
			Pgfault:    3174,
			Pgmajfault: 12,
		},
		HierarchicalData: info.MemoryStatsMemoryData{
			Pgfault:    3174,
			Pgmajfault: 12,
		},
	}

	stats.Network = info.NetworkStats{
		InterfaceStats: info.InterfaceStats{
			Name:      "eth0",
			RxBytes:   123223,
			RxPackets: 128,
			TxBytes:   10240,
			TxPackets: 10,
		},
		Interfaces: []info.InterfaceStats{
			{
				Name:      "eth0",
				RxBytes:   123223,
				RxPackets: 128,

				TxBytes:   10240,
				TxPackets: 10,
			},
		},
	}

	stats.Filesystem = []info.FsStats{}

	stats.TaskStats = info.LoadStats{}

	return &stats, nil
}

func (self *hyperContainerHandler) ListContainers(listType container.ListType) ([]info.ContainerReference, error) {
	containers, err := self.client.ListContainers()
	if err != nil {
		return nil, err
	}

	ret := make([]info.ContainerReference, 0, len(containers))
	for _, c := range containers {
		ret = append(ret, info.ContainerReference{
			Name:      "/hyper/" + c.containerID,
			Namespace: HyperNamespace,
		})
	}

	return ret, nil
}

func (self *hyperContainerHandler) ListThreads(listType container.ListType) ([]int, error) {
	return nil, nil
}

func (self *hyperContainerHandler) ListProcesses(listType container.ListType) ([]int, error) {
	return nil, nil
}

func (self *hyperContainerHandler) WatchSubcontainers(events chan container.SubcontainerEvent) error {
	go func(self *hyperContainerHandler) {
		for {
			time.Sleep(WatchInterval)
			containers, err := self.client.ListContainers()
			if err != nil {
				glog.Errorf("Error list hyper containers: %v", err)
				continue
			}

			newContainerMap := make(map[string]string)
			for _, c := range containers {
				containerName := c.name
				newContainerMap[containerName] = containerName

				if _, ok := self.containers[containerName]; !ok {
					self.containers[containerName] = containerName
					// Deliver the event.
					events <- container.SubcontainerEvent{
						EventType: container.SubcontainerAdd,
						Name:      containerName,
					}
				}
			}

			for k := range self.containers {
				if _, ok := newContainerMap[k]; !ok {
					delete(self.containers, k)
					// Deliver the event.
					events <- container.SubcontainerEvent{
						EventType: container.SubcontainerDelete,
						Name:      k,
					}
				}
			}
		}
	}(self)

	return nil
}

func (self *hyperContainerHandler) StopWatchingSubcontainers() error {
	// Rendezvous with the watcher thread.
	self.stopWatcher <- nil
	return <-self.stopWatcher
}

func (self *hyperContainerHandler) GetCgroupPath(resource string) (string, error) {
	return "", fmt.Errorf("CgroupPath is not supported for Hyper container deriver")
}

func (self *hyperContainerHandler) GetContainerLabels() map[string]string {
	return map[string]string{}
}

func (self *hyperContainerHandler) Exists() bool {
	_, err := self.client.GetContainer(self.name)
	if err != nil {
		return false
	}

	return true
}
