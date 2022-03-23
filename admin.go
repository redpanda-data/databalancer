package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

var client = &http.Client{Timeout: 10 * time.Second}

const MOVE_REPLICA int = 1000
const GET_BROKERS int = 1001
const GET_PARTITION_DETAIL int = 1002
const GET_CONTROLLER int = 1003
const GET_NODE_CONFIG int = 1004

type partitionDetail struct {
	Status      string        `json:"status"`
	LeaderId    int           `json:"leader_id"`
	RaftGroupId int           `json:"raft_group_id"`
	Replicas    []replicaInfo `json:"replicas"`
}

type replicaInfo struct {
	NodeId int `json:"node_id"`
	CoreId int `json:"core"`
}

type brokerInfo struct {
	NodeId           int    `json:"node_id"`
	NumCores         int    `json:"num_cores"`
	MembershipStatus string `json:"membership_status"`
	IsAlive          bool   `json:"is_alive"`
	DiskSpace        []struct {
		Path  string `json:"path"`
		Free  int64  `json:"free"`
		Total int64  `json:"total"`
	} `json:"disk_space"`
	Version string `json:"version"`
}

type brokerInfos []brokerInfo

func getJson(url string, target interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	err = json.NewDecoder(resp.Body).Decode(target)
	return err
}

func moveReplica(fromBrokerID int32, toBrokerID int32, partInfo partitionInfo, host string) (partitionDetail, error) {

	err, pdetail := getPartitionDetail(host, partInfo)
	if err != nil {
		return pdetail, err
	}
	// Stop if balancing is going on this topic
	if pdetail.Status != "done" {
		return pdetail, nil
	}

	max_core, err := findMaxCore(host)
	if err != nil {
		return pdetail, nil
	}
	core := rand.Intn(max_core) // pick a random core

	replicas := pdetail.Replicas
	for i := 0; i < len(replicas); i++ {
		attr := &replicas[i]
		if attr.NodeId == int(fromBrokerID) { // if current broker is fromBroker then change
			attr.NodeId = int(toBrokerID)
			attr.CoreId = core
			break
		}
	}
	pdetail.Replicas = replicas
	postMoveReplica(host, partInfo, pdetail)
	return pdetail, nil
}

func getPartitionDetail(host string, partInfo partitionInfo) (error, partitionDetail) {
	partDetail := partitionDetail{}
	param := []string{partInfo.topicName, strconv.FormatInt(int64(partInfo.partition), 10)}
	url := buildUri(host, GET_PARTITION_DETAIL, param)
	err := getJson(url, &partDetail)
	if err != nil {
		return nil, partDetail
	}
	return err, partDetail
}

func postMoveReplica(host string, partInfo partitionInfo, payload partitionDetail) {
	param := []string{partInfo.topicName, strconv.FormatInt(int64(partInfo.partition), 10)}
	url := buildUri(host, MOVE_REPLICA, param)
	p, _ := json.Marshal(payload.Replicas)

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(p))
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	log.Println("Request:", request)

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()

	fmt.Println("response Status:", response.Status)
	fmt.Println("response Headers:", response.Header)
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Println("response Body:", string(body))
}

func getAvailableSpace(host string, destinationBrokerID int32) (free int64, e error) {
	brokInfos := brokerInfos{}
	param := []string{}
	url := buildUri(host, GET_BROKERS, param)
	err := getJson(url, &brokInfos)
	if err != nil {
		return -1, err
	}
	if len(brokInfos) == 0 {
		return -1, errors.New("Cannot find broker info")
	}
	for _, v := range brokInfos {
		if int32(v.NodeId) == destinationBrokerID {
			space := v.DiskSpace
			free := space[0].Free
			return free, nil
		}
	}
	return -1, errors.New("Cannot find broker info")

}

func isEnoughSpaceAvailable(host string, destinationBrokerID int32, neededSpace int64) (flag bool, e error) {
	free, err := getAvailableSpace(host, destinationBrokerID)
	if err != nil {
		return false, err
	}
	if free > neededSpace {
		return true, nil
	}
	return false, nil
}

func findMaxCore(host string) (maxC int, e error) {

	brokInfos := brokerInfos{}
	param := []string{}
	url := buildUri(host, GET_BROKERS, param)
	err := getJson(url, &brokInfos)
	if err != nil {
		return 0, err
	}

	if len(brokInfos) == 0 {
		return 0, errors.New("Cannot find available cores of a broker")
	}
	max := 0
	for _, v := range brokInfos {
		if v.NumCores > max {
			max = v.NumCores
		}
	}
	return max, nil
}

func buildUri(host string, action int, param []string) string {
	switch action {
	case MOVE_REPLICA:
		return "http://" + host + ":9644/v1/partitions/kafka/" + param[0] + "/" + param[1] + "/replicas"
	case GET_BROKERS:
		return "http://" + host + ":9644/v1/brokers/"
	case GET_PARTITION_DETAIL:
		return "http://" + host + ":9644/v1/partitions/kafka/" + param[0] + "/" + param[1]
	case GET_CONTROLLER:
		return "http://" + host + ":9644/v1/partitions/redpanda/controller/0"
	case GET_NODE_CONFIG:
		return "http://" + host + ":9644/v1/node_config"
	}

	return ""
}
