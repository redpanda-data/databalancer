package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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

type controllerInfo struct {
	Ns          string `json:"ns"`
	Topic       string `json:"topic"`
	PartitionId int    `json:"partition_id"`
	Status      string `json:"status"`
	LeaderId    int    `json:"leader_id"`
	RaftGroupId int    `json:"raft_group_id"`
	Replicas    []struct {
		NodeId int `json:"node_id"`
		Core   int `json:"core"`
	} `json:"replicas"`
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

func moveReplica(broker_id int32, partInfo partitionInfo, host string) error {

	partDetail := partitionDetail{}
	param := []string{partInfo.topicName, strconv.FormatInt(int64(partInfo.partition), 10)}
	url := buildUri(host, GET_PARTITION_DETAIL, param)
	err := getJson(url, &partDetail)
	if err != nil {
		return err
	}
	// Stop if balancing is going on this topic
	if partDetail.Status != "done" {
		return fmt.Errorf("partition in middle of transititon")
	}

	max_core, err := findMaxCore(host)
	if err != nil {
		return err
	}
	core := rand.Intn(max_core)

	replicas := partDetail.Replicas
	for i := 0; i < len(replicas); i++ {
		attr := &replicas[i]
		if attr.CoreId != partDetail.LeaderId {
			attr.NodeId = int(broker_id)
			attr.CoreId = core
			break
		}
	}
	partDetail.Replicas = replicas
	url = buildUri(host, MOVE_REPLICA, param)
	postMoveReplica(url, partDetail)
	return nil
}

func postMoveReplica(url string, payload partitionDetail) {
	p, _ := json.Marshal(payload.Replicas)

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(p))
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	fmt.Println("Request:", request)

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

func isEnoughSpaceAvailable(host string, destinationBrokerID int, neededSpace int64) (flag bool, e error) {
	brokInfos := brokerInfos{}
	param := []string{}
	url := buildUri(host, GET_BROKERS, param)
	err := getJson(url, &brokInfos)
	if err != nil {
		return false, err
	}
	if len(brokInfos) == 0 {
		return false, errors.New("Cannot find broker info")
	}
	for _, v := range brokInfos {
		if v.NodeId == destinationBrokerID {
			space := v.DiskSpace
			free := space[0].Free
			fmt.Println("free space on broker id:", v.NodeId, free)
			fmt.Println("needed space on broker id:", v.NodeId, neededSpace)
			if free > neededSpace {

				return true, nil
			}
		}
	}
	return false, errors.New("Cannot find broker with available space")

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
		return 0, errors.New("Cannot find avaiable cores of a broker")
	}
	max := 0
	for _, v := range brokInfos {
		if v.NumCores > max {
			max = v.NumCores
		}
	}
	return max, nil
}

func findController(host string) (int, error) {
	brokInfos := brokerInfos{}
	param := []string{}
	url := buildUri(host, GET_BROKERS, param)
	err := getJson(url, &brokInfos)
	if err != nil {
		return 0, err
	}
	controllerInfo := controllerInfo{}
	param = []string{}
	url = buildUri(host, GET_CONTROLLER, param)
	err = getJson(url, &controllerInfo)
	if err != nil {
		return 0, err
	}

	if len(brokInfos) == 0 {
		return 0, errors.New("Cannot get broker info")
	}
	leaderID := -1
	for _, v := range brokInfos {
		if v.NodeId == controllerInfo.LeaderId {
			leaderID = controllerInfo.LeaderId
		}
	}
	return leaderID, nil
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
