package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"
)

var client = &http.Client{Timeout: 10 * time.Second}

const MOVE_REPLICA int = 1000
const GET_BROKERS int = 1001
const GET_PARTITION_DETAIL int = 1002

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
	NodeId           string    `json:"node_id"`
	NumCores         int       `json:"num_cores"`
	MembershipStatus string    `json:"membership_status"`
	isAlive          bool      `json:"is_alive"`
	DiskSpace        diskSpace `json:"diskSpace"`
	Version          string    `json:"Version"`
}

type diskSpace struct {
	Path  string `json:"path"`
	Free  int32  `json:"free"`
	Total int32  `json:total"`
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

func moveReplica(broker_id int32, part int32, topicName string, host string) error {

	partDetail := partitionDetail{}
	param := []string{topicName, string(part)}
	url := buildUri(host, GET_PARTITION_DETAIL, param)
	getJson(url, &partDetail)

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
	for _, replica := range replicas {
		if replica.CoreId != partDetail.LeaderId {
			replica.NodeId = int(broker_id)
			replica.CoreId = core
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

func findMaxCore(host string) (maxC int, e error) {

	brokInfos := brokerInfos{}
	param := []string{}
	url := buildUri(host, GET_BROKERS, param)
	err := getJson(url, &brokInfos)
	if err != nil {
		return 0, err
	}

	if len(brokInfos) == 0 {
		return 0, errors.New("Cannot detect a minimum value in an empty slice")
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
		return "http://" + host + ":9644/v1/partitions/kafka/" + param[0] + "/1"
	}
	return ""
}
