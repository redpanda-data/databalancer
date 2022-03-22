package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/out"
	"github.com/twmb/tlscfg"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

/*
1. Get a list of all topics
2. Get a list of all partitions and replica placement
3. For each partition get the log dir size of the partition
4. Sum by each node all the replicas using the log dir size of the partition
5. Compare the node with the most data to the node with the least amount of data
6. Find the largest partition on the node with the most data
7. Check to see if this partition has a replica on the node with the least amount of data
8. If not then move the replica to the node with less data
9. If true then move to the next largest partition on the node with the most data
10. Repeat till the diff of utilization between most and least is within 20%
*/

var (
	seedBrokers    = flag.String("brokers", "", "comma delimited list of seed brokers")
	controllerHost = flag.String("controller-host", "", "Current controller broker")
	saslMethod     = flag.String("sasl-method", "", "if non-empty, sasl method to use (must specify all options; supports plain, scram-sha-256, scram-sha-512, aws_msk_iam)")
	saslUser       = flag.String("sasl-user", "", "if non-empty, username to use for sasl (must specify all options)")
	saslPass       = flag.String("sasl-pass", "", "if non-empty, password to use for sasl (must specify all options)")
	dialTLS        = flag.Bool("tls", false, "if true, use tls for connecting (if using well-known TLS certs)")
	caFile         = flag.String("ca-cert", "", "if non-empty, path to CA cert to use for TLS (implies -tls)")
	certFile       = flag.String("client-cert", "", "if non-empty, path to client cert to use for TLS (requires -client-key, implies -tls)")
	keyFile        = flag.String("client-key", "", "if non-empty, path to client key to use for TLS (requires -client-cert, implies -tls)")
)

func die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

type partitionInfo struct {
	topicName string
	partition int32
	size      int64
}

func main() {
	flag.Parse()
	fmt.Println("starting data balancer...")

	logFile, err := os.OpenFile("databalacer.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		die(err.Error())
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	if *controllerHost == "" {
		die("must specify controller . Use 'rpk cluster metadata' command")
	}
	var customTLS bool
	if *caFile != "" || *certFile != "" || *keyFile != "" {
		*dialTLS = true
		customTLS = true
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
	}
	if *dialTLS {
		if customTLS {
			tc, err := tlscfg.New(
				tlscfg.MaybeWithDiskCA(*caFile, tlscfg.ForClient),
				tlscfg.MaybeWithDiskKeyPair(*certFile, *keyFile),
			)
			if err != nil {
				die("unable to create tls config: %v", err)
			}
			opts = append(opts, kgo.DialTLSConfig(tc))
		} else {
			opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
		}
	}

	/*	opts = append(opts, kgo.SASL(scram.Auth{
		User: *saslUser,
		Pass: *saslPass,
	}.AsSha256Mechanism()))*/

	var adm *kadm.Client

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		die("unable to create admin client: %v", err)
	}
	adm = kadm.NewClient(cl)

	for {
		time.Sleep(5 * time.Second)

		tps, errored := getTopicsAndPartitions(err, adm)
		log.Println("Retrieving topics and partitions:", tps)
		if errored {
			die("unable to get topics", err)
			return
		}

		partInfoMap, brokerSizeMap := getLogDirSizeForPartitions(tps, cl)

		log.Println("Size of each brokers:", brokerSizeMap)
		log.Println("Partition/Broker Info:", partInfoMap)

		leastDataSizeBroker, mostDataSizeBroker, brokerIds := findLessAndMostSizeBrokers(brokerSizeMap)
		log.Println("Broker info from least to most in size:", brokerIds)
		mostDataSizeBrokerUtilization := brokerSizeMap[mostDataSizeBroker]
		leastDataSizeBrokerUtilization := brokerSizeMap[leastDataSizeBroker]
		// % Decrease = Most - Least / Least Size × 100
		diffUtilization := ((mostDataSizeBrokerUtilization - leastDataSizeBrokerUtilization) / leastDataSizeBrokerUtilization) * 100
		log.Println("Percent diff of disk utilization between most and least size broker:", diffUtilization)
		if diffUtilization < 20 { // if diff of Utilization less that 20 percent don't move partitions
			//sleep for 5min
			log.Println("Pausing movement for 5min")
			time.Sleep(5 * time.Minute)
			continue
		}

		largPart := findLargestPartitionInBroker(partInfoMap, mostDataSizeBroker)
		log.Println("Large partition to be moved:", largPart)
		// move from least to most size brokers
		for idx := 0; idx < len(brokerIds)-1; idx++ { //ignore existing most size node since its sorted

			isOn, _ := isPartitionOnBroker(partInfoMap, brokerIds[idx], largPart)
			log.Println("Is replica:", largPart, " on broker:", brokerIds[idx], isOn)
			isSpaceAvailable, err := isEnoughSpaceAvailable(*controllerHost, brokerIds[idx], largPart.size)
			log.Println("Is free space available on broker: ", brokerIds[idx], isSpaceAvailable)
			if err != nil {
				die("Unable to find if space is available on broker id to move larger replica", brokerIds[idx], err)
			}
			//see if its in the less loaded broker
			if isOn != true && isSpaceAvailable {
				log.Println("Moving replica.. ", largPart, "broker:", brokerIds[idx])
				movePart, err := moveReplica(mostDataSizeBroker, brokerIds[idx], largPart, *controllerHost)
				log.Println("Replica movement initiated.. ", movePart, "broker:", brokerIds[idx])
				if err != nil {
					die("Error while moving replica:", largPart, "broker:", brokerIds[idx], err)
				}
				flag := true
				for flag {
					err, partDetail := getPartitionDetail(*controllerHost, largPart)
					log.Println("Partition movement in progress.Replica Status:", partDetail)
					if err != nil {
						die("Error retrieving partition status while moving replica:", largPart, "broker:", brokerIds[idx], err)
					}
					if partDetail.Status == "in_progress" {
						log.Println("Partition movement in progress.. ", largPart, "to broker:", brokerIds[idx])
						time.Sleep(1 * time.Minute)
						continue
					} else if partDetail.Status == "done" {
						flag = false
						log.Println("Replica movement done.. ", largPart, "moved to broker:", brokerIds[idx])
						break
					}
					log.Println("Unknown status during Replica movement .. ", largPart, "moved to broker:", brokerIds[idx], "status:", partDetail.Status)
				}
				break
			}

		}
	}
}

func getTopicsAndPartitions(err error, adm *kadm.Client) (map[string][]int32, bool) {
	topicList, err := adm.ListTopics(context.Background()) //returns internal topics as well. filter it below
	if err != nil {
		log.Println("unable to list topics: %w", err)
		return nil, true
	}

	log.Println("Controller Host is:" + *controllerHost)

	metaReq := kmsg.NewMetadataRequest()
	var tps = make(map[string][]int32)

	for indexT, _ := range topicList {
		t := topicList[indexT]
		if t.Err != nil {
			die("unable to describe topics, topic err: %w", t.Err)
		}
		//ignore RP internal topcs
		if strings.Contains(indexT, "__redpanda") {
			continue
		}

		metaReqTopic := kmsg.NewMetadataRequestTopic()
		metaReqTopic.Topic = &t.Topic
		metaReq.Topics = append(metaReq.Topics, metaReqTopic)
		if len(metaReq.Topics) == 0 {
			die("unable to request metadata: %v", err)
		}
		for _, part := range t.Partitions {
			tps[indexT] = append(tps[indexT], part.Partition)
		}
	}
	return tps, false
}

func getLogDirSizeForPartitions(tps map[string][]int32, cl *kgo.Client) (map[int32][]partitionInfo, map[int32]int64) {
	var req kmsg.DescribeLogDirsRequest
	for topic, partitions := range tps {
		req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}

	kresps := cl.RequestSharded(context.Background(), &req)
	tw := out.BeginTabWrite()

	var partInfoMap = make(map[int32][]partitionInfo)
	_, _ = fmt.Fprintf(tw, "BROKER\tERR\tDIR\tTOPIC\tPARTITION\tSIZE\tOFFSET LAG\tIS FUTURE\n")
	var brokerSizeMap = make(map[int32]int64)
	for _, kresp := range kresps {
		if kresp.Err != nil {
			_, _ = fmt.Fprintf(tw, "%d\t%v\t\t\t\t\t\t\n", kresp.Meta.NodeID, kresp.Err)
			continue
		}

		resp := kresp.Resp.(*kmsg.DescribeLogDirsResponse)

		sort.Slice(resp.Dirs, func(i, j int) bool { return resp.Dirs[i].Dir < resp.Dirs[j].Dir })
		for _, dir := range resp.Dirs {

			if err := kerr.ErrorForCode(dir.ErrorCode); err != nil {
				_, _ = fmt.Fprintf(tw, "%d\t%v\t%s\t\t\t\t\t\n", kresp.Meta.NodeID, err, dir.Dir)
				continue
			}

			sort.Slice(dir.Topics, func(i, j int) bool { return dir.Topics[i].Topic < dir.Topics[j].Topic })
			for _, topic := range dir.Topics {
				sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].Partition < topic.Partitions[j].Partition })
				for _, partition := range topic.Partitions {
					_, _ = fmt.Fprintf(tw, "%d\t\t%s\t%s\t%d\t%d\t%d\t%v\n",
						kresp.Meta.NodeID,
						dir.Dir,
						topic.Topic,
						partition.Partition,
						partition.Size,
						partition.OffsetLag,
						partition.IsFuture,
					)
					brokerSizeMap[kresp.Meta.NodeID] = brokerSizeMap[kresp.Meta.NodeID] + partition.Size
					partInfo := partitionInfo{topicName: topic.Topic, partition: partition.Partition, size: partition.Size}
					partInfoMap[kresp.Meta.NodeID] = append(partInfoMap[kresp.Meta.NodeID], partInfo)
				}
			}
		}
	}

	_ = tw.Flush()
	return partInfoMap, brokerSizeMap
}

func findLessAndMostSizeBrokers(brokerSizeMap map[int32]int64) (lessSize int32, moreSize int32, ids []int32) {
	brokerIds := make([]int32, 0, len(brokerSizeMap))
	for brokerid := range brokerSizeMap {
		brokerIds = append(brokerIds, brokerid)
	}
	sort.Slice(brokerIds, func(i, j int) bool {
		return brokerSizeMap[brokerIds[i]] < brokerSizeMap[brokerIds[j]]
	})
	lessDataSizeBroker := brokerIds[0]
	mostDataSizeBroker := brokerIds[len(brokerIds)-1]
	return lessDataSizeBroker, mostDataSizeBroker, brokerIds
}

func findLargestPartitionInBroker(pmap map[int32][]partitionInfo, brokerId int32) partitionInfo {
	for k, v := range pmap {
		if k != brokerId {
			continue
		}
		sort.Slice(v, func(i, j int) bool { return v[i].size > v[j].size })
		return v[0]
	}
	return partitionInfo{}
}

func isPartitionOnBroker(pmap map[int32][]partitionInfo, broker_id int32, part partitionInfo) (b bool, topic string) {
	for k, v := range pmap {
		if k == broker_id {
			p_info := v
			for i := 0; i < len(p_info); i++ {
				if p_info[i].partition == part.partition && p_info[i].topicName == part.topicName {
					log.Println("found partition on ", i, " part:", p_info[i].partition, "topic:", p_info[i].topicName)
					return true, p_info[i].topicName
				}
				continue
			}
		}
	}
	return false, ""
}
