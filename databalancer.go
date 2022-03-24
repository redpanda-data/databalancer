package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/out"
	"github.com/twmb/tlscfg"
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
	seedBrokers = flag.String("brokers", "", "comma delimited list of seed brokers")
	saslMethod  = flag.String("sasl-method", "", "if non-empty, sasl method to use (must specify all options; supports plain, scram-sha-256, scram-sha-512, aws_msk_iam)")
	saslUser    = flag.String("sasl-user", "", "if non-empty, username to use for sasl (must specify all options)")
	saslPass    = flag.String("sasl-pass", "", "if non-empty, password to use for sasl (must specify all options)")
	dialTLS     = flag.Bool("tls", false, "if true, use tls for connecting (if using well-known TLS certs)")
	caFile      = flag.String("ca-cert", "", "if non-empty, path to CA cert to use for TLS (implies -tls)")
	certFile    = flag.String("client-cert", "", "if non-empty, path to client cert to use for TLS (requires -client-key, implies -tls)")
	keyFile     = flag.String("client-key", "", "if non-empty, path to client key to use for TLS (requires -client-cert, implies -tls)")
)

type partitionInfo struct {
	topicName string
	partition int32
	size      int64
}

func retry_backoff() {
	time.Sleep(10 * time.Second)
}

func main() {
	flag.Parse()
	log.Println("starting data balancer...")

	logFile, err := os.OpenFile("databalacer.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("unable to open databalacer.log file, error: %v", err.Error())
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

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
				log.Fatalf("unable to create tls config: %v", err)
			}
			opts = append(opts, kgo.DialTLSConfig(tc))
		} else {
			opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
		}
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("unable to create client: %v", err)
	}
	adm := kadm.NewClient(cl)

	for {

		tps, err := getTopicsAndPartitions(adm)
		if err != nil {
			log.Errorf("unable to get topic list, error: %v, waiting 10 seconds before next retry", err)
			retry_backoff()
			continue
		}
		log.Infof("Topic partitions list: %v", tps)

		partInfoMap, brokerSizeMap := getLogDirSizeForPartitions(tps, cl)
		if len(brokerSizeMap) == 1 {
			log.Info("Single node cluster, nothing to move")
			os.Exit(1)
		}
		log.Infof("Size of each broker: %v", brokerSizeMap)
		log.Infof("Partition/Broker Info: %v", partInfoMap)

		leastDataSizeBroker, mostDataSizeBroker, brokerIds := findLessAndMostSizeBrokers(brokerSizeMap)
		log.Infof("Broker ids from least to most in size: %v", brokerIds)
		mostDataSizeBrokerUtilization := brokerSizeMap[mostDataSizeBroker]
		leastDataSizeBrokerUtilization := brokerSizeMap[leastDataSizeBroker]
		// % Decrease = Most - Least / Least Size × 100
		diffUtilization := ((mostDataSizeBrokerUtilization - leastDataSizeBrokerUtilization) / leastDataSizeBrokerUtilization) * 100
		log.Infof("Percent diff of disk utilization between most and least size broker: %v", diffUtilization)
		if diffUtilization < 20 { // if diff of Utilization less that 20 percent don't move partitions
			//sleep for 5min
			log.Info("Pausing movement for 5min")
			time.Sleep(5 * time.Minute)
			continue
		}

		largePartition := findLargestPartitionInBroker(partInfoMap, mostDataSizeBroker)
		log.Infof("Large partition to be moved: %v", largePartition)
		// iterate from least to most size brokers
		for _, brokerId := range brokerIds[:len(brokerIds)-1] { //ignore existing most size node since its sorted
			controllerHost, err := discoverController(adm)
			if err != nil {
				log.Errorf("Unable to find controller, will retry in 10 seconds, error: %v", err)
			}
			log.Infof("Controller host: %v", controllerHost)
			isOn, _ := isPartitionOnBroker(partInfoMap, brokerId, largePartition)
			log.Infof("Is replica: %v on broker: %v - %v", largePartition, brokerId, isOn)
			isSpaceAvailable, err := isEnoughSpaceAvailable(controllerHost, brokerId, largePartition.size)
			log.Infof("Is free space available on broker: %v - %v", brokerId, isSpaceAvailable)
			if err != nil {
				log.Errorf("Unable to find if space is available on broker id to move larger replica", brokerId, err)
				retry_backoff()
				break
			}
			//see if its in the less loaded broker
			if !isOn && isSpaceAvailable {
				log.Infof("Moving partition replica %v to broker %v", largePartition, brokerId)
				movePart, err := moveReplica(mostDataSizeBroker, brokerId, largePartition, controllerHost)
				log.Infof("Partition %v replica movement initiated to %v ", movePart, brokerId)
				if err != nil {
					log.Errorf("Error while moving replica %v to broker %v - %v", largePartition, brokerId, err)
					retry_backoff()
					break
				}
				err, partDetail := getPartitionDetail(controllerHost, largePartition)

				for err != nil || partDetail.Status != "done" {
					err, partDetail = getPartitionDetail(controllerHost, largePartition)
					if err != nil {
						log.Errorf("Unable to retrieve partition %v details, error: %v", largePartition, err)
						retry_backoff()
					}
					log.Infof("Replica status: %v", partDetail)

					if partDetail.Status == "in_progress" {
						log.Infof("Waiting for partition %v move to broker %v", largePartition, brokerId)
						time.Sleep(1 * time.Minute)
						continue
					} else if partDetail.Status == "done" {
						log.Infof("Partition %v move to broker %v finished", largePartition, brokerId)
						break
					}
				}
				break
			}

		}
	}
}

func getTopicsAndPartitions(adm *kadm.Client) (map[string][]int32, error) {
	topicList, err := adm.ListTopics(context.Background()) //returns internal topics as well. filter it below
	if err != nil {
		return nil, err
	}

	var tps = make(map[string][]int32)

	for _, topic := range topicList {

		if topic.Err != nil {
			log.Fatalf("unable to describe topics, topic err: %w", topic.Err)
		}
		//ignore RP internal topics
		if strings.Contains(topic.Topic, "__redpanda") || strings.Contains(topic.Topic, "__consumer_offsets") {
			continue
		}

		for _, part := range topic.Partitions {
			tps[topic.Topic] = append(tps[topic.Topic], part.Partition)
		}
	}
	return tps, nil
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
					log.Infof("found partition on %d - partition: %v, topic: %v", i, p_info[i].topicName, p_info[i].partition)
					return true, p_info[i].topicName
				}
				continue
			}
		}
	}
	return false, ""
}

func discoverController(adm *kadm.Client) (string, error) {
	m, err := adm.MetadataWithoutTopics(context.Background())
	if err != nil {
		log.Errorf("Unable to request metadata: %v", err)
		return "", err
	}
	controllerHost := getControllerHost(m.Controller, m.Brokers)
	return controllerHost, nil
}

func getControllerHost(controllerID int32, brokers kadm.BrokerDetails) string {
	for i := range brokers {
		if brokers[i].NodeID == controllerID {
			return brokers[i].Host
			break
		}
	}
	return ""
}
