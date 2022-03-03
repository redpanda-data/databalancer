package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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
*/

var (
	seedBrokers = flag.String("brokers", "34.223.52.26:32523", "comma delimited list of seed brokers")
	saslMethod  = flag.String("sasl-method", "", "if non-empty, sasl method to use (must specify all options; supports plain, scram-sha-256, scram-sha-512, aws_msk_iam)")
	saslUser    = flag.String("sasl-user", "svc-acct", "if non-empty, username to use for sasl (must specify all options)")
	saslPass    = flag.String("sasl-pass", "WF9wlpVVtK5UikA8CqB9:L", "if non-empty, password to use for sasl (must specify all options)")
	dialTLS     = flag.Bool("tls", false, "if true, use tls for connecting (if using well-known TLS certs)")
	caFile      = flag.String("ca-cert", "/Users/praseed/ca.crt", "if non-empty, path to CA cert to use for TLS (implies -tls)")
	certFile    = flag.String("client-cert", "", "if non-empty, path to client cert to use for TLS (requires -client-key, implies -tls)")
	keyFile     = flag.String("client-key", "", "if non-empty, path to client key to use for TLS (requires -client-cert, implies -tls)")
)

func die(msg string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

type partitionInfo struct {
	topicName string
	partition int32
	size      int64
}

func main() {
	flag.Parse()
	fmt.Println("starting...")
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

	opts = append(opts, kgo.SASL(scram.Auth{
		User: *saslUser,
		Pass: *saslPass,
	}.AsSha256Mechanism()))

	var adm *kadm.Client

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		die("unable to create admin client: %v", err)
	}
	adm = kadm.NewClient(cl)

	topicList, err := adm.ListTopics(context.Background()) //returns internal topics as well. filter it below
	if err != nil {
		fmt.Println("unable to list topics: %w", err)
		return
	}

	metaReq := kmsg.NewMetadataRequest()
	var tps = make(map[string][]int32)
	var req kmsg.DescribeLogDirsRequest
	for indexT, topic := range topicList {
		log.Println("At topic index", indexT, "value is", topic)
		t := topicList[indexT]
		if t.Err != nil {
			die("unable to describe topics, topic err: %w", t.Err)
		}
		//ignore RP internal topcs
		if strings.Contains(indexT, "__redpanda") {
			log.Println("Ignoring topic", indexT)
			continue
		}

		metaReqTopic := kmsg.NewMetadataRequestTopic()
		metaReqTopic.Topic = &t.Topic
		metaReq.Topics = append(metaReq.Topics, metaReqTopic)
		if len(metaReq.Topics) == 0 {
			die("unable to request metadata: %v", err)
		}
		for _, part := range t.Partitions {
			log.Println("At partition index", t.ID.String(), "value is", part)
			tps[indexT] = append(tps[indexT], part.Partition)
		}
	}

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
	log.Println("Size of each brokers:", brokerSizeMap)
	log.Println("Partition/Broker Info:", partInfoMap)

	leastDataSizeBroker, mostDataSizeBroker := findLessAndMostSizeBrokers(brokerSizeMap)

	fmt.Println("Broker with less data:", leastDataSizeBroker)
	fmt.Println("Broker with most data:", mostDataSizeBroker)
	largPart := findLargestPartitionInBroker(partInfoMap, mostDataSizeBroker)

	brokerIds := make([]int32, 0, len(brokerSizeMap))
	for brokerId := range brokerSizeMap {
		brokerIds = append(brokerIds, brokerId)
	}
	host := strings.Split(*seedBrokers, ":")
	if len(host) == 0 {
		die("Unable to parse hostname from seed brokers")
	}

	for j := int32(0); j < brokerIds[len(brokerIds)-2]; j++ { //ignore existing node
		isOn, topicName := isPartitionOnBroker(partInfoMap, j, largPart)

		//see if its in the less loaded broker
		if !isOn {
			fmt.Println("Not found partition:", largPart, " on broker:", j, " so moving it here")
			err := moveReplica(j, largPart, topicName, host[0])
			if err != nil {
				return
			}
			break //TODO- do we loop for next one ?
		}
		//else move to next broker
	}

}

func findLessAndMostSizeBrokers(brokerSizeMap map[int32]int64) (lessSize int32, moreSize int32) {
	brokerIds := make([]int32, 0, len(brokerSizeMap))
	for brokerid := range brokerSizeMap {
		brokerIds = append(brokerIds, brokerid)
	}
	sort.Slice(brokerIds, func(i, j int) bool {
		return brokerSizeMap[brokerIds[i]] < brokerSizeMap[brokerIds[j]]
	})
	lessDataSizeBroker := brokerIds[0]
	mostDataSizeBroker := brokerIds[len(brokerIds)-1]
	return lessDataSizeBroker, mostDataSizeBroker
}

func findLargestPartitionInBroker(pmap map[int32][]partitionInfo, brokerId int32) int32 {
	for k, v := range pmap {
		if k != brokerId {
			continue
		}
		fmt.Printf("v: %v\n", v)
		sort.Slice(v, func(i, j int) bool { return v[i].size > v[j].size })
		return v[0].partition
	}
	return -1
}

func isPartitionOnBroker(pmap map[int32][]partitionInfo, brokerId int32, part int32) (b bool, topic string) {
	for k, v := range pmap {
		if k != brokerId {
			continue
		}
		pInfo := v
		for i := 0; i < len(pInfo); i++ {
			if pInfo[0].partition == part {
				return true, pInfo[0].topicName
			}
			continue
		}
	}
	return false, ""
}
