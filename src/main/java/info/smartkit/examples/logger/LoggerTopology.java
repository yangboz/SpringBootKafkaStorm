package info.smartkit.examples.logger;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by smartkit on 2016/12/17.
 */
public class LoggerTopology {

    private static final Logger LOG = Logger.getLogger(LoggerTopology.class);

    public static void  main(String[] args){
        args = new String[] {"localhost:2181", "storm-test-topic", "/brokers", "storm-consumer"};
        // Log program usages and exit if there are less than 4 command line arguments
        if(args.length < 4) {
            LOG.fatal("Incorrect number of arguments. Required arguments: <zk-hosts> <kafka-topic> <zk-path> <clientid>");
            System.exit(1);
        }
        // Build Spout configuration using input command line parameters
        final BrokerHosts zkrHosts = new ZkHosts(args[0]);
        final String kafkaTopic = args[1];
        final String zkRoot = args[2];
        final String clientId = args[3];
        final SpoutConfig spoutConfig = new SpoutConfig(zkrHosts,kafkaTopic,zkRoot,clientId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        // Build topology to consume message from kafka and print them on console
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        // Build topology to consume message from kafka and print them on console
        topologyBuilder.setSpout("kafka-spout",new KafkaSpout(spoutConfig));
        //Route the output of Kafka Spout to Logger bolt to log messages consumed from Kafka
        topologyBuilder.setBolt("kafka-message",new LoggerBolt()).globalGrouping("kafka-spout");
        //Route the output of Kafka Spout to Logger bolt to log messages consumed from Kafka
        final LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kafka-toology",new Config(),topologyBuilder.createTopology());
    }
}
