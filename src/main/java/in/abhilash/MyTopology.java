package in.abhilash;

import in.abhilash.Bolts.ValidateRequestBolt;
import in.abhilash.Bolts.ModifyFirewallBolt;
import in.abhilash.TupleProducers.JmsTupleProducer;
import in.abhilash.TupleProducers.PortOpenRequestTupleProducer;
import in.abhilash.Spouts.ActiveMQSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTopology {
    private static Logger LOG = LoggerFactory.getLogger(MyTopology.class);

    public static final String JMS_QUEUE_SPOUT = "JMS_QUEUE_SPOUT";
    public static final String VALIDATE_REQUEST_BOLT = "VALIDATE_REQUEST_BOLT";
    public static final String MODIFY_FIREWALL_BOLT = "MODIFY_FIREWALL_BOLT";


    public static void main(String[] args) {

        ActiveMQSpout queueSpout = new ActiveMQSpout();
        queueSpout.setTopicName("test");
        queueSpout.setClientId("queueSpout");

        TopologyBuilder builder = new TopologyBuilder();

        // ActiveMQ jms spout with 5 parallel instances
        builder.setSpout(JMS_QUEUE_SPOUT, queueSpout, 3);

        // intermediate bolt, subscribes to jms spout, anchors on tuples, and auto-acks
        JmsTupleProducer producer = new PortOpenRequestTupleProducer();
        ValidateRequestBolt validateRequestBolt = new ValidateRequestBolt(VALIDATE_REQUEST_BOLT, true, true);
        validateRequestBolt.setJmsTupleProducer(producer);
        builder.setBolt(VALIDATE_REQUEST_BOLT, validateRequestBolt, 3).shuffleGrouping(JMS_QUEUE_SPOUT);

        // bolt that subscribes to the intermediate bolt, and publishes to a JMS Topic
        ModifyFirewallBolt firewallBolt = new ModifyFirewallBolt(MODIFY_FIREWALL_BOLT,true,true,null);
        builder.setBolt(MODIFY_FIREWALL_BOLT,firewallBolt,1).shuffleGrouping(VALIDATE_REQUEST_BOLT);

        Config conf = new Config();

        if (args.length > 0) {
            conf.setNumWorkers(3);

            try{
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (Exception e){
                LOG.debug(e.getMessage());
                e.printStackTrace();
            }
        } else {

            conf.setDebug(true);
            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-activemq", conf, builder.createTopology());


            /*Utils.sleep(60000);
            cluster.killTopology("storm-jms-example");
            cluster.shutdown();*/
        }

    }
}
