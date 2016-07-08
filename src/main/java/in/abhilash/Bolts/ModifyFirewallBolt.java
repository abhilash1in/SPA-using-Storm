package in.abhilash.Bolts;

import ActiveMQClient.PortOpenRequest;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Created by abhilash1in on 2/7/16.
 */
public class ModifyFirewallBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(ModifyFirewallBolt.class);
    private boolean autoAck = false;
    private boolean autoAnchor = false;
    private Fields declaredFields;
    private String name;

    private OutputCollector collector;

    public ModifyFirewallBolt(String name, boolean autoAck, boolean autoAnchor, Fields declaredFields) {
        this.autoAck = autoAck;
        this.autoAnchor = autoAnchor;
        this.declaredFields = declaredFields;
        this.name = name;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple instanceof PortOpenRequest){
            //modify firewall rules
            //open port in firewall
        }
        else{
            LOG.debug("[" + this.name + "] Received input which is not of type PortOpenRequest. " + tuple);
        }

        //testing - by just printing what is received
        LOG.debug(tuple.toString());

        if(this.autoAck){
            LOG.debug("[" + this.name + "] ACKing tuple: " + tuple);
            this.collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        if(this.declaredFields != null){
            outputFieldsDeclarer.declare(this.declaredFields);
        }
    }

}
