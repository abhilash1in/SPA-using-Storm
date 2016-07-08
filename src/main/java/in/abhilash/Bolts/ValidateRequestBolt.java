package in.abhilash.Bolts;

import java.util.Map;

import in.abhilash.TupleProducers.JmsTupleProducer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ActiveMQClient.PortOpenRequest;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import javax.jms.JMSException;

/**
 * Created by abhilash1in on 2/7/16.
 */

/**
 * A generic <code>backtype.storm.topology.IRichBolt</code> implementation
 * for testing/debugging the Storm JMS Spout and example topologies.
 * <p/>
 * For debugging purposes, set the log level of the
 * <code>backtype.storm.contrib.jms</code> package to DEBUG for debugging
 * output.
 * @author tgoetz
 *
 */
@SuppressWarnings("serial")
public class ValidateRequestBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateRequestBolt.class);
    private OutputCollector collector;
    private boolean autoAck = false;
    private boolean autoAnchor = false;
    private Fields declaredFields;
    private String name;
    private JmsTupleProducer tupleProducer;

    /**
     * Constructs a new <code>ValidateRequestBolt</code> instance.
     *
     * @param name The name of the bolt (used in DEBUG logging)
     * @param autoAck Whether or not this bolt should automatically acknowledge received tuples.
     * @param autoAnchor Whether or not this bolt should automatically anchor to received tuples.
     * param declaredFields The fields this bolt declares as output.
     */

    public ValidateRequestBolt(String name, boolean autoAck, boolean autoAnchor){
        this.name = name;
        this.autoAck = autoAck;
        this.autoAnchor = autoAnchor;
    }

    public void setJmsTupleProducer(JmsTupleProducer producer){
        this.tupleProducer = producer;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        LOG.debug("[" + this.name + "] Received message: " + input);
        if(input!=null){
            // only emit if we have declared fields.
            if(input instanceof PortOpenRequest){
                LOG.debug("[" + this.name + "] emitting: " + input);
            /*if(this.autoAnchor){
                this.collector.emit(input, input.getValues());
            } else{
                this.collector.emit(input.getValues());
            }*/
                try {
                    if(this.tupleProducer == null)
                        throw new IllegalArgumentException("Tuple producer not set. Call setJmsTupleProducer(JmsTupleProducer producer) on the spout");
                    Values vals = this.tupleProducer.toTuple((PortOpenRequest)input);
                    if(vals != null)
                        this.collector.emit(vals);
                    else
                        LOG.debug("Received null Values/Tuple");
                } catch (JMSException e) {
                    LOG.warn("Unable to convert JMS message: " + input);
                }
            }
            else {
                LOG.debug("[" + this.name + "] Received input which is not of type PortOpenRequest. " + input);
            }

            if(this.autoAck){
                LOG.debug("[" + this.name + "] ACKing tuple: " + input);
                this.collector.ack(input);
            }
        }
    }
    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("validatedPortOpenRequest"));
    }


}
