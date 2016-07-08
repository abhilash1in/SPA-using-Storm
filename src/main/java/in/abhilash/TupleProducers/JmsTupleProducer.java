package in.abhilash.TupleProducers;

/**
 * Created by abhilash1in on 2/7/16.
 */
import java.io.Serializable;

import javax.jms.JMSException;

import ActiveMQClient.PortOpenRequest;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

/**
 * Interface to define classes that can produce a Storm <code>Values</code> objects
 * from a <code>javax.jms.Message</code> object>.
 * <p/>
 * Implementations are also responsible for declaring the output
 * fields they produce.
 * <p/>
 * If for some reason the implementation can't process a message
 * (for example if it received a <code>javax.jms.ObjectMessage</code>
 * when it was expecting a <code>javax.jms.TextMessage</code> it should
 * return <code>null</code> to indicate to the <code>JmsSpout</code> that
 * the message could not be processed.
 *
 * @author P. Taylor Goetz
 *
 */
public interface JmsTupleProducer extends Serializable{
    /**
     * Process a JMS message object to create a Values object.
     * @param request - the JMS message
     * @return the Values tuple, or null if the message couldn't be processed.
     * @throws JMSException
     */
    Values toTuple(PortOpenRequest request) throws JMSException;

    /**
     * Declare the output fields produced by this JmsTupleProducer.
     * @param declarer The OutputFieldsDeclarer for the spout.
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
