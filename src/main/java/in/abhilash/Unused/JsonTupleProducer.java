package in.abhilash.Unused;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Created by abhilash1in on 2/7/16.
 */
@SuppressWarnings("serial")
public class JsonTupleProducer /*implements JmsTupleProducer*/ {

    public Values toTuple(Message msg) throws JMSException {
        if(msg instanceof TextMessage){
            String json = ((TextMessage) msg).getText();
            return new Values(json);
        } else {
            return null;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

}