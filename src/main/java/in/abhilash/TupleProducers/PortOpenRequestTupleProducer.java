package in.abhilash.TupleProducers;

import ActiveMQClient.PortOpenRequest;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

import javax.jms.JMSException;

/**
 * Created by abhilash1in on 2/7/16.
 */
public class PortOpenRequestTupleProducer implements JmsTupleProducer {
    PortOpenRequest request;

    @Override
    public Values toTuple(PortOpenRequest request) throws JMSException {
        this.request = request;
        if(isValidRequest())
        {
            return new Values(request);
        }
        return null;
    }

    private Boolean isValidRequest(){
        if(validAuth() && nonExistentRequest())
            return true;
        return false;
    }

    private Boolean validAuth(){
        //validate username and password
        return true;
    }

    private Boolean nonExistentRequest(){
        //check if port is already open for that IP
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
