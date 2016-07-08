package in.abhilash.Spouts;

import java.util.concurrent.LinkedBlockingQueue;

import javax.crypto.SealedObject;
import javax.crypto.spec.SecretKeySpec;
import javax.jms.*;

import ActiveMQClient.PortOpenRequest;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by abhilash1in on 2/7/16.
 */
public class ActiveMQSpout extends BaseRichSpout implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSpout.class);


    // JMS options
    private LinkedBlockingQueue<PortOpenRequest> queue;
    private SpoutOutputCollector collector;
    private transient Connection connection;
    private transient Session session;
    private String topicName = null;
    private String clientId = null;

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    /**
     * <code>javax.jms.MessageListener</code> implementation.
     * <p/>
     * Stored the JMS message in an internal queue for processing
     * by the <code>nextTuple()</code> method.
     */
    public void onMessage(Message message) {
        if(message!=null){
            try {
                System.out.println("Received: Client ID"+message.getStringProperty("clientid")+" Global IP + "+ message.getStringProperty("globalip")
                        +" Local IP + "+ message.getStringProperty("localip"));
            } catch (JMSException e) {
                e.printStackTrace();
            }
            LOG.debug("received message: ");
            ObjectMessage objMsg = (ObjectMessage) message;
            PortOpenRequest portOpenRequest;
            try{
                SealedObject s = (SealedObject) objMsg.getObject();
                portOpenRequest = (ActiveMQClient.PortOpenRequest) s.getObject(new SecretKeySpec("abcdefghij123456".getBytes(),"AES"));
                this.queue.offer(portOpenRequest);
                System.out.println(portOpenRequest.getUsername()+portOpenRequest.getPassword()+portOpenRequest.getRequestedPort()+portOpenRequest.getDuration().toString());
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }else{
            LOG.debug("Null message received");
        }

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.queue = new LinkedBlockingQueue<PortOpenRequest>();
        this.collector = collector;
        try {
            ConnectionFactory cf = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
            this.connection = cf.createConnection();
            if(clientId == null || clientId.isEmpty())
                throw new IllegalArgumentException("Client ID not set!");
            this.connection.setClientID(clientId);
            this.session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
            if(topicName == null || topicName.isEmpty())
                throw new IllegalArgumentException("Topic name is not set!");
            Destination dest = this.session.createQueue(topicName);
            MessageConsumer consumer = session.createConsumer(dest);
            consumer.setMessageListener(this);
            this.connection.start();
        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }
    }

    @Override
    public void close() {
        try {
            LOG.debug("Closing JMS connection.");
            this.session.close();
            this.connection.close();
        } catch (JMSException e) {
            LOG.warn("Error closing JMS connection.", e);
        }
    }

    @Override
    public void nextTuple() {
        PortOpenRequest request = this.queue.poll();
        if (request == null) {
            Utils.sleep(50);
        } else {
            LOG.debug("sending tuple: " + request);
            // perform some computation
            // generate and emit Tuples if required
            // get the tuple from the handler
            /*try {
                if(this.tupleProducer == null)
                    throw new IllegalArgumentException("Tuple producer not set. Call setJmsTupleProducer(JmsTupleProducer producer) on the spout");
                Values vals = this.tupleProducer.toTuple(request);
                if(vals != null)
                    this.collector.emit(vals);
                else
                    LOG.debug("Received null Values/Tuple");
            } catch (JMSException e) {
                LOG.warn("Unable to convert JMS message: " + request);
            }*/

            //if we're just emitting what we received
            this.collector.emit(new Values(request));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //this.tupleProducer.declareOutputFields(outputFieldsDeclarer);
        outputFieldsDeclarer.declare(new Fields("rawPortOpenRequest"));
    }
}