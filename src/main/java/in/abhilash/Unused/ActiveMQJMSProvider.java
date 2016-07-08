package in.abhilash.Unused;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.lang.IllegalStateException;


/**
 * Created by abhilash1in on 2/7/16.
 */
/**
 * A <code>JmsProvider</code> that uses the spring framework
 * to obtain a JMS <code>ConnectionFactory</code> and
 * <code>Desitnation</code> objects.
 * <p/>
 * The constructor takes three arguments:
 * <ol>
 * <li>A string pointing to the the spring application context file contining the JMS configuration
 * (must be on the classpath)
 * </li>
 * <li>The name of the connection factory bean</li>
 * <li>The name of the destination bean</li>
 * </ol>
 *
 *
 * @author tgoetz
 *
 */

@SuppressWarnings("serial")
public class ActiveMQJMSProvider implements JmsProvider {
    private ConnectionFactory connectionFactory;
    private Destination destination;

    /**
     * Constructs a <code>SpringJmsProvider</code> object given the name of a
     * classpath resource (the spring application context file), and the bean
     * names of a JMS connection factory and destination.
     * <p>
     * appContextClasspathResource - the spring configuration file (classpath resource)
     * connectionFactoryBean - the JMS connection factory bean name
     * destinationBean - the JMS destination bean name
     */

    public ActiveMQJMSProvider(String clientId, String topicName, String topicOrQueue) {
        try {
            this.connectionFactory = new ActiveMQConnectionFactory(
                    ActiveMQConnection.DEFAULT_BROKER_URL);

            if(topicOrQueue.equals("queue"))
            {
                //this.destination =
            }
            else if (topicOrQueue.equals("topic"))
            {
                //this.destination =
            }
            else
                throw new IllegalStateException("Illegal argument topicOrQueue");

        } catch (Exception jmse) {
            jmse.printStackTrace();
        }

    }

    public ConnectionFactory connectionFactory() throws Exception {
        return this.connectionFactory;
    }

    public Destination destination() throws Exception {
        return this.destination;
    }
}
