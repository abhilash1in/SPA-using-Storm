package ActiveMQClient;

import java.io.Serializable;
import java.time.Duration;

/**
 * Created by abhilash1in on 2/7/16.
 */
public class PortOpenRequest implements Serializable{
    private static final long serialVersionUID = 1L;
    private String username;
    private String password;
    private int requestedPort;
    private Duration duration;

    public PortOpenRequest(String username, String password, int requestedPort, Duration duration) {
        this.username = username;
        this.password = password;
        this.requestedPort = requestedPort;
        this.duration = duration;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getRequestedPort() {
        return requestedPort;
    }

    public Duration getDuration() {
        return duration;
    }
}
