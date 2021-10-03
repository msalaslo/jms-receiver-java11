package com.msl.jms.receiver;

import java.util.Hashtable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSTopicReceiver implements MessageListener {
	
    // Defines the JNDI context factory.
    public final static String JNDI_FACTORY = "weblogic.jndi.WLInitialContextFactory";
    // Defines the JMS connection factory for the queue.
    public final static String JMS_FACTORY = "myFactory";

    public final static String PROVIDER_URL = "t3://localhost:7001";

    // Defines the queue.
    public final static String TOPIC = "myTopic";
    private TopicConnectionFactory topicConFactory;
    private TopicConnection tCon;
    private TopicSession tSession;
    private TopicSubscriber tSubscriver;
    private Topic topic;
    private boolean quit = false;
    
    /**
     * Message listener interface.
     * @param msg  message
     */
    public void onMessage(Message msg) {
        try {
            String msgText;
            if (msg instanceof TextMessage) {
                msgText = ((TextMessage) msg).getText();
            } else {
                msgText = msg.toString();
            }
            System.out.println("Message Received: " + msgText);
            if (msgText.equalsIgnoreCase("This is my message")) {
                synchronized(this) {
                    quit = true;
                    this.notifyAll(); // Notify main thread to quit
                }
            }
        } catch (JMSException jmse) {
            System.err.println("An exception occurred: " + jmse.getMessage());
        }
    }
    
    /**
     * Creates all the necessary objects for receiving
     * messages from a JMS queue.
     *
     * @param   ctx    JNDI initial context
     * @param    queueName    name of queue
     * @exception NamingException if operation cannot be performed
     * @exception JMSException if JMS fails to initialize due to internal error
     */
    public void init(Context ctx, String queueName)
    throws NamingException, JMSException {
    	topicConFactory = (TopicConnectionFactory) ctx.lookup(JMS_FACTORY);
        tCon = topicConFactory.createTopicConnection();
        tSession = tCon.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topic= (Topic) ctx.lookup(queueName);
        tSubscriver = tSession.createSubscriber(topic);
        tSubscriver.setMessageListener(this);
        tCon.start();
    }
    
    /**
     * Closes JMS objects.
     * @exception JMSException if JMS fails to close objects due to internal error
     */
    public void close() throws JMSException {
    	tSubscriver.close();
        tSession.close();
        tCon.close();
    }
    
    /**
     * main() method.
     *
     * @param args  WebLogic Server URL
     * @exception  Exception if execution fails
     */
    public static void main(String[] args) throws Exception {
        InitialContext ic = getInitialContext();
        JMSTopicReceiver topicReceiver = new JMSTopicReceiver();
        topicReceiver.init(ic, TOPIC);
        System.out.println(
            "JMS Topic Subcribed To Receive Messages...");
        // Wait until a "This is my message" message has been received.
        synchronized(topicReceiver) {
            while (!topicReceiver.quit) {
                try {
                	topicReceiver.wait();
                } catch (InterruptedException ie) {}
            }
        }
        topicReceiver.close();
    }
    
    private static InitialContext getInitialContext() throws NamingException {
        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_FACTORY);
        env.put(Context.PROVIDER_URL, PROVIDER_URL);
        return new InitialContext(env);
    }
}