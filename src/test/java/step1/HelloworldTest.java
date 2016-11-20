package step1;

import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class HelloworldTest {
	
	@Test
	public void test() throws IOException, TimeoutException, InterruptedException {
		String HOST = "localhost";
		String QUEUE_NAME = "hello";

		Sender sender = new Sender(HOST, QUEUE_NAME);
		Receiver receiver = new Receiver(HOST, QUEUE_NAME);
		
		sender.send("Hello World!");
		receiver.receive();
	}
	
	private static class RabbitMQClient {
		final Connection connection;
		final List<Channel> channelList = new ArrayList<>();
		final String queueName;

		RabbitMQClient(String host, String queueName) throws IOException, TimeoutException {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(host);
			this.connection = factory.newConnection();
			this.queueName = queueName;
		}
		
		Channel createChannel() throws IOException {
			Channel channel = connection.createChannel();
			channel.queueDeclare(queueName, false, false, false, null);
			channelList.add(channel);
			return channel;
		}

		void destroyChannel(Channel channel) throws IOException, TimeoutException {
			Channel willDestroyChannel = this.channelList.remove(this.channelList.indexOf(channel));
			willDestroyChannel.close();
		}
	}
	
	private static class Sender extends Thread {
		final RabbitMQClient client;
		final String queueName;

		Sender(String host, String queueName) throws IOException, TimeoutException {
			this.queueName = queueName;
			this.client = new RabbitMQClient(host, queueName);
		}
		Channel send(String message) throws IOException {
			Channel channel = client.createChannel();
			channel.basicPublish("", queueName, null, message.getBytes("UTF-8"));
			System.out.println(" [Sender] Sent : '" + message + "'");
			return channel;
		}
	}
	
	private static class Receiver {
		final RabbitMQClient client;
		final String queueName;

		Receiver(String host, String queueName) throws IOException, TimeoutException {
			this.queueName = queueName;
			this.client = new RabbitMQClient(host, queueName);
		}

		Channel receive() throws IOException {
			Channel channel = client.createChannel();
			Consumer consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
						throws IOException {
					String message = new String(body, "UTF-8");
					System.out.println(" [Receiver] Received '" + message + "'");
				}
			};
			channel.basicConsume(queueName, true, consumer);
			return channel;
		}
	}
}
