package tutorial.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTaskCreator {
	private static final String TASK_QUEUE_NAME = "task_queue";

	public static void main( String[] argv ) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost( "localhost" );

		boolean durable = true;  //queue won't be lost even if RabbitMQ restarts.
		try ( Connection connection = factory.newConnection();
				Channel channel = connection.createChannel() ) {
			channel.queueDeclare( TASK_QUEUE_NAME, durable, false, false, null );

			String message = String.join( " ", argv );

			channel.basicPublish( "", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,
					message.getBytes( "UTF-8" ) );
			System.out.println( " [x] Sent '" + message + "'" );
		}
	}
}
