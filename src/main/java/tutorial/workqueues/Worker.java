package tutorial.workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/*
-> Round-robin dispatching is used in case more workers exist

-> If a consumer dies (its channel is closed, connection is closed, or TCP connection is lost) without
sending an ack, RabbitMQ will understand that a message wasn't processed fully and will re-queue
it. If there are other consumers online at the same time, it will then quickly redeliver it to
another consumer.  */
public class Worker {

	private static final String TASK_QUEUE_NAME = "task_queue";

	public static void main( String[] argv ) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost( "localhost" );
		final Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();

		boolean durable = true;
		channel.queueDeclare( TASK_QUEUE_NAME, durable, false, false, null );
		System.out.println( " [*] Waiting for messages. To exit press CTRL+C" );

		// fair dispatch: accept only one unack-ed message at a time
		int prefetchCount = 1;
		channel.basicQos(prefetchCount);

		DeliverCallback deliverCallback = ( consumerTag, delivery ) -> {
			String message = new String( delivery.getBody(), "UTF-8" );

			System.out.println( " [x] Received '" + message + "'" );
			try {
				doWork( message );
			} finally {
				System.out.println( " [x] Done" );
				channel.basicAck( delivery.getEnvelope().getDeliveryTag(), false );
			}
		};
		boolean autoAck = false;
		channel.basicConsume( TASK_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {} );
	}

	private static void doWork( String task ) {
		for ( char ch : task.toCharArray() ) {
			if ( ch == '.' ) {
				try {
					Thread.sleep( 1000 );
				} catch ( InterruptedException _ignored ) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
}
