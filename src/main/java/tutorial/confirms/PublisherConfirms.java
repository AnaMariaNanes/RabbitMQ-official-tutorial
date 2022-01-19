package tutorial.confirms;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/*
Output example:
Published 50,000 messages individually in 7,172 ms
Published 50,000 messages in batch in 1,016 ms
Published 50,000 messages and handled confirms asynchronously in 636 ms
 */
public class PublisherConfirms {

	static final int MESSAGE_COUNT = 50_000;

	static Connection createConnection() throws Exception {
		ConnectionFactory cf = new ConnectionFactory();
		cf.setHost( "localhost" );
		cf.setUsername( "guest" );
		cf.setPassword( "guest" );
		return cf.newConnection();
	}

	public static void main( String[] args ) throws Exception {
		// it significantly slows down publishing, as the confirmation of a message blocks the publishing of all subsequent messages
		publishMessagesIndividually();

		/*
		-> improves throughput drastically over waiting for a confirm for individual message
		->one drawback is that we do not know exactly what went wrong in case of failure, so we may
		have to keep a whole batch in memory to log something meaningful or to re-publish the
		messages
		-> still a synchronous solution, so it blocks the publishing of messages. */
		publishMessagesInBatch();

		/*
		-> provide a way to correlate the publishing sequence number with a message.
		-> register a confirm listener on the channel to be notified when publisher acks/nacks arrive
		to perform the appropriate actions, like logging or re-publishing a nack-ed message. The
		sequence-number-to-message correlation mechanism may also require some cleaning during this step.
		-> track the publishing sequence number before publishing a message.
		 */
		handlePublishConfirmsAsynchronously();
	}

	static void publishMessagesIndividually() throws Exception {
		try ( Connection connection = createConnection() ) {
			Channel ch = connection.createChannel();

			String queue = UUID.randomUUID().toString();
			ch.queueDeclare( queue, false, false, true, null );

			// Publisher confirms are enabled at the channel level
			ch.confirmSelect();

			long start = System.nanoTime();
			for ( int i = 0; i < MESSAGE_COUNT; i++ ) {
				String body = String.valueOf( i );
				ch.basicPublish( "", queue, null, body.getBytes() );
				ch.waitForConfirmsOrDie( 5_000 );
			}
			long end = System.nanoTime();
			System.out.format( "Published %,d messages individually in %,d ms%n", MESSAGE_COUNT,
					Duration.ofNanos( end - start ).toMillis() );
		}
	}

	static void publishMessagesInBatch() throws Exception {
		try ( Connection connection = createConnection() ) {
			Channel ch = connection.createChannel();

			String queue = UUID.randomUUID().toString();
			ch.queueDeclare( queue, false, false, true, null );

			ch.confirmSelect();

			int batchSize = 100;
			int outstandingMessageCount = 0;

			long start = System.nanoTime();
			for ( int i = 0; i < MESSAGE_COUNT; i++ ) {
				String body = String.valueOf( i );
				ch.basicPublish( "", queue, null, body.getBytes() );
				outstandingMessageCount++;

				if ( outstandingMessageCount == batchSize ) {
					ch.waitForConfirmsOrDie( 5_000 );
					outstandingMessageCount = 0;
				}
			}

			if ( outstandingMessageCount > 0 ) {
				ch.waitForConfirmsOrDie( 5_000 );
			}
			long end = System.nanoTime();
			System.out.format( "Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT,
					Duration.ofNanos( end - start ).toMillis() );
		}
	}

	static void handlePublishConfirmsAsynchronously() throws Exception {
		try ( Connection connection = createConnection() ) {
			Channel ch = connection.createChannel();

			String queue = UUID.randomUUID().toString();
			ch.queueDeclare( queue, false, false, true, null );

			ch.confirmSelect();

			ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

			ConfirmCallback cleanOutstandingConfirms = ( sequenceNumber, multiple ) -> {
				if ( multiple ) {
					ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(
							sequenceNumber, true );
					confirmed.clear();
				} else {
					outstandingConfirms.remove( sequenceNumber );
				}
			};

			ch.addConfirmListener( cleanOutstandingConfirms, ( sequenceNumber, multiple ) -> {
				String body = outstandingConfirms.get( sequenceNumber );
				System.err.format(
						"Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
						body, sequenceNumber, multiple );
				cleanOutstandingConfirms.handle( sequenceNumber, multiple );
			} );

			long start = System.nanoTime();
			for ( int i = 0; i < MESSAGE_COUNT; i++ ) {
				String body = String.valueOf( i );
				outstandingConfirms.put( ch.getNextPublishSeqNo(), body );
				ch.basicPublish( "", queue, null, body.getBytes() );
			}

			if ( !waitUntil( Duration.ofSeconds( 60 ), () -> outstandingConfirms.isEmpty() ) ) {
				throw new IllegalStateException(
						"All messages could not be confirmed in 60 seconds" );
			}

			long end = System.nanoTime();
			System.out.format(
					"Published %,d messages and handled confirms asynchronously in %,d ms%n",
					MESSAGE_COUNT, Duration.ofNanos( end - start ).toMillis() );
		}
	}

	static boolean waitUntil( Duration timeout, BooleanSupplier condition )
			throws InterruptedException {
		int waited = 0;
		while ( !condition.getAsBoolean() && waited < timeout.toMillis() ) {
			Thread.sleep( 100L );
			waited = +100;
		}
		return condition.getAsBoolean();
	}
}
