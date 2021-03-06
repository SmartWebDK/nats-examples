<?php
declare(strict_types = 1);


namespace SmartWeb\NatsTest;

use Nats\ConnectionOptions;
use NatsStreaming\Connection;
use NatsStreaming\ConnectionOptions as StreamingConnectionOptions;
use NatsStreaming\Exceptions\ConnectException;
use NatsStreaming\Exceptions\TimeoutException;
use NatsStreaming\Subscription;
use NatsStreaming\SubscriptionOptions;
use NatsStreamingProtos\StartPosition;
use SmartWeb\CloudEvents\Nats\Event\EventBuilder;
use SmartWeb\CloudEvents\Nats\Event\EventInterface;
use SmartWeb\Nats\Connection\StreamingConnection;
use SmartWeb\Nats\Connection\StreamingConnectionInterface;
use SmartWeb\Nats\Event\Serialization\EventDecoder;
use SmartWeb\Nats\Event\Serialization\EventDenormalizer;
use SmartWeb\Nats\Event\Serialization\EventNormalizer;
use SmartWeb\Nats\Message\Serialization\MessageDecoder;
use SmartWeb\Nats\Message\Serialization\MessageDenormalizer;
use SmartWeb\Nats\Message\Serialization\MessageDeserializer;
use SmartWeb\NatsTest\Subscriber\SubscriberTest;
use Symfony\Component\Serializer\Encoder\JsonEncode;
use Symfony\Component\Serializer\Serializer;

/**
 * Class Service.
 *
 * @author Nicolai Agersbæk <na@smartweb.dk>
 *
 * @internal
 */
class Service
{
    
    /**
     * @var string
     */
    private static $defaultChannelName = 'some.channel';
    
    /**
     * @var ConnectionOptions
     */
    private $natsConnectionOptions;
    
    /**
     * @var string
     */
    private $name;
    
    /**
     * @inheritDoc
     */
    public function getName() : string
    {
        return $this->name;
    }
    
    /**
     * Service constructor.
     *
     * @param string            $name
     * @param ConnectionOptions $natsConnectionOptions
     */
    public function __construct(string $name, ConnectionOptions $natsConnectionOptions)
    {
        $this->natsConnectionOptions = $natsConnectionOptions;
        $this->name = $name;
    }
    
    /**
     * @return string
     */
    private function getClusterID() : string
    {
        $clusterID = \getenv(StreamingConnectionInterface::CLUSTER_ID_KEY);
        
        $this->validateClusterID($clusterID);
        
        return $clusterID;
    }
    
    /**
     * @param $clusterID
     */
    private function validateClusterID($clusterID) : void
    {
        if (!\is_string($clusterID)) {
            $actualType = \gettype($clusterID);
            throw new \RuntimeException("Invalid cluster ID configured. Expected string, was: {$actualType}");
        }
    }
    
    /**
     * @return string
     */
    private function getClientID() : string
    {
        return \str_replace('.', '', \uniqid($this->getName(), true));
    }
    
    /**
     * @param string|null $clientID
     *
     * @throws ConnectException
     * @throws TimeoutException
     * @throws \Exception
     */
    public function runSimplePublishTest(string $clientID = null) : void
    {
        $connection = $this->createConnection($clientID);
        $connection->connect();
        
        $subject = self::$defaultChannelName;
        $data = 'Foo!';
        
        $request = $connection->publish($subject, $data);
        
        $gotAck = $request->wait();
        
        $statusResponse = $gotAck
            ? 'Acknowledged'
            : 'Not acknowledged';
        
        \printf("$statusResponse\r\n");
        
        $connection->close();
    }
    
    /**
     * @param string|null $channelName
     * @param int|null    $count
     *
     * @throws ConnectException
     * @throws TimeoutException
     */
    public function runPublishTest(string $channelName = null, int $count = null) : void
    {
        $channel = $channelName ?? self::$defaultChannelName;
        $count = $count ?? 20;
        
        $connection = $this->createConnection();
        $connection->connect();
    
        $adapter = $this->createStreamingConnection($connection);
    
    
        $data = [
            'foo' => 'bar',
        ];
        $payload = EventBuilder::create()
                               ->setEventType('some.event')
                               ->setCloudEventsVersion('0.1.0')
                               ->setSource('some.source')
                               ->setEventId('some.event.id')
                               ->setData($data)
                               ->build();
    
        for ($i = 0; $i < $count; $i ++) {
            $this->publish($adapter, $channel, $payload);
        }
        
        $connection->close();
    }
    
    /**
     * @param StreamingConnection $connection
     * @param string              $channel
     * @param EventInterface      $event
     */
    private function publish(StreamingConnection $connection, string $channel, EventInterface $event) : void
    {
        static $count = 0;
        $count++;
        
        $request = $connection->publish($channel, $event);
    
        $gotAck = $request->wait();
    
        $statusResponse = $gotAck
            ? 'Acknowledged'
            : 'Not acknowledged';
    
        \printf("{$statusResponse}({$count})\r\n");
    }
    
    /**
     * @param null|string $channel
     *
     * @throws ConnectException
     * @throws TimeoutException
     * @throws \Exception
     */
    public function runSimpleSubscribeTest(?string $channel = null) : void
    {
        $connection = $this->createConnection();
        $connection->connect();
        
        $subOptions = new SubscriptionOptions();
        $subOptions->setStartAt(StartPosition::NewOnly());
        
        $subject = $channel ?? self::$defaultChannelName;
        $callback = function ($message) {
            \printf($message);
        };
        
        $sub = $connection->subscribe($subject, $callback, $subOptions);
        $sub->wait(1);
        
        $connection->close();
    }
    
    /**
     * @param string[] $channels
     *
     * @throws ConnectException
     * @throws TimeoutException
     * @throws \Exception
     */
    public function runMultipleChannelSimpleSubscribeTest(string ...$channels) : void
    {
        $connection = $this->createConnection();
        $connection->connect();
        
        $subOptions = new SubscriptionOptions();
        $subOptions->setStartAt(StartPosition::NewOnly());
        
        $callback = function ($message) {
            \printf($message);
        };
        
        $subjects = empty($channels)
            ? [self::$defaultChannelName]
            : $channels;
        
        /** @var Subscription[] $subs */
        $subs = [];
        
        foreach ($subjects as $subject) {
            $sub = $connection->subscribe($subject, $callback, $subOptions);
            $subs[] = $sub;
        }
        
        foreach ($subs as $sub) {
            $sub->wait(1);
        }
        
        $connection->close();
    }
    
    /**
     * @param null|string $channelName
     *
     * @throws ConnectException
     * @throws TimeoutException
     * @throws \Exception
     */
    public function runSubscribeTest(?string $channelName = null) : void
    {
        $connection = $this->createConnection();
        $connection->connect();
        
        $subOptions = new SubscriptionOptions();
        $subOptions->setStartAt(StartPosition::NewOnly());
        
        $adapter = $this->createStreamingConnection($connection);
        
        $channel = $channelName ?? self::$defaultChannelName;
        
        $subscriber = new SubscriberTest(0.100);
        
        $subscription = $adapter->subscribe($channel, $subscriber, $subOptions);
        $subscription->wait(1);
        
        // not explicitly needed
        $subscription->unsubscribe(); // or $subscription->close();
        
        $connection->close();
    }
    
    /**
     * @param null|string $channelName
     *
     * @throws ConnectException
     * @throws TimeoutException
     * @throws \Exception
     */
    public function runSimpleQueueGroupSubscribeTest(?string $channelName = null) : void
    {
        $connection = $this->createConnection();
        
        $connection->connect();
        
        $subOptions = new SubscriptionOptions();
        $subOptions->setStartAt(StartPosition::NewOnly());
        
        $channel = $channelName ?? self::$defaultChannelName;
        $queue = 'some.queue';
        $callback = function ($message) {
            \printf($message);
        };
        
        $subscription = $connection->queueSubscribe($channel, $queue, $callback, $subOptions);
        
        $subscription->wait(1);
        
        // not explicitly needed
        $subscription->close(); // or $subscription->unsubscribe();
        
        $connection->close();
    }
    
    /**
     * @param null|string $channelName
     *
     * @throws ConnectException
     * @throws TimeoutException
     * @throws \Exception
     */
    public function runQueueGroupSubscribeTest(?string $channelName = null) : void
    {
        $connection = $this->createConnection();
        
        $connection->connect();
        
        $subOptions = new SubscriptionOptions();
        $subOptions->setStartAt(StartPosition::LastReceived());
        
        $adapter = $this->createStreamingConnection($connection);
        
        $channel = $channelName ?? self::$defaultChannelName;
        $queue = 'some.queue';
        
        $subscriber = new SubscriberTest(0.1);
        
        $subscription = $adapter->groupSubscribe($channel, $queue, $subscriber, $subOptions);
        
        $connection->wait();
        
        $connection->close();
    }
    
    /**
     * @param string|null $clientID
     *
     * @return Connection
     */
    private function createConnection(string $clientID = null) : Connection
    {
        $options = new StreamingConnectionOptions();
        
        $options->setNatsOptions($this->natsConnectionOptions);
        $options->setClientID($clientID ?? $this->getClientID());
        $options->setClusterID($this->getClusterID());
        
        return new Connection($options);
    }
    
    /**
     * @param Connection $connection
     *
     * @return StreamingConnection
     */
    private function createStreamingConnection(Connection $connection) : StreamingConnection
    {
        $eventNormalizer = new EventNormalizer();
        $eventEncoder = new JsonEncode();
        $eventDecoder = new EventDecoder();
        $eventDenormalizer = new EventDenormalizer();
        
        $eventSerializer = new Serializer(
            [$eventNormalizer, $eventDenormalizer],
            [$eventEncoder, $eventDecoder]
        );
        
        return new StreamingConnection(
            $connection,
            $eventSerializer
        );
    }
}
