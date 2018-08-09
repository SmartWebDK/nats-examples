<?php
declare(strict_types = 1);


namespace SmartWeb\NatsTest;

use Nats\ConnectionOptions;
use NatsStreaming\Connection;
use NatsStreaming\ConnectionOptions as StreamingConnectionOptions;
use NatsStreaming\Subscription;
use NatsStreaming\SubscriptionOptions;
use NatsStreamingProtos\StartPosition;
use SmartWeb\CloudEvents\Nats\Payload\Data\ArrayData;
use SmartWeb\CloudEvents\Nats\Payload\PayloadBuilder;
use SmartWeb\Nats\Connection\StreamingConnection;
use SmartWeb\Nats\Connection\StreamingConnectionInterface;
use SmartWeb\Nats\Message\Serialization\MessageDecoder;
use SmartWeb\Nats\Message\Serialization\MessageDenormalizer;
use SmartWeb\Nats\Message\Serialization\MessageDeserializer;
use SmartWeb\Nats\Payload\Serialization\PayloadDecoder;
use SmartWeb\Nats\Payload\Serialization\PayloadDenormalizer;
use SmartWeb\Nats\Payload\Serialization\PayloadNormalizer;
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
     * @throws \NatsStreaming\Exceptions\ConnectException
     * @throws \NatsStreaming\Exceptions\TimeoutException
     */
    public function runSimplePublishTest(string $clientID = null) : void
    {
        $connection = $this->createConnection($clientID);
        $connection->connect();
        
        $subject = self::$defaultChannelName;
        $data = 'Foo!';
        
        $r = $connection->publish($subject, $data);
        
        $gotAck = $r->wait();
        
        $statusResponse = $gotAck
            ? 'Acknowledged'
            : 'Not acknowledged';
        
        \printf("$statusResponse\r\n");
        
        $connection->close();
    }
    
    /**
     * @param string|null $channelName
     *
     * @throws \NatsStreaming\Exceptions\ConnectException
     * @throws \NatsStreaming\Exceptions\TimeoutException
     */
    public function runPublishTest(string $channelName = null) : void
    {
        $connection = $this->createConnection();
        $connection->connect();
        
        $adapter = $this->createStreamingConnection($connection);
        
        $channel = $channelName ?? self::$defaultChannelName;
        
        $data = new ArrayData(
            [
                'foo' => 'bar',
            ]
        );
        $payload = PayloadBuilder::create()
                                 ->setEventType('some.event')
                                 ->setCloudEventsVersion('0.1.0')
                                 ->setSource('some.source')
                                 ->setEventId('some.event.id')
                                 ->setData($data)
                                 ->build();
        
        $request = $adapter->publish($channel, $payload);
        
        $gotAck = $request->wait();
        
        $statusResponse = $gotAck
            ? 'Acknowledged'
            : 'Not acknowledged';
        
        \printf("$statusResponse\r\n");
        
        $connection->close();
    }
    
    /**
     * @param null|string $channel
     *
     * @throws \NatsStreaming\Exceptions\ConnectException
     * @throws \NatsStreaming\Exceptions\TimeoutException
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
     * @throws \NatsStreaming\Exceptions\ConnectException
     * @throws \NatsStreaming\Exceptions\TimeoutException
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
     * @throws \NatsStreaming\Exceptions\ConnectException
     * @throws \NatsStreaming\Exceptions\TimeoutException
     */
    public function runSubscribeTest(?string $channelName = null) : void
    {
        $connection = $this->createConnection();
        $connection->connect();
        
        $subOptions = new SubscriptionOptions();
        $subOptions->setStartAt(StartPosition::NewOnly());
        
        $adapter = $this->createStreamingConnection($connection);
        
        $channel = $channelName ?? self::$defaultChannelName;
        
        $subscriber = new SubscriberTest();
        
        $subscription = $adapter->subscribe($channel, $subscriber, $subOptions);
        $subscription->wait(1);
        
        // not explicitly needed
        $subscription->unsubscribe(); // or $sub->close();
        
        $connection->close();
    }
    
    /**
     * @param string|null $clientID
     *
     * @throws \NatsStreaming\Exceptions\ConnectException
     * @throws \NatsStreaming\Exceptions\TimeoutException
     */
    public function runSimpleQueueGroupSubscribeTest(string $clientID = null) : void
    {
        $connection = $this->createConnection($clientID);
        
        $connection->connect();
        
        $subOptions = new SubscriptionOptions();
        $subOptions->setStartAt(StartPosition::NewOnly());
        
        $subjects = 'some.channel';
        $queue = 'some.queue';
        $callback = function ($message) {
            \printf($message);
        };
        
        $sub = $connection->queueSubscribe($subjects, $queue, $callback, $subOptions);
        
        $sub->wait(1);
        
        // not explicitly needed
        $sub->close(); // or $sub->unsubscribe();
        
        $connection->close();
        
    }
    
    public function runQueueGroupSubscribeTest() : void
    {
        // TODO: Implement runQueueGroupSubscribeTest() method.
        throw new \BadMethodCallException(__METHOD__ . ' not yet implemented!');
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
        $messageDecoder = new MessageDecoder();
        $messageDenormalizer = new MessageDenormalizer();
        
        $messageDeserializer = new MessageDeserializer($messageDecoder, $messageDenormalizer);
        
        $payloadNormalizer = new PayloadNormalizer();
        $payloadEncoder = new JsonEncode();
        $payloadDecoder = new PayloadDecoder();
        $payloadDenormalizer = new PayloadDenormalizer();
        
        $payloadSerializer = new Serializer(
            [$payloadNormalizer, $payloadDenormalizer],
            [$payloadEncoder, $payloadDecoder]
        );
        
        return new StreamingConnection(
            $connection,
            $messageDeserializer,
            $payloadSerializer
        );
    }
}