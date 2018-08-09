<?php
declare(strict_types = 1);


namespace SmartWeb\NatsTest\Subscriber;

use SmartWeb\CloudEvents\Nats\Payload\PayloadInterface;
use SmartWeb\Nats\Subscriber\SubscriberInterface;

/**
 * Internal test subscriber.
 *
 * @internal
 */
class SubscriberTest implements SubscriberInterface
{
    
    /**
     * @inheritDoc
     */
    public function handle(PayloadInterface $payload) : void
    {
        \var_dump($payload);
    }
}
