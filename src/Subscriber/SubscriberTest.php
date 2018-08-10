<?php
declare(strict_types = 1);


namespace SmartWeb\NatsTest\Subscriber;

use SmartWeb\CloudEvents\Nats\Event\EventInterface;
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
    public function handle(EventInterface $event) : void
    {
        \var_dump($event);
    }
}
