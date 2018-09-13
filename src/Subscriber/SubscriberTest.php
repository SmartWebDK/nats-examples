<?php
declare(strict_types = 1);


namespace SmartWeb\NatsTest\Subscriber;

use SmartWeb\CloudEvents\Nats\Event\EventInterface;
use SmartWeb\Nats\Message\Acknowledge;
use SmartWeb\Nats\Subscriber\SubscriberInterface;

/**
 * Internal test subscriber.
 *
 * @internal
 */
class SubscriberTest implements SubscriberInterface
{
    
    /**
     * @var int
     */
    private $sleepDurationMs;
    
    /**
     * @var bool
     */
    private $shouldSleep = false;
    
    /**
     * @param float|null $sleepDuration
     */
    public function __construct(float $sleepDuration = null)
    {
        if ($sleepDuration !== null) {
            $this->shouldSleep = true;
            $this->sleepDurationMs = (int)(1000 * $sleepDuration);
        }
    }
    
    /**
     * @inheritDoc
     */
    public function handle(EventInterface $event) : void
    {
        \printf("Handled event: {$event->getEventType()}|{$event->getEventId()} -- {$this->getHandledCount()}\r\n");
        $this->sleep();
    }
    
    /**
     * @return Acknowledge
     */
    public function acknowledge() : Acknowledge
    {
        return Acknowledge::before();
    }
    
    /**
     * @return int
     */
    private function getHandledCount() : int
    {
        static $count = 0;
        
        return $count++;
    }
    
    private function sleep() : void
    {
        if ($this->shouldSleep) {
            \usleep($this->sleepDurationMs * 1000);
        }
    }
}
