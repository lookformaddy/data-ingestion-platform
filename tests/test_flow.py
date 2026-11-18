"""Flow control: token bucket, bounded buffer, batch sizer."""

import threading
import time

from ingest.flow import Batch, BatchSizer, BoundedBuffer, TokenBucket


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_token_bucket_refills_with_clock():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=10, capacity=5, clock=clock)
    assert bucket.try_acquire(5) is True
    assert bucket.try_acquire(1) is False
    clock.now += 0.5  # +5 tokens
    assert bucket.try_acquire(5) is True
    assert bucket.wait_seconds(1) == 0.1  # 1 token at 10/sec, empty bucket


def test_token_bucket_acquire_blocks_until_refill():
    clock = FakeClock()
    bucket = TokenBucket(rate_per_sec=10, capacity=1, clock=clock)
    assert bucket.try_acquire(1) is True
    slept = []
    bucket.acquire(1, sleep=lambda s: (slept.append(s), setattr(clock, "now", clock.now + s))[1])
    assert slept and slept[0] == 0.1


def test_bounded_buffer_backpressure_and_drain():
    buf = BoundedBuffer(max_batches=1, put_timeout=0.05)
    assert buf.put(Batch("b1", [], {}, "s")) is True
    # Buffer full and no consumer: put blocks and counts backpressure
    # events until room frees up (or the buffer is closed).
    result: dict[str, bool | None] = {}
    t = threading.Thread(target=lambda: result.__setitem__("ok", buf.put(Batch("b2", [], {}, "s"))))
    t.start()
    time.sleep(0.2)
    assert buf.backpressure_events >= 1
    assert buf.get(timeout=0.1).batch_id == "b1"  # frees a slot
    t.join(timeout=2.0)
    assert result["ok"] is True  # b2 enqueued once room freed
    assert buf.get(timeout=0.1).batch_id == "b2"  # drain b2
    assert buf.put(Batch("b3", [], {}, "s")) is True
    buf.close()
    assert buf.get(timeout=0.1).batch_id == "b3"
    assert buf.get(timeout=0.1) is None  # drained


def test_bounded_buffer_put_returns_false_when_closed():
    buf = BoundedBuffer(max_batches=1, put_timeout=0.05)
    assert buf.put(Batch("b1", [], {}, "s")) is True
    result: dict[str, bool | None] = {}
    t = threading.Thread(target=lambda: result.__setitem__("ok", buf.put(Batch("b2", [], {}, "s"))))
    t.start()
    time.sleep(0.2)
    buf.close()  # unblocks the waiting producer
    t.join(timeout=2.0)
    assert result["ok"] is False


def test_bounded_buffer_pause_resume():
    buf = BoundedBuffer(max_batches=2)
    buf.put(Batch("b1", [], {}, "s"))
    buf.pause()
    got = []
    t = threading.Thread(target=lambda: got.append(buf.get(timeout=2.0)))
    t.start()
    time.sleep(0.15)
    assert got == []  # paused: consumer waits
    buf.resume()
    t.join(timeout=2.0)
    assert got and got[0].batch_id == "b1"
    buf.close()


def test_batch_sizer_shrinks_when_slow_grows_when_fast():
    sizer = BatchSizer(current=500, minimum=50, maximum=2000, target_latency_ms=250.0)
    assert sizer.observe(500.0) == 250
    assert sizer.observe(100.0) == 313  # 250 * 1.25 + 1
