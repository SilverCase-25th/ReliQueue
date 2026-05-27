from reliqueue.queue import ReliQueue


class _NoPool:
    pass


def test_retry_delay_grows_with_attempts() -> None:
    queue = ReliQueue(_NoPool())

    first = queue.retry_delay_seconds(1)
    second = queue.retry_delay_seconds(2)
    third = queue.retry_delay_seconds(3)

    assert first >= 2
    assert second >= 4
    assert third >= 8
    assert first < second < third
