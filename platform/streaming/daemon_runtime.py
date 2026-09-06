"""
Shared runtime bits for the platform's two Kafka consumer daemons
(stage 16): signal handling and logging setup.

Deliberately small. The two daemons stay separate modules because Decision
C keeps their ownership separate — session_consumer_daemon.py owns RAW
state (session:{id}:events, the Postgres log), refresh_daemon.py owns
DERIVED recommendations — and they run as separate processes in separate
Kafka consumer groups. Only the bits that are genuinely identical live
here; nothing about what either daemon *does* does.

## Why these daemons commit offsets when nothing else in the platform does

Every consumer written before stage 16 runs `enable.auto.commit: False`
and never commits: `session_consumer.py`'s stage 9-10 functions,
`recommendation_refresh.process_one_event()`, and both loops
`eval/8_2`'s harness drives. That is correct for those callers — each is
a bounded, one-shot or test-scoped read, and a throwaway group id per run
means "start from earliest" is exactly the wanted behaviour.

A daemon is the opposite case. It runs under the canonical group id from
config.py, restarts, and must not re-read the whole topic when it does:
`behavioral-events` is never purged, and replaying it would push
duplicate events into `session:{id}:events` (record_event rpushes) and
duplicate rows into the Postgres log. So the daemons commit manually,
synchronously, after each message is fully processed — at-least-once
delivery, the standard pattern.

This does not change any existing consumer's configuration. Commits are
per-consumer-group, and every pre-stage-16 caller uses a throwaway group
id, so none of them can observe a daemon's committed offsets.
"""
import logging
import signal
import threading

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=LOG_FORMAT)


def install_signal_handlers(stop: threading.Event, log: logging.Logger) -> None:
    """Sets `stop` on SIGINT/SIGTERM so the daemon's loop can finish the
    message it is holding and close its connections, rather than being
    torn down mid-write.

    Only installable from the main thread (Python raises otherwise), which
    is why every entry point here takes `stop` as a parameter: a test
    drives the same loop from a worker thread with its own Event and never
    touches signals.
    """
    def _handle(signum, _frame):
        log.info("received signal %s, stopping after the current message", signal.Signals(signum).name)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle)
