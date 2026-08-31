"""
Stage 13: PyFlink job computing the session semantic profile centroid.

Runs INSIDE the docker-compose network (submitted via `flink run -py` from
inside the flink-jobmanager container, which has PyFlink + this stage's
deps baked in -- see Dockerfile.flink), so hostnames are the in-cluster
service names (kafka, redis), not localhost.

keyBy(session_id), EVENT TIME with a bounded-out-of-orderness watermark
(5s), SlidingEventTimeWindows(5 min, slide 30s) -- processing-time windows
would be wrong here: the event simulator (stage 12) replays sessions
faster than real time via --speed, so only event_time reflects the
session's actual pacing.

Per-window, for each session key: sort events by event_time, apply the
weight table below, apply exponential recency decay (half-life 3 events,
computed by ordinal position within the window -- "3 events" is a count,
not a duration), and write the resulting weighted centroid to
session:{id}:profile in Redis (session_state.py::profile_key() format,
reserved for this job by Decision C).

Event weights:
    complete             +1.0
    like                 +1.5
    skip, <5000ms        -1.0   (position_ms, absolute -- matches the
                                  simulator's early-skip scripting)
    skip, >80% duration  +0.8   (needs the track's duration_sec, preloaded
                                  into Redis by preload_embeddings_to_redis.py
                                  alongside the embeddings)
    skip, otherwise      -0.3
    play                 +0.2
"""
import json

from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.window import SlidingEventTimeWindows, Time

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "behavioral-events"
REDIS_HOST = "redis"
REDIS_PORT = 6379
EMBED_DIM = 512

RECORD_TYPE = Types.TUPLE(
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG()]
)


def _parse_event(raw: str):
    """raw JSON string -> (session_id, event_type, track_id, event_time_ms, position_ms).
    Non-behavioral-event / malformed messages (e.g. stage 7's own plain-string
    test payloads on the same topic) are filtered out upstream by the None check."""
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(event, dict) or "session_id" not in event or "event_time" not in event:
        return None
    from datetime import datetime

    event_time_ms = int(datetime.fromisoformat(event["event_time"]).timestamp() * 1000)
    return (
        event["session_id"],
        event.get("event_type", ""),
        str(event.get("track_id", "")),
        event_time_ms,
        int(event.get("position_ms", 0)),
    )


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[3]


class SessionProfileWindow(ProcessWindowFunction):
    def open(self, runtime_context):
        import redis

        self._redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self._embeddings = {}
        self._durations = {}
        for key in self._redis.scan_iter("track:*:embedding"):
            track_id = key.split(":")[1]
            self._embeddings[track_id] = json.loads(self._redis.get(key))
        for key in self._redis.scan_iter("track:*:duration_sec"):
            track_id = key.split(":")[1]
            self._durations[track_id] = int(self._redis.get(key))

    def _base_weight(self, event_type: str, track_id: str, position_ms: int) -> float:
        if event_type == "complete":
            return 1.0
        if event_type == "like":
            return 1.5
        if event_type == "play":
            return 0.2
        if event_type == "skip":
            if position_ms < 5000:
                return -1.0
            duration_sec = self._durations.get(track_id)
            if duration_sec and position_ms >= 0.8 * duration_sec * 1000:
                return 0.8
            return -0.3
        return 0.0

    def process(self, key, context, elements):
        events = sorted(elements, key=lambda e: e[3])
        n = len(events)
        weighted_sum = [0.0] * EMBED_DIM
        weight_total = 0.0
        for idx, (session_id, event_type, track_id, event_time_ms, position_ms) in enumerate(events):
            events_ago = n - 1 - idx
            decay = 0.5 ** (events_ago / 3.0)
            weight = self._base_weight(event_type, track_id, position_ms) * decay
            embedding = self._embeddings.get(track_id)
            if embedding is None:
                continue
            for i in range(EMBED_DIM):
                weighted_sum[i] += weight * embedding[i]
            weight_total += abs(weight)

        if weight_total > 0:
            centroid = [v / weight_total for v in weighted_sum]
            self._redis.set(f"session:{key}:profile", json.dumps(centroid))
            yield f"{key}: {n} events in window, weight_total={weight_total:.3f}, profile written"
        else:
            yield f"{key}: {n} events in window, weight_total=0, no profile written"


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # KafkaSource's Java class isn't found by the client-side py4j gateway
    # otherwise, even though the JAR is on /opt/flink/lib/ -- add_jars is
    # PyFlink's own mechanism for both client-side job-graph construction
    # and shipping the JAR with the submitted job (the CLI's -C classpath
    # flag only reaches the cluster side, which was never the failure).
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("flink-session-profile")
        # latest, not earliest: this computes a live "recent session mood"
        # signal, not a durable historical record (that's stages 9-10's
        # job, over a separate consumer group) -- a freshly (re)started
        # instance of this job has no business replaying the topic's
        # entire history to backfill session:{id}:profile keys nobody is
        # reading yet.
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "behavioral-events-source"
    )

    parsed = raw_stream.map(_parse_event, output_type=RECORD_TYPE).filter(lambda r: r is not None)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(EventTimeAssigner())

    timed = parsed.assign_timestamps_and_watermarks(watermark_strategy)

    result = (
        timed.key_by(lambda r: r[0], key_type=Types.STRING())
        .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
        .process(SessionProfileWindow(), output_type=Types.STRING())
    )

    result.print()
    env.execute("session-profile-centroid")


if __name__ == "__main__":
    main()
