"""Verifies the Stage 3 L2 enrichment: CLAP embeddings in Milvus, graph in Neo4j."""
CLAP_EMBED_DIM = 512


def _track_row_count(pg_conn) -> int:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tracks")
        return cur.fetchone()[0]


def _enriched_row_count(pg_conn) -> int:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tracks WHERE enriched_at IS NOT NULL")
        return cur.fetchone()[0]


def test_every_track_is_enriched(pg_conn):
    total = _track_row_count(pg_conn)
    enriched = _enriched_row_count(pg_conn)
    assert enriched == total, f"{total - enriched} tracks missing enriched_at"


def _milvus_track_ids(milvus_collection) -> list[int]:
    # Query-based, not milvus_collection.num_entities: right after an
    # upsert, num_entities can transiently over-count until Milvus
    # compacts tombstoned segments in the background, even though a query
    # already sees the correct, deduplicated live rows.
    ids = []
    iterator = milvus_collection.query_iterator(expr="track_id >= 0", output_fields=["track_id"])
    while True:
        batch = iterator.next()
        if not batch:
            iterator.close()
            break
        ids.extend(row["track_id"] for row in batch)
    return ids


def test_milvus_vector_count_matches_tracks(pg_conn, milvus_collection):
    total = _track_row_count(pg_conn)
    milvus_ids = _milvus_track_ids(milvus_collection)
    assert len(milvus_ids) == total


def test_milvus_embedding_dim(milvus_collection):
    embedding_field = next(f for f in milvus_collection.schema.fields if f.name == "embedding")
    assert embedding_field.params["dim"] == CLAP_EMBED_DIM


def test_milvus_ids_match_postgres_ids(pg_conn, milvus_collection):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id FROM tracks")
        pg_ids = {row[0] for row in cur.fetchall()}

    milvus_ids = _milvus_track_ids(milvus_collection)
    assert len(milvus_ids) == len(set(milvus_ids)), "duplicate track_id rows in Milvus"
    assert pg_ids == set(milvus_ids)


def test_neo4j_track_node_count_matches_postgres(pg_conn, neo4j_driver):
    total = _track_row_count(pg_conn)
    with neo4j_driver.session() as session:
        result = session.run("MATCH (t:Track) RETURN count(t) AS n")
        neo4j_count = result.single()["n"]
    assert neo4j_count == total


def test_neo4j_every_track_has_an_artist(neo4j_driver):
    with neo4j_driver.session() as session:
        result = session.run(
            "MATCH (t:Track) WHERE NOT (t)<-[:PERFORMED]-(:Artist) RETURN count(t) AS n"
        )
        assert result.single()["n"] == 0
