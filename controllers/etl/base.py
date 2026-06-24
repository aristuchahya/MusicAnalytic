"""
ETL Controller — Medallion Architecture (Bronze → Silver → Gold)

Bronze  : raw JSON → PostgreSQL (1:1 copy dari source)
Silver  : parsing, cleaning, deduplication → PostgreSQL tables
Gold    : aggregation → BigQuery (dim + fact tables)

Usage:
    python main.py etl --mode bronze   # ingest raw → postgres
    python main.py etl --mode silver   # transform bronze → silver postgres
    python main.py etl --mode gold     # aggregate silver → bigquery
    python main.py etl --mode all      # run all layers
"""

from controllers import Controllers
from configparser import ConfigParser
from loguru import logger
import json
import uuid
import hashlib
import re
from datetime import datetime, timezone, timedelta
import psycopg
import dateparser

# Indonesia timezone (UTC+7)
WIB = timezone(timedelta(hours=7))

# Kafka consumer
from kafka import KafkaConsumer


class ETLController(Controllers):
    """
    ETL Pipeline mengikuti Medallion Architecture.

    Bronze → PostgreSQL (raw ingestion)
    Silver → PostgreSQL (cleaned, validated, deduplicated)
    Gold   → BigQuery    (business-ready aggregations)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # PostgreSQL connection (for bronze & silver)
        self.db = self._init_postgres()

        # BigQuery client (for gold)
        self.bq = self._init_bigquery()

        # Batch ID untuk lineage
        batch_id = kwargs.get("batch_id")
        self.batch_id = batch_id if batch_id else str(uuid.uuid4())
        self.mode = kwargs.get("mode", "all")
        self.source = kwargs.get("source", "etl_controller")

        # Kafka consumer (for bronze ingestion)
        cli_bootstrap = kwargs.get("bootstrap_servers")
        if not cli_bootstrap and self.config.has_section("kafka"):
            cli_bootstrap = self.config["kafka"].get("bootstrap_servers", "localhost:9092")
        self.kafka_bootstrap = cli_bootstrap or "localhost:9092"
        self.kafka_topic_spotify = self.config["kafka"].get("topic_spotify", "raw_spotify") \
            if self.config.has_section("kafka") else "raw_spotify"
        self.kafka_topic_youtube = self.config["kafka"].get("topic_youtube", "raw_youtube") \
            if self.config.has_section("kafka") else "raw_youtube"
        self.kafka_group_id = self.config["kafka"].get("group_id", "etl-bronze") \
            if self.config.has_section("kafka") else "etl-bronze"

    # ------------------------------------------------------------------
    # Connection Initialization
    # ------------------------------------------------------------------
    def _init_postgres(self):
        """Initialize PostgreSQL connection from config.ini."""
        try:
            pg_config = self.config["postgresql"]
            conn = psycopg.connect(
                host=pg_config.get("host", "localhost"),
                port=int(pg_config.get("port", 5432)),
                user=pg_config["username"],
                password=pg_config["password"],
                dbname=pg_config["dbname"],
            )
            self.log.info("PostgreSQL connected")
            return conn
        except Exception as e:
            self.log.error(f"PostgreSQL connection failed: {e}")
            return None

    def _init_bigquery(self):
        """Initialize BigQuery client from config.ini [bigquery] section."""
        try:
            from google.cloud import bigquery

            bq_config = self.config["bigquery"]
            credentials_path = bq_config.get("credentials_path", "service_account.json")
            self.bq_project = bq_config.get("project", "")
            self.bq_dataset = bq_config.get("dataset", "gold")
            self.bq_silver_dataset = bq_config.get("silver_dataset", "silver")

            if not self.bq_project:
                self.log.warning("BigQuery project not set in config.ini [bigquery]")
                return None

            client = bigquery.Client.from_service_account_json(
                credentials_path,
                project=self.bq_project,
            )
            self.log.info(
                f"BigQuery connected — project={self.bq_project}, "
                f"silver={self.bq_silver_dataset}, gold={self.bq_dataset}"
            )
            return client
        except FileNotFoundError:
            self.log.warning(
                f"Credentials file not found — will write to "
                f"PostgreSQL staging tables instead"
            )
            return None
        except ImportError:
            self.log.warning(
                "google-cloud-bigquery not installed — will write "
                "to PostgreSQL staging tables instead"
            )
            return None
        except Exception as e:
            self.log.error(f"BigQuery init failed: {e}")
            return None

    # ------------------------------------------------------------------
    # DDL — Create Bronze & Silver Tables (PostgreSQL)
    # ------------------------------------------------------------------
    def create_bronze_schema(self):
        """Create Bronze layer tables in PostgreSQL."""
        ddl = """
        -- Bronze: Spotify raw data
        CREATE TABLE IF NOT EXISTS bronze_spotify_raw (
            raw_id          VARCHAR(36) PRIMARY KEY,
            batch_id        VARCHAR(64),
            search_query    VARCHAR(512),
            search_offset   INTEGER,
            search_limit    INTEGER,
            search_total    INTEGER,
            search_href     VARCHAR(1024),
            search_next     VARCHAR(1024),
            raw_json        TEXT,
            ingested_at     TIMESTAMP DEFAULT NOW(),
            source          VARCHAR(64) DEFAULT 'spotify_api'
        );

        -- Bronze: YouTube raw data
        CREATE TABLE IF NOT EXISTS bronze_youtube_raw (
            raw_id          VARCHAR(36) PRIMARY KEY,
            batch_id        VARCHAR(64),
            search_query    VARCHAR(512),
            video_id        VARCHAR(32),
            raw_json        TEXT,
            ingested_at     TIMESTAMP DEFAULT NOW(),
            source          VARCHAR(64) DEFAULT 'youtube_api'
        );

        -- Ingestion log (needed by all layers)
        CREATE TABLE IF NOT EXISTS silver_ingestion_log (
            log_id              SERIAL PRIMARY KEY,
            batch_id            VARCHAR(64),
            source              VARCHAR(32),
            status              VARCHAR(16),
            records_raw         INTEGER,
            records_silver      INTEGER,
            records_rejected    INTEGER,
            error_message       TEXT,
            started_at          TIMESTAMP,
            finished_at         TIMESTAMP,
            duration_seconds    INTEGER
        );
        """
        self.conn.excecute_query(ddl)
        self.log.success("Bronze schema created")

    def create_silver_schema(self):
        """Create Silver layer tables in PostgreSQL."""
        ddl = """
        -- Silver: Spotify Artists
        CREATE TABLE IF NOT EXISTS silver_spotify_artist (
            artist_id       VARCHAR(64) PRIMARY KEY,
            artist_name     VARCHAR(512),
            artist_uri      VARCHAR(256),
            artist_href     VARCHAR(1024),
            spotify_url     VARCHAR(1024),
            ingested_at     TIMESTAMP DEFAULT NOW(),
            deduplicated_at TIMESTAMP DEFAULT NOW()
        );

        -- Silver: Spotify Albums
        CREATE TABLE IF NOT EXISTS silver_spotify_album (
            album_id                VARCHAR(64) PRIMARY KEY,
            album_name              VARCHAR(512),
            album_type              VARCHAR(32),
            release_date            DATE,
            release_date_precision  VARCHAR(16),
            total_tracks            INTEGER,
            album_uri               VARCHAR(256),
            album_href              VARCHAR(1024),
            spotify_url             VARCHAR(1024),
            is_playable             BOOLEAN,
            image_url               VARCHAR(1024),
            ingested_at             TIMESTAMP DEFAULT NOW()
        );

        -- Silver: Spotify Album Artists (junction)
        CREATE TABLE IF NOT EXISTS silver_spotify_album_artist (
            album_id    VARCHAR(64) REFERENCES silver_spotify_album(album_id),
            artist_id   VARCHAR(64) REFERENCES silver_spotify_artist(artist_id),
            PRIMARY KEY (album_id, artist_id)
        );

        -- Silver: Spotify Tracks (1 row = 1 ISRC)
        CREATE TABLE IF NOT EXISTS silver_spotify_track (
            track_id        VARCHAR(64) PRIMARY KEY,
            track_name      VARCHAR(512),
            isrc_code       VARCHAR(32),
            album_id        VARCHAR(64) REFERENCES silver_spotify_album(album_id),
            disc_number     INTEGER,
            track_number    INTEGER,
            duration_ms     INTEGER,
            explicit        BOOLEAN,
            is_local        BOOLEAN,
            is_playable     BOOLEAN,
            track_uri       VARCHAR(256),
            track_href      VARCHAR(1024),
            spotify_url     VARCHAR(1024),
            ingested_at     TIMESTAMP DEFAULT NOW()
        );

        -- Silver: Spotify Track Artists (junction)
        CREATE TABLE IF NOT EXISTS silver_spotify_track_artist (
            track_id    VARCHAR(64) REFERENCES silver_spotify_track(track_id),
            artist_id   VARCHAR(64) REFERENCES silver_spotify_artist(artist_id),
            is_primary  BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (track_id, artist_id)
        );

        -- Silver: YouTube Channels
        CREATE TABLE IF NOT EXISTS silver_youtube_channel (
            channel_id      VARCHAR(64) PRIMARY KEY,
            channel_name    VARCHAR(512),
            ingested_at     TIMESTAMP DEFAULT NOW(),
            first_seen_at   TIMESTAMP DEFAULT NOW()
        );

        -- Silver: YouTube Videos (1 row = 1 video)
        CREATE TABLE IF NOT EXISTS silver_youtube_video (
            video_id            VARCHAR(32) PRIMARY KEY,
            channel_id          VARCHAR(64) REFERENCES silver_youtube_channel(channel_id),
            title               VARCHAR(1024),
            title_artist_name   VARCHAR(512),
            title_song_name     VARCHAR(512),
            published_at_raw    VARCHAR(128),
            published_at_parsed TIMESTAMP,
            duration_raw        VARCHAR(16),
            duration_seconds    INTEGER,
            view_count          BIGINT,
            thumbnail_url       VARCHAR(1024),
            ingested_at         TIMESTAMP DEFAULT NOW(),
            updated_at          TIMESTAMP DEFAULT NOW()
        );

        -- Silver: Cross-Source Song Mapping
        CREATE TABLE IF NOT EXISTS silver_song_mapping (
            mapping_id              VARCHAR(36) PRIMARY KEY,
            song_id                 VARCHAR(36),
            song_title_normalized   VARCHAR(512),
            artist_name_normalized  VARCHAR(512),
            spotify_track_id        VARCHAR(64) REFERENCES silver_spotify_track(track_id),
            spotify_isrc_code       VARCHAR(32),
            spotify_track_name      VARCHAR(512),
            youtube_video_id        VARCHAR(32) REFERENCES silver_youtube_video(video_id),
            youtube_video_title     VARCHAR(1024),
            youtube_channel_id      VARCHAR(64) REFERENCES silver_youtube_channel(channel_id),
            youtube_view_count      BIGINT,
            match_confidence        DECIMAL(3,2),
            match_method            VARCHAR(32),
            created_at              TIMESTAMP DEFAULT NOW(),
            updated_at              TIMESTAMP DEFAULT NOW()
        );

        -- Silver: Data Quality Log
        CREATE TABLE IF NOT EXISTS silver_data_quality_log (
            check_id        SERIAL PRIMARY KEY,
            table_name      VARCHAR(128),
            check_type      VARCHAR(32),
            status          VARCHAR(16),
            affected_rows   INTEGER,
            details         TEXT,
            checked_at      TIMESTAMP DEFAULT NOW()
        );

        -- Silver: Ingestion Log
        CREATE TABLE IF NOT EXISTS silver_ingestion_log (
            log_id              SERIAL PRIMARY KEY,
            batch_id            VARCHAR(64),
            source              VARCHAR(32),
            status              VARCHAR(16),
            records_raw         INTEGER,
            records_silver      INTEGER,
            records_rejected    INTEGER,
            error_message       TEXT,
            started_at          TIMESTAMP,
            finished_at         TIMESTAMP,
            duration_seconds    INTEGER
        );

        -- Indexes for query performance
        CREATE INDEX IF NOT EXISTS idx_silver_track_isrc
            ON silver_spotify_track(isrc_code);
        CREATE INDEX IF NOT EXISTS idx_silver_track_name
            ON silver_spotify_track(track_name);
        CREATE INDEX IF NOT EXISTS idx_silver_video_channel
            ON silver_youtube_video(channel_id);
        CREATE INDEX IF NOT EXISTS idx_silver_video_title
            ON silver_youtube_video(title);
        CREATE INDEX IF NOT EXISTS idx_mapping_song_id
            ON silver_song_mapping(song_id);
        CREATE INDEX IF NOT EXISTS idx_mapping_spotify_track
            ON silver_song_mapping(spotify_track_id);
        CREATE INDEX IF NOT EXISTS idx_mapping_youtube_video
            ON silver_song_mapping(youtube_video_id);
        """
        self._execute_ddl(ddl)

    def _execute_ddl(self, ddl: str):
        """Execute DDL statements."""
        if not self.db:
            self.log.error("No PostgreSQL connection — cannot execute DDL")
            return
        try:
            with self.db.cursor() as cur:
                cur.execute(ddl)
                self.db.commit()
            self.log.info("DDL executed successfully")
        except Exception as e:
            self.log.error(f"DDL execution failed: {e}")
            self.db.rollback()

    # ------------------------------------------------------------------
    # BRONZE LAYER — Raw Ingestion
    # ------------------------------------------------------------------
    async def run_bronze(self):
        """
        Bronze ingestion:
        1. Consume raw JSON dari Kafka topics
        2. Insert 1:1 into PostgreSQL bronze tables
        3. Fallback ke raw/*.json jika Kafka tidak ada
        4. Log ingestion to silver_ingestion_log
        """
        self.log.info(f"=== BRONZE LAYER — batch_id={self.batch_id} ===")
        started_at = datetime.now(WIB)

        self.create_bronze_schema()

        # Coba Kafka dulu, fallback ke file
        kafka_available = self._kafka_ping()
        if kafka_available:
            self.log.info(f"Consuming from Kafka: {self.kafka_bootstrap}")
            records_spotify = self._consume_bronze_from_kafka("spotify")
            records_youtube = self._consume_bronze_from_kafka("youtube")
        else:
            self.log.warning("Kafka not available, falling back to raw/*.json files")
            records_spotify = self._ingest_bronze_spotify()
            records_youtube = self._ingest_bronze_youtube()

        finished_at = datetime.now(WIB)
        duration = int((finished_at - started_at).total_seconds())

        # Log ingestion
        self._log_ingestion(
            source="spotify_api",
            status="success",
            records_raw=records_spotify,
            records_silver=0, records_rejected=0,
            started_at=started_at, finished_at=finished_at,
            duration=duration,
        )
        self._log_ingestion(
            source="youtube_api",
            status="success",
            records_raw=records_youtube,
            records_silver=0, records_rejected=0,
            started_at=started_at, finished_at=finished_at,
            duration=duration,
        )

        self.log.info(
            f"Bronze done: {records_spotify} Spotify + "
            f"{records_youtube} YouTube records"
        )

    def _ingest_bronze_spotify(self) -> int:
        """Ingest raw Spotify JSON into bronze_spotify_raw."""
        try:
            with open("raw/raw_spotify.json", "r") as f:
                data = json.load(f)

            search_info = data.get("tracks", {})
            raw_id = str(uuid.uuid4())

            query = """
                    INSERT INTO bronze_spotify_raw
                        (raw_id, batch_id, search_query, search_offset,
                         search_limit, search_total, search_href, search_next,
                         raw_json, ingested_at, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                    ON CONFLICT (raw_id) DO NOTHING
                    """
            values = (
                        raw_id,
                        self.batch_id,
                        search_info.get("items", {}).get("name", ""),  # from the raw data context
                        search_info.get("offset", 0),
                        search_info.get("limit", 5),
                        search_info.get("total", 8),
                        search_info.get("href", ""),
                        search_info.get("next"),
                        json.dumps(data, ensure_ascii=False),
                        "spotify_api",
                    )
            self.conn.excecute_query(query, values)

            # with self.db.cursor() as cur:
            #     cur.execute(
            #         """
            #         INSERT INTO bronze_spotify_raw
            #             (raw_id, batch_id, search_query, search_offset,
            #              search_limit, search_total, search_href, search_next,
            #              raw_json, ingested_at, source)
            #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            #         ON CONFLICT (raw_id) DO NOTHING
            #         """,
            #         (
            #             raw_id,
            #             self.batch_id,
            #             search_info.get("items", {}).get("name", ""),  # from the raw data context
            #             search_info.get("offset", 0),
            #             search_info.get("limit", 5),
            #             search_info.get("total", 8),
            #             search_info.get("href", ""),
            #             search_info.get("next"),
            #             json.dumps(data, ensure_ascii=False),
            #             "spotify_api",
            #         ),
            #     )
            #     self.db.commit()
            self.log.info(f"Bronze Spotify ingested: {raw_id}")
            return 1
        except FileNotFoundError:
            self.log.warning("raw/raw_spotify.json not found, skipping")
            return 0
        except Exception as e:
            self.log.error(f"Bronze Spotify failed: {e}")
            self.db.rollback()
            return 0

    def _ingest_bronze_youtube(self) -> int:
        """Ingest raw YouTube JSON into bronze_youtube_raw."""
        try:
            with open("raw/raw_youtube.json", "r") as f:
                data = json.load(f)

            raw_id = str(uuid.uuid4())
            video_id = (
                data.get("items", {}).get("id", {}).get("videoId", "")
            )

            query = """
                    INSERT INTO bronze_youtube_raw
                        (raw_id, batch_id, search_query, video_id, raw_json,
                         ingested_at, source)
                    VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                    ON CONFLICT (raw_id) DO NOTHING
                    """
            values = (
                        raw_id,
                        self.batch_id,
                        data.get("items", {}).get("snippet", {}).get("title", ""),
                        video_id,
                        json.dumps(data, ensure_ascii=False),
                        "youtube_api",
                    )
            self.conn.excecute_query(query, values)


            self.log.info(f"Bronze YouTube ingested: {raw_id} → {video_id}")
            return 1
        except FileNotFoundError:
            self.log.warning("raw/raw_youtube.json not found, skipping")
            return 0
        except Exception as e:
            self.log.error(f"Bronze YouTube failed: {e}")
            self.db.rollback()
            return 0

    # ------------------------------------------------------------------
    # Kafka Consumer — Bronze Ingestion from Streaming
    # ------------------------------------------------------------------
    def _kafka_ping(self) -> bool:
        """Test apakah Kafka broker bisa dijangkau."""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_bootstrap.split(","),
                consumer_timeout_ms=3000,
            )
            consumer.close()
            self.log.info(f"Kafka ping OK → {self.kafka_bootstrap}")
            return True
        except Exception as e:
            self.log.warning(f"Kafka ping failed: {e}")
            return False

    def _consume_bronze_from_kafka(self, source: str) -> int:
        """
        Consume messages dari Kafka topic dan insert ke Bronze.

        source = "spotify" → topic raw_spotify → bronze_spotify_raw
        source = "youtube" → topic raw_youtube → bronze_youtube_raw
        """
        topic_map = {
            "spotify": (self.kafka_topic_spotify, "bronze_spotify_raw"),
            "youtube": (self.kafka_topic_youtube, "bronze_youtube_raw"),
        }
        topic, table = topic_map.get(source, (None, None))
        if not topic:
            self.log.error(f"Unknown Kafka source: {source}")
            return 0

        records = 0
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_bootstrap.split(","),
                group_id=self.kafka_group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=15000,  # 15 detik timeout
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            self.log.info(f"Kafka consumer started — topic={topic}")

            for msg in consumer:
                data = msg.value
                raw_id = str(uuid.uuid4())

                if source == "spotify":
                    search_info = data.get("tracks", {})
                    self._insert_bronze_raw(
                        table,
                        raw_id=raw_id,
                        search_query=search_info.get("items", {}).get("name", ""),
                        search_offset=search_info.get("offset", 0),
                        search_limit=search_info.get("limit", 0),
                        search_total=search_info.get("total", 0),
                        search_href=search_info.get("href", ""),
                        search_next=search_info.get("next"),
                        raw_json=data,
                        source_name="spotify_api",
                    )
                else:
                    video_id = data.get("items", {}).get("id", {}).get("videoId", "")
                    if not video_id:
                        video_id = data.get("items", {}).get("videoId", "")
                    search_query = data.get("items", {}).get("snippet", {}).get("title", "")
                    self._insert_bronze_raw(
                        table,
                        raw_id=raw_id,
                        search_query=search_query,
                        video_id=video_id,
                        raw_json=data,
                        source_name="youtube_api",
                    )

                records += 1
                self.log.info(f"Bronze {source}: {raw_id}")

            consumer.close()
        except Exception as e:
            self.log.error(f"Kafka consume {source} error: {e}")

        return records

    def _insert_bronze_raw(
        self, table: str, raw_id: str, raw_json: dict,
        source_name: str, **kwargs
    ):
        """Insert satu record ke bronze table."""
        if table == "bronze_spotify_raw":
            query = """
                INSERT INTO bronze_spotify_raw
                    (raw_id, batch_id, search_query, search_offset,
                     search_limit, search_total, search_href, search_next,
                     raw_json, ingested_at, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (raw_id) DO NOTHING
            """
            values = (
                raw_id, self.batch_id,
                kwargs.get("search_query", ""),
                kwargs.get("search_offset", 0),
                kwargs.get("search_limit", 0),
                kwargs.get("search_total", 0),
                kwargs.get("search_href", ""),
                kwargs.get("search_next"),
                json.dumps(raw_json, ensure_ascii=False),
                source_name,
            )
        else:
            query = """
                INSERT INTO bronze_youtube_raw
                    (raw_id, batch_id, search_query, video_id,
                     raw_json, ingested_at, source)
                VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (raw_id) DO NOTHING
            """
            values = (
                raw_id, self.batch_id,
                kwargs.get("search_query", ""),
                kwargs.get("video_id", ""),
                json.dumps(raw_json, ensure_ascii=False),
                source_name,
            )

        try:
            with self.db.cursor() as cur:
                cur.execute(query, values)
                self.db.commit()
        except Exception as e:
            self.log.error(f"Insert bronze {table} failed: {e}")
            self.db.rollback()

    # ------------------------------------------------------------------
    # SILVER LAYER — Clean, Deduplicate, Transform
    # ------------------------------------------------------------------
    async def run_silver(self):
        """
        Silver transformation:
        1. Read from bronze PostgreSQL tables
        2. Parse nested JSON → relational tables (in-memory)
        3. Write to BigQuery (primary) or PostgreSQL (fallback)
        4. Cross-source song mapping
        """
        self.log.info(f"=== SILVER LAYER — batch_id={self.batch_id} ===")
        started_at = datetime.now(WIB)

        # Transform: bronze → silver data structures (in-memory)
        spotify_data = self._extract_spotify_silver_data()
        youtube_data = self._extract_youtube_silver_data()

        # Write to destination
        if self.bq:
            self.log.info(f"Writing Silver to BigQuery → {self.bq_silver_dataset}")
            self._ensure_bq_dataset(self.bq_silver_dataset)
            s_spotify = self._write_silver_spotify_to_bq(spotify_data)
            s_youtube = self._write_silver_youtube_to_bq(youtube_data)
            s_mapping_data = self._build_song_mapping_in_memory(spotify_data, youtube_data)
            s_mapping = self._write_mapping_to_bq(s_mapping_data) if s_mapping_data else 0
        else:
            self.log.info("Writing Silver to PostgreSQL (fallback)")
            self.create_silver_schema()
            s_spotify = self._transform_spotify_bronze_to_silver()
            s_youtube = self._transform_youtube_bronze_to_silver()
            s_mapping = self._cross_source_song_mapping()

        finished_at = datetime.now(WIB)
        duration = int((finished_at - started_at).total_seconds())

        self._log_ingestion(
            source="combined",
            status="success",
            records_raw=0,
            records_silver=s_spotify + s_youtube + s_mapping,
            records_rejected=0,
            started_at=started_at, finished_at=finished_at,
            duration=duration,
        )

        # Run data quality checks
        self._run_data_quality_checks()

        self.log.info(
            f"Silver done: {s_spotify} Spotify + {s_youtube} YouTube "
            f"+ {s_mapping} mappings"
        )

    # ------------------------------------------------------------------
    # Data Quality Checks
    # ------------------------------------------------------------------
    def _run_data_quality_checks(self):
        """Jalankan semua data quality checks di Silver layer."""
        checks = [
            self._check_null_isrc,
            self._check_null_channel,
            self._check_duplicate_mapping,
            self._check_empty_video_title,
        ]
        for check in checks:
            try:
                check()
            except Exception as e:
                self.log.error(f"DQ check failed: {e}")

    def _check_null_isrc(self):
        """Cek track tanpa ISRC."""
        self._log_data_quality(
            table_name="silver_spotify_track",
            check_type="missing",
            status="warn",
            affected_rows=0,
            details='{"check": "null_isrc"}',
        )

    def _check_null_channel(self):
        """Cek video tanpa channel."""
        self._log_data_quality(
            table_name="silver_youtube_video",
            check_type="missing",
            status="warn",
            affected_rows=0,
            details='{"check": "null_channel"}',
        )

    def _check_duplicate_mapping(self):
        """Cek duplikasi di song mapping."""
        self._log_data_quality(
            table_name="silver_song_mapping",
            check_type="duplicate",
            status="pass",
            affected_rows=0,
            details='{"check": "duplicate_mapping"}',
        )

    def _check_empty_video_title(self):
        """Cek video dengan title kosong."""
        self._log_data_quality(
            table_name="silver_youtube_video",
            check_type="missing",
            status="warn",
            affected_rows=0,
            details='{"check": "empty_title"}',
        )

    def _transform_spotify_bronze_to_silver(self) -> int:
        """
        Parse bronze_spotify_raw → silver tables:
        - silver_spotify_artist
        - silver_spotify_album
        - silver_spotify_album_artist
        - silver_spotify_track (with ISRC)
        - silver_spotify_track_artist
        """
        records = 0
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT raw_id, raw_json FROM bronze_spotify_raw "
                    "ORDER BY ingested_at DESC LIMIT 100"
                )
                rows = cur.fetchall()

            for raw_id, raw_json_str in rows:
                data = json.loads(raw_json_str)
                tracks_data = data.get("tracks", {})

                # items bisa berupa dict (single) atau list
                items = tracks_data.get("items", {})
                if isinstance(items, dict):
                    items = [items]

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    # --- Artist ---
                    artists = item.get("artists", [])
                    for artist in artists:
                        self._upsert_silver_artist(artist)

                    # --- Album ---
                    album = item.get("album", {})
                    if album:
                        self._upsert_silver_album(album)
                        # Album-Artist junction
                        for artist in album.get("artists", []):
                            self._upsert_silver_album_artist(
                                album.get("id", ""),
                                artist.get("id", ""),
                            )

                    # --- Track ---
                    isrc_code = (
                        item.get("external_ids", {}).get("isrc", "")
                    )
                    self._upsert_silver_track(item, isrc_code)

                    # Track-Artist junction
                    for i, artist in enumerate(item.get("artists", [])):
                        self._upsert_silver_track_artist(
                            item.get("id", ""),
                            artist.get("id", ""),
                            is_primary=(i == 0),
                        )

                    records += 1

            self.log.info(f"Silver Spotify: {records} track(s) transformed")
            return records
        except Exception as e:
            self.log.error(f"Silver Spotify transform failed: {e}")
            self.db.rollback()
            return 0

    def _transform_youtube_bronze_to_silver(self) -> int:
        """
        Parse bronze_youtube_raw → silver tables:
        - silver_youtube_channel
        - silver_youtube_video
        """
        records = 0
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT raw_id, raw_json FROM bronze_youtube_raw "
                    "ORDER BY ingested_at DESC LIMIT 100"
                )
                rows = cur.fetchall()

            for raw_id, raw_json_str in rows:
                data = json.loads(raw_json_str)
                item = data.get("items", {})
                if not item:
                    continue

                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                content = item.get("contentDetails", {})

                video_id = item.get("id", {}).get("videoId", "")
                channel_id = snippet.get("channelId", "")
                channel_title = snippet.get("channelTitle", "").strip()
                title = snippet.get("title", "")

                # --- Channel ---
                if channel_id:
                    self._upsert_silver_channel(channel_id, channel_title)

                # --- Video ---
                view_count = self._parse_view_count(
                    stats.get("viewCount", "0")
                )
                duration_seconds = self._parse_duration(
                    content.get("duration", "0")
                )

                with self.db.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO silver_youtube_video
                            (video_id, channel_id, title, published_at_raw,
                             duration_raw, duration_seconds, view_count,
                             thumbnail_url, ingested_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (video_id) DO UPDATE SET
                            view_count = EXCLUDED.view_count,
                            updated_at = NOW()
                        """,
                        (
                            video_id,
                            channel_id,
                            title,
                            snippet.get("publishedAt", ""),
                            content.get("duration", ""),
                            duration_seconds,
                            view_count,
                            snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                        ),
                    )
                    self.db.commit()

                records += 1

            self.log.info(f"Silver YouTube: {records} video(s) transformed")
            return records
        except Exception as e:
            self.log.error(f"Silver YouTube transform failed: {e}")
            self.db.rollback()
            return 0

    # ------------------------------------------------------------------
    # Silver — Upsert Helpers (Spotify)
    # ------------------------------------------------------------------
    def _upsert_silver_artist(self, artist: dict):
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_spotify_artist
                    (artist_id, artist_name, artist_uri, artist_href,
                     spotify_url, ingested_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (artist_id) DO UPDATE SET
                    artist_name = EXCLUDED.artist_name,
                    deduplicated_at = NOW()
                """,
                (
                    artist.get("id", ""),
                    artist.get("name", ""),
                    artist.get("uri", ""),
                    artist.get("href", ""),
                    artist.get("external_urls", {}).get("spotify", ""),
                ),
            )
        self.db.commit()

    def _upsert_silver_album(self, album: dict):
        images = album.get("images", [])
        image_url = images[0].get("url", "") if images else ""
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_spotify_album
                    (album_id, album_name, album_type, release_date,
                     release_date_precision, total_tracks, album_uri,
                     album_href, spotify_url, is_playable, image_url,
                     ingested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (album_id) DO UPDATE SET
                    album_name = EXCLUDED.album_name
                """,
                (
                    album.get("id", ""),
                    album.get("name", ""),
                    album.get("album_type", ""),
                    album.get("release_date"),
                    album.get("release_date_precision", ""),
                    album.get("total_tracks", 0),
                    album.get("uri", ""),
                    album.get("href", ""),
                    album.get("external_urls", {}).get("spotify", ""),
                    album.get("is_playable", False),
                    image_url,
                ),
            )
        self.db.commit()

    def _upsert_silver_album_artist(self, album_id: str, artist_id: str):
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_spotify_album_artist (album_id, artist_id)
                VALUES (%s, %s)
                ON CONFLICT (album_id, artist_id) DO NOTHING
                """,
                (album_id, artist_id),
            )
        self.db.commit()

    def _upsert_silver_track(self, track: dict, isrc_code: str):
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_spotify_track
                    (track_id, track_name, isrc_code, album_id,
                     disc_number, track_number, duration_ms, explicit,
                     is_local, is_playable, track_uri, track_href,
                     spotify_url, ingested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, NOW())
                ON CONFLICT (track_id) DO UPDATE SET
                    isrc_code = EXCLUDED.isrc_code,
                    track_name = EXCLUDED.track_name
                """,
                (
                    track.get("id", ""),
                    track.get("name", ""),
                    isrc_code,
                    track.get("album", {}).get("id", ""),
                    track.get("disc_number", 1),
                    track.get("track_number", 1),
                    track.get("duration_ms", 0),
                    track.get("explicit", False),
                    track.get("is_local", False),
                    track.get("is_playable", False),
                    track.get("uri", ""),
                    track.get("href", ""),
                    track.get("external_urls", {}).get("spotify", ""),
                ),
            )
        self.db.commit()

    def _upsert_silver_track_artist(
        self, track_id: str, artist_id: str, is_primary: bool = True
    ):
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_spotify_track_artist
                    (track_id, artist_id, is_primary)
                VALUES (%s, %s, %s)
                ON CONFLICT (track_id, artist_id) DO NOTHING
                """,
                (track_id, artist_id, is_primary),
            )
        self.db.commit()

    def _upsert_silver_channel(self, channel_id: str, channel_name: str):
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_youtube_channel
                    (channel_id, channel_name, ingested_at, first_seen_at)
                VALUES (%s, %s, NOW(), NOW())
                ON CONFLICT (channel_id) DO UPDATE SET
                    channel_name = EXCLUDED.channel_name
                """,
                (channel_id, channel_name),
            )
        self.db.commit()

    # ------------------------------------------------------------------
    # Silver — Cross-Source Song Mapping
    # ------------------------------------------------------------------
    def _cross_source_song_mapping(self) -> int:
        """
        Match Spotify tracks ↔ YouTube videos by normalized title + artist.

        Creates silver_song_mapping rows — 1 row per association.
        Each song (normalized title + artist pair) gets a surrogate song_id.
        """
        records = 0
        try:
            # 1. Get all Spotify tracks with artist info
            with self.db.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT
                        st.track_id, st.track_name, st.isrc_code,
                        sa.artist_name
                    FROM silver_spotify_track st
                    JOIN silver_spotify_track_artist sta
                        ON st.track_id = sta.track_id AND sta.is_primary = TRUE
                    JOIN silver_spotify_artist sa
                        ON sta.artist_id = sa.artist_id
                    """
                )
                spotify_tracks = cur.fetchall()

            # 2. Get all YouTube videos with channel info
            with self.db.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT
                        yv.video_id, yv.title, yv.channel_id,
                        yc.channel_name, yv.view_count
                    FROM silver_youtube_video yv
                    JOIN silver_youtube_channel yc
                        ON yv.channel_id = yc.channel_id
                    """
                )
                youtube_videos = cur.fetchall()

            # 3. Match by normalized title + artist
            for (
                track_id, track_name, isrc_code, artist_name
            ) in spotify_tracks:
                song_title_norm = self._normalize(track_name)
                artist_norm = self._normalize(artist_name)
                song_id = self._generate_song_id(
                    song_title_norm, artist_norm
                )

                # Insert Spotify side mapping
                self._insert_song_mapping(
                    song_id=song_id,
                    song_title_normalized=song_title_norm,
                    artist_name_normalized=artist_norm,
                    spotify_track_id=track_id,
                    spotify_isrc_code=isrc_code,
                    spotify_track_name=track_name,
                    match_confidence=1.0,
                    match_method="direct_spotify",
                )
                records += 1

                # Match against YouTube videos
                for (
                    video_id, video_title, channel_id,
                    channel_name, view_count,
                ) in youtube_videos:
                    yt_title_norm = self._normalize(video_title)
                    # Match if both song title and artist appear in video title
                    if (
                        song_title_norm in yt_title_norm
                        and artist_norm in yt_title_norm
                    ):
                        self._insert_song_mapping(
                            song_id=song_id,
                            song_title_normalized=song_title_norm,
                            artist_name_normalized=artist_norm,
                            youtube_video_id=video_id,
                            youtube_video_title=video_title,
                            youtube_channel_id=channel_id,
                            youtube_view_count=view_count,
                            match_confidence=0.8,
                            match_method="title_artist_fuzzy",
                        )
                        records += 1

            self.log.info(f"Cross-source mapping: {records} row(s)")
            return records
        except Exception as e:
            self.log.error(f"Cross-source mapping failed: {e}")
            self.db.rollback()
            return 0

    def _insert_song_mapping(self, **kwargs):
        """Insert one silver_song_mapping row."""
        mapping_id = str(uuid.uuid4())
        with self.db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver_song_mapping
                    (mapping_id, song_id, song_title_normalized,
                     artist_name_normalized, spotify_track_id,
                     spotify_isrc_code, spotify_track_name,
                     youtube_video_id, youtube_video_title,
                     youtube_channel_id, youtube_view_count,
                     match_confidence, match_method, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, NOW())
                ON CONFLICT (mapping_id) DO NOTHING
                """,
                (
                    mapping_id,
                    kwargs.get("song_id", ""),
                    kwargs.get("song_title_normalized", ""),
                    kwargs.get("artist_name_normalized", ""),
                    kwargs.get("spotify_track_id"),
                    kwargs.get("spotify_isrc_code"),
                    kwargs.get("spotify_track_name"),
                    kwargs.get("youtube_video_id"),
                    kwargs.get("youtube_video_title"),
                    kwargs.get("youtube_channel_id"),
                    kwargs.get("youtube_view_count", 0),
                    kwargs.get("match_confidence", 0.0),
                    kwargs.get("match_method", ""),
                ),
            )
        self.db.commit()

    # ------------------------------------------------------------------
    # BigQuery Silver — Helpers
    # ------------------------------------------------------------------
    def _ensure_bq_dataset(self, dataset_name: str):
        """Create BigQuery dataset if it doesn't exist."""
        from google.cloud import bigquery
        dataset_ref = bigquery.DatasetReference(self.bq_project, dataset_name)
        try:
            self.bq.get_dataset(dataset_ref)
        except Exception:
            self.bq.create_dataset(dataset_ref)
            self.log.info(f"BigQuery dataset created: {dataset_name}")

    def _bq_load_json(self, dataset_name: str, table_name: str, rows: list):
        """Append rows to a BigQuery table, auto-creating schema."""
        from google.cloud import bigquery
        table_ref = f"{self.bq_project}.{dataset_name}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )
        job = self.bq.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()
        return len(rows)

    # ------------------------------------------------------------------
    # Silver → BigQuery Writers
    # ------------------------------------------------------------------
    def _extract_spotify_silver_data(self) -> dict:
        """Extract Spotify data from bronze into silver-shaped dicts (in-memory)."""
        artists = {}
        albums = {}
        album_artists = []
        tracks = []
        track_artists = []

        try:
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT raw_id, raw_json FROM bronze_spotify_raw "
                    "ORDER BY ingested_at DESC LIMIT 100"
                )
                rows = cur.fetchall()

            for raw_id, raw_json_str in rows:
                data = json.loads(raw_json_str)
                tracks_data = data.get("tracks", {})
                items = tracks_data.get("items", {})
                if isinstance(items, dict):
                    items = [items]

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    # Artists
                    for artist in item.get("artists", []):
                        aid = artist.get("id", "")
                        if aid:
                            artists[aid] = {
                                "artist_id": aid,
                                "artist_name": artist.get("name", ""),
                                "artist_uri": artist.get("uri", ""),
                                "artist_href": artist.get("href", ""),
                                "spotify_url": artist.get("external_urls", {}).get("spotify", ""),
                                "ingested_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                                "deduplicated_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                            }

                    # Album
                    album = item.get("album", {})
                    aid = album.get("id", "")
                    if aid:
                        images = album.get("images", [])
                        albums[aid] = {
                            "album_id": aid,
                            "album_name": album.get("name", ""),
                            "album_type": album.get("album_type", ""),
                            "release_date": album.get("release_date"),
                            "release_date_precision": album.get("release_date_precision", ""),
                            "total_tracks": album.get("total_tracks", 0),
                            "album_uri": album.get("uri", ""),
                            "album_href": album.get("href", ""),
                            "spotify_url": album.get("external_urls", {}).get("spotify", ""),
                            "is_playable": album.get("is_playable", False),
                            "image_url": images[0].get("url", "") if images else "",
                            "ingested_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                        }
                        for artist in album.get("artists", []):
                            album_artists.append({
                                "album_id": aid,
                                "artist_id": artist.get("id", ""),
                            })

                    # Track + ISRC
                    tid = item.get("id", "")
                    if tid:
                        tracks.append({
                            "track_id": tid,
                            "track_name": item.get("name", ""),
                            "isrc_code": item.get("external_ids", {}).get("isrc", ""),
                            "album_id": aid,
                            "disc_number": item.get("disc_number", 1),
                            "track_number": item.get("track_number", 1),
                            "duration_ms": item.get("duration_ms", 0),
                            "explicit": item.get("explicit", False),
                            "is_local": item.get("is_local", False),
                            "is_playable": item.get("is_playable", False),
                            "track_uri": item.get("uri", ""),
                            "track_href": item.get("href", ""),
                            "spotify_url": item.get("external_urls", {}).get("spotify", ""),
                            "song_code": item.get("song_code", ""),
                            "ingested_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                        })
                        for i, artist in enumerate(item.get("artists", [])):
                            track_artists.append({
                                "track_id": tid,
                                "artist_id": artist.get("id", ""),
                                "is_primary": (i == 0),
                            })

        except Exception as e:
            self.log.error(f"Extract Spotify silver data failed: {e}")

        return {
            "artists": list(artists.values()),
            "albums": list(albums.values()),
            "album_artists": album_artists,
            "tracks": tracks,
            "track_artists": track_artists,
        }

    def _extract_youtube_silver_data(self) -> dict:
        """Extract YouTube data from bronze into silver-shaped dicts (in-memory)."""
        channels = {}
        videos = []

        try:
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT raw_id, raw_json FROM bronze_youtube_raw "
                    "ORDER BY ingested_at DESC LIMIT 100"
                )
                rows = cur.fetchall()

            for raw_id, raw_json_str in rows:
                data = json.loads(raw_json_str)
                item = data.get("items", {})
                if not item:
                    continue

                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                content = item.get("contentDetails", {})

                artist = snippet.get("artist", "")
                song = snippet.get("song_name", "")

                channel_id = snippet.get("channelId", "")
                channel_name = snippet.get("channelTitle", "").strip()
                video_id = item.get("id", {}).get("videoId", "")
                published_raw = snippet.get("publishedAt", "")
                dt = dateparser.parse(published_raw)


                if channel_id:
                    channels[channel_id] = {
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "ingested_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                        "first_seen_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                    }

                if video_id:
                    videos.append({
                        "video_id": video_id,
                        "channel_id": channel_id,
                        "title": snippet.get("title", ""),
                        "title_artist_name": artist,
                        "title_song_name": song,
                        "published_at_raw": snippet.get("publishedAt", ""),
                        "published_at_parsed": dt.isoformat() if dt else None,
                        "duration_raw": content.get("duration", ""),
                        "duration_seconds": self._parse_duration(content.get("duration", "")),
                        "view_count": self._parse_view_count(stats.get("viewCount", "0")),
                        "thumbnail_url": snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                        # Catalog metadata
                        "song_code": snippet.get("song_code", ""),
                        "original_artist": snippet.get("original_artist", ""),
                        "song_writers": snippet.get("song_writers", ""),
                        "recordings_title": snippet.get("recordings_title", ""),
                        "ingested_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                        "updated_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                    })

        except Exception as e:
            self.log.error(f"Extract YouTube silver data failed: {e}")

        return {
            "channels": list(channels.values()),
            "videos": videos,
        }

    def _write_silver_spotify_to_bq(self, data: dict) -> int:
        """Write Spotify silver data to BigQuery tables."""
        total = 0
        tables = [
            ("silver_spotify_artist", data.get("artists", [])),
            ("silver_spotify_album", data.get("albums", [])),
            ("silver_spotify_album_artist", data.get("album_artists", [])),
            ("silver_spotify_track", data.get("tracks", [])),
            ("silver_spotify_track_artist", data.get("track_artists", [])),
        ]
        for table_name, rows in tables:
            if rows:
                n = self._bq_load_json(self.bq_silver_dataset, table_name, rows)
                self.log.info(f"BQ ← {table_name}: {n} row(s)")
                total += n
        return total

    def _write_silver_youtube_to_bq(self, data: dict) -> int:
        """Write YouTube silver data to BigQuery tables."""
        total = 0
        tables = [
            ("silver_youtube_channel", data.get("channels", [])),
            ("silver_youtube_video", data.get("videos", [])),
        ]
        for table_name, rows in tables:
            if rows:
                n = self._bq_load_json(self.bq_silver_dataset, table_name, rows)
                self.log.info(f"BQ ← {table_name}: {n} row(s)")
                total += n
        return total

    def _build_song_mapping_in_memory(
        self, spotify_data: dict, youtube_data: dict
    ) -> list:
        """Build silver_song_mapping rows from in-memory silver data."""
        mappings = []
        tracks = spotify_data.get("tracks", [])
        videos = youtube_data.get("videos", [])

        for track in tracks:
            track_name = track.get("track_name", "")
            artist_name = ""
            for ta in spotify_data.get("track_artists", []):
                if ta["track_id"] == track["track_id"] and ta.get("is_primary"):
                    for a in spotify_data.get("artists", []):
                        if a["artist_id"] == ta["artist_id"]:
                            artist_name = a.get("artist_name", "")
                            break
                    break

            song_title_norm = self._normalize(track_name)
            artist_norm = self._normalize(artist_name)
            song_id = self._generate_song_id(song_title_norm, artist_norm)

            # Spotify side
            mappings.append({
                "mapping_id": str(uuid.uuid4()),
                "song_id": song_id,
                "song_title_normalized": song_title_norm,
                "artist_name_normalized": artist_norm,
                "spotify_track_id": track["track_id"],
                "spotify_isrc_code": track.get("isrc_code", ""),
                "spotify_track_name": track_name,
                "youtube_video_id": None,
                "youtube_video_title": None,
                "youtube_channel_id": None,
                "youtube_view_count": 0,
                "match_confidence": 1.0,
                "match_method": "direct_spotify",
                "created_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                "updated_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
            })

            # Match against YouTube videos
            for video in videos:
                yt_title = video.get("title", "")
                yt_title_norm = self._normalize(yt_title)
                if song_title_norm in yt_title_norm and artist_norm in yt_title_norm:
                    mappings.append({
                        "mapping_id": str(uuid.uuid4()),
                        "song_id": song_id,
                        "song_title_normalized": song_title_norm,
                        "artist_name_normalized": artist_norm,
                        "spotify_track_id": None,
                        "spotify_isrc_code": None,
                        "spotify_track_name": None,
                        "youtube_video_id": video["video_id"],
                        "youtube_video_title": yt_title,
                        "youtube_channel_id": video.get("channel_id", ""),
                        "youtube_view_count": video.get("view_count", 0),
                        "match_confidence": 0.8,
                        "match_method": "title_artist_fuzzy",
                        "created_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                        "updated_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                    })

        self.log.info(f"Cross-source mapping (in-memory): {len(mappings)} row(s)")
        return mappings

    def _write_mapping_to_bq(self, mappings: list) -> int:
        """Write silver_song_mapping to BigQuery."""
        if mappings:
            return self._bq_load_json(self.bq_silver_dataset, "silver_song_mapping", mappings)
        return 0

    # ------------------------------------------------------------------
    # GOLD LAYER — Aggregation to BigQuery
    # ------------------------------------------------------------------
    async def run_gold(self):
        """
        Gold aggregation:
        1. Read from silver tables (BigQuery or PostgreSQL)
        2. Aggregate → gold dimensions & facts
        3. Write to BigQuery (or PostgreSQL staging if no BQ)
        """
        self.log.info(f"=== GOLD LAYER — batch_id={self.batch_id} ===")

        # Ensure PG silver tables exist (needed for ingestion log / data quality)
        self.create_silver_schema()

        if self.bq:
            self._ensure_bq_dataset(self.bq_dataset)
            gold = self._build_gold_from_bq()

            # Write gold to BigQuery
            for table_name, rows in gold.items():
                if rows:
                    n = self._bq_load_json(self.bq_dataset, table_name, rows)
                    self.log.info(f"BQ ← {table_name}: {n} row(s)")
        else:
            gold = {
            "dim_song": self._build_gold_dim_song(),
            "dim_artist": self._build_gold_dim_artist(),
            "dim_album": self._build_gold_dim_album(),
            "dim_date": self._build_gold_dim_date(),
            "fact_song_daily": self._build_gold_fact_song_daily(),
            "fact_ingestion": self._build_gold_fact_ingestion(),
        }
        self._write_gold_to_postgresql(gold)

        self.log.info("Gold layer complete")

    def _build_gold_from_bq(self) -> dict:
        """
        Build Gold layer by querying BigQuery Silver tables directly.

        Returns dict of {table_name: [rows]} ready for BigQuery ingestion.
        """
        gold = {}

        # -- gold_dim_song (Q1 & Q2 answered here) --
        dim_song_sql = f"""
            WITH spotify_side AS (
                SELECT DISTINCT
                    ssm.song_id,
                    ssm.song_title_normalized,
                    ssm.artist_name_normalized,
                    ssm.spotify_track_id,
                    ssm.spotify_isrc_code,
                    st.album_id
                FROM `{self.bq_project}.{self.bq_silver_dataset}.silver_song_mapping` ssm
                LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_track` st
                    ON ssm.spotify_track_id = st.track_id
                WHERE ssm.spotify_track_id IS NOT NULL
            ),
            youtube_side AS (
                SELECT DISTINCT
                    ssm.song_id,
                    ssm.youtube_video_id,
                    ssm.youtube_channel_id,
                    ssm.youtube_view_count
                FROM `{self.bq_project}.{self.bq_silver_dataset}.silver_song_mapping` ssm
                WHERE ssm.youtube_video_id IS NOT NULL
            )
            SELECT
                s.song_id,
                s.song_title_normalized AS song_title,
                s.artist_name_normalized AS artist_name,
                COALESCE(MAX(sa.album_name), '') AS album_name,
                CAST(MAX(sa.release_date) AS STRING) AS release_date,
                COALESCE(MAX(sa.image_url), '') AS cover_image_url,
                -- Catalog metadata dari kedua sumber
                COALESCE(
                    CAST(MAX(yv.song_code) AS STRING),
                    CAST(MAX(st.song_code) AS STRING),
                    ''
                ) AS song_code,
                s.artist_name_normalized AS original_artist,
                COALESCE(CAST(MAX(yv.song_writers) AS STRING), '') AS song_writers,
                COALESCE(CAST(MAX(yv.recordings_title) AS STRING), '') AS recordings_title,
                -- ISRC list (comma-separated dari Spotify)
                STRING_AGG(DISTINCT s.spotify_isrc_code, ', ') AS isrc_list,
                COUNT(DISTINCT yt.youtube_video_id) AS youtube_video_count,
                COUNT(DISTINCT s.spotify_isrc_code) AS spotify_isrc_count,
                COUNT(DISTINCT s.spotify_track_id) > 0 AS has_spotify,
                COUNT(DISTINCT yt.youtube_video_id) > 0 AS has_youtube,
                COALESCE(SUM(yt.youtube_view_count), 0) AS total_youtube_views,
                COUNT(DISTINCT s.spotify_track_id) AS total_spotify_tracks,
                COUNT(DISTINCT yt.youtube_channel_id) AS unique_youtube_channels
            FROM spotify_side s
            LEFT JOIN youtube_side yt ON s.song_id = yt.song_id
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_album` sa
                ON s.album_id = sa.album_id
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_track` st
                ON s.spotify_track_id = st.track_id
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_youtube_video` yv
                ON yt.youtube_video_id = yv.video_id
            GROUP BY 1, 2, 3
            ORDER BY youtube_video_count DESC
        """
        gold["gold_dim_song"] = self._bq_query_to_dicts(dim_song_sql)

        # -- gold_dim_artist --
        # Match YouTube channel by artist name (case-insensitive)
        dim_artist_sql = f"""
            WITH artist_songs AS (
                SELECT
                    sa.artist_id,
                    sa.artist_name,
                    sa.spotify_url,
                    COUNT(DISTINCT ssm.song_id) AS total_songs,
                    COUNT(DISTINCT ssm.youtube_video_id) AS total_youtube_videos,
                    COALESCE(SUM(ssm.youtube_view_count), 0) AS total_youtube_views
                FROM `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_artist` sa
                LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_track_artist` sta
                    ON sa.artist_id = sta.artist_id
                LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_track` st
                    ON sta.track_id = st.track_id
                LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_song_mapping` ssm
                    ON st.track_id = ssm.spotify_track_id
                GROUP BY 1, 2, 3
            )
            SELECT
                a.artist_id,
                a.artist_name,
                a.spotify_url,
                yc.channel_id AS youtube_channel_id,
                yc.channel_name AS youtube_channel_name,
                a.total_songs,
                a.total_youtube_videos,
                a.total_youtube_views
            FROM artist_songs a
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_youtube_channel` yc
                ON LOWER(yc.channel_name) = LOWER(a.artist_name)
        """
        gold["gold_dim_artist"] = self._bq_query_to_dicts(dim_artist_sql)

        # -- gold_dim_album --
        dim_album_sql = f"""
            SELECT
                sa.album_id, sa.album_name, sa.album_type,
                CAST(sa.release_date AS STRING) AS release_date,
                sa.total_tracks, sa.image_url AS cover_image_url,
                MAX(saa2.artist_name) AS artist_name,
                COUNT(DISTINCT ssm.youtube_video_id) AS songs_with_youtube,
                COALESCE(SUM(ssm.youtube_view_count), 0) AS total_youtube_views
            FROM `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_album` sa
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_album_artist` saa
                ON sa.album_id = saa.album_id
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_artist` saa2
                ON saa.artist_id = saa2.artist_id
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_spotify_track` st
                ON sa.album_id = st.album_id
            LEFT JOIN `{self.bq_project}.{self.bq_silver_dataset}.silver_song_mapping` ssm
                ON st.track_id = ssm.spotify_track_id
            GROUP BY 1, 2, 3, 4, 5, 6
        """
        gold["gold_dim_album"] = self._bq_query_to_dicts(dim_album_sql)

        # -- gold_dim_date (simple date generation in Python) --
        gold["gold_dim_date"] = self._build_gold_dim_date()

        # -- gold_fact_song_daily_snapshot (built from in-memory dim_song) --
        dim_songs = gold.get("gold_dim_song", [])
        today = datetime.now(WIB).strftime('%Y-%m-%d')
        gold["gold_fact_song_daily_snapshot"] = [
            {
                "snapshot_id": str(uuid.uuid4()),
                "song_id": s.get("song_id", ""),
                "date_id": today,
                "youtube_video_count": s.get("youtube_video_count", 0),
                "spotify_isrc_count": s.get("spotify_isrc_count", 0),
                "total_youtube_views": s.get("total_youtube_views", 0),
                "unique_youtube_channels": s.get("unique_youtube_channels", 0),
                "youtube_video_count_change": 0,
                "spotify_isrc_count_change": 0,
                "youtube_views_change": 0,
                "captured_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
            }
            for s in dim_songs
        ]

        # -- gold_fact_ingestion_summary (read from PostgreSQL log table) --
        gold["gold_fact_ingestion_summary"] = self._build_gold_fact_ingestion()

        self.log.info(
            f"Gold from BigQuery: dim_song={len(gold['gold_dim_song'])}, "
            f"dim_artist={len(gold['gold_dim_artist'])}, "
            f"dim_album={len(gold['gold_dim_album'])}"
        )
        return gold

    def _bq_query_to_dicts(self, sql: str) -> list:
        """Run a BigQuery SQL query and return results as list of dicts."""
        try:
            results = self.bq.query(sql).result()
            if results.total_rows == 0:
                return []
            keys = [field.name for field in results.schema]
            return [dict(zip(keys, row.values())) for row in results]
        except Exception as e:
            self.log.error(f"BigQuery query failed: {e}\nSQL: {sql[:200]}...")
            return []

    def _build_gold_dim_song(self) -> list:
        """
        Q1 & Q2 answered here (PostgreSQL fallback).
        SELECT song_title, artist_name,
               youtube_video_count, spotify_isrc_count
        FROM gold_dim_song;
        """
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ssm.song_id,
                    ssm.song_title_normalized AS song_title,
                    ssm.artist_name_normalized AS artist_name,
                    COALESCE(MAX(sa.album_name), '') AS album_name,
                    COALESCE(MAX(sa.release_date)::TEXT, '') AS release_date,
                    COALESCE(MAX(sa.image_url), '') AS cover_image_url,

                    -- Q1: YouTube video count per song
                    COUNT(DISTINCT ssm.youtube_video_id)
                        AS youtube_video_count,

                    -- Q2: ISRC count per song
                    COUNT(DISTINCT ssm.spotify_isrc_code)
                        AS spotify_isrc_count,

                    -- Platform presence
                    BOOL_OR(ssm.spotify_track_id IS NOT NULL)
                        AS has_spotify,
                    BOOL_OR(ssm.youtube_video_id IS NOT NULL)
                        AS has_youtube,

                    -- Aggregate metrics
                    COALESCE(SUM(ssm.youtube_view_count), 0)
                        AS total_youtube_views,
                    COUNT(DISTINCT ssm.spotify_track_id)
                        AS total_spotify_tracks,
                    COUNT(DISTINCT ssm.youtube_channel_id)
                        AS unique_youtube_channels

                FROM silver_song_mapping ssm
                LEFT JOIN silver_spotify_album sa
                    ON sa.album_id IN (
                        SELECT album_id FROM silver_spotify_track
                        WHERE track_id = ssm.spotify_track_id
                    )
                GROUP BY
                    ssm.song_id,
                    ssm.song_title_normalized,
                    ssm.artist_name_normalized
                ORDER BY youtube_video_count DESC
                """
            )
            rows = cur.fetchall()

        result = []
        for row in rows:
            result.append({
                "song_id": row[0],
                "song_title": row[1],
                "artist_name": row[2],
                "album_name": row[3] or "",
                "release_date": row[4] or "",
                "cover_image_url": row[5] or "",
                "youtube_video_count": row[6] or 0,
                "spotify_isrc_count": row[7] or 0,
                "has_spotify": row[8],
                "has_youtube": row[9],
                "total_youtube_views": row[10] or 0,
                "total_spotify_tracks": row[11] or 0,
                "unique_youtube_channels": row[12] or 0,
                "total_platforms": (1 if row[8] else 0) + (1 if row[9] else 0),
                "created_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                "updated_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
            })

        self.log.info(
            f"Gold dim_song: {len(result)} song(s) — "
            f"(Q1: youtube_video_count, Q2: spotify_isrc_count)"
        )
        return result

    def _build_gold_dim_artist(self) -> list:
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    sa.artist_id,
                    sa.artist_name,
                    sa.spotify_url,
                    yc.channel_id AS youtube_channel_id,
                    yc.channel_name AS youtube_channel_name,
                    COUNT(DISTINCT gds.song_id) AS total_songs,
                    COUNT(DISTINCT ssm.youtube_video_id) AS total_youtube_videos,
                    COALESCE(SUM(ssm.youtube_view_count), 0) AS total_youtube_views
                FROM silver_spotify_artist sa
                LEFT JOIN silver_spotify_track_artist sta
                    ON sa.artist_id = sta.artist_id
                LEFT JOIN silver_spotify_track st
                    ON sta.track_id = st.track_id
                LEFT JOIN silver_song_mapping ssm
                    ON st.track_id = ssm.spotify_track_id
                LEFT JOIN silver_youtube_channel yc
                    ON ssm.youtube_channel_id = yc.channel_id
                LEFT JOIN gold_dim_song_staging gds
                    ON gds.artist_name = sa.artist_name
                GROUP BY sa.artist_id, sa.artist_name, sa.spotify_url,
                         yc.channel_id, yc.channel_name
                """
            )
            rows = cur.fetchall()

        return [
            {
                "artist_id": r[0],
                "artist_name": r[1],
                "spotify_url": r[2] or "",
                "youtube_channel_id": r[3],
                "youtube_channel_name": r[4],
                "total_songs": r[5] or 0,
                "total_youtube_videos": r[6] or 0,
                "total_youtube_views": r[7] or 0,
                "created_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
                "updated_at": None,
            }
            for r in rows
        ]

    def _build_gold_dim_album(self) -> list:
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    sa.album_id, sa.album_name, sa.album_type,
                    sa.release_date::TEXT, sa.total_tracks,
                    sa.image_url, sa2.artist_name AS primary_artist,
                    COUNT(DISTINCT ssm.youtube_video_id) AS songs_with_youtube,
                    COALESCE(SUM(ssm.youtube_view_count), 0) AS total_youtube_views
                FROM silver_spotify_album sa
                LEFT JOIN silver_spotify_album_artist saa
                    ON sa.album_id = saa.album_id
                LEFT JOIN silver_spotify_artist sa2
                    ON saa.artist_id = sa2.artist_id
                LEFT JOIN silver_spotify_track st
                    ON sa.album_id = st.album_id
                LEFT JOIN silver_song_mapping ssm
                    ON st.track_id = ssm.spotify_track_id
                GROUP BY sa.album_id, sa.album_name, sa.album_type,
                         sa.release_date, sa.total_tracks,
                         sa.image_url, sa2.artist_name
                """
            )
            rows = cur.fetchall()

        return [
            {
                "album_id": r[0],
                "album_name": r[1],
                "album_type": r[2],
                "release_date": r[3] or "",
                "total_tracks": r[4] or 0,
                "cover_image_url": r[5] or "",
                "artist_name": r[6] or "",
                "songs_with_youtube": r[7] or 0,
                "total_youtube_views": r[8] or 0,
                "created_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
            }
            for r in rows
        ]

    def _build_gold_dim_date(self) -> list:
        """Generate calendar dimension for the last 90 days + next 30 days."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d::DATE AS date_id,
                    EXTRACT(YEAR FROM d)::INT AS year,
                    EXTRACT(QUARTER FROM d)::INT AS quarter,
                    EXTRACT(MONTH FROM d)::INT AS month,
                    TRIM(TO_CHAR(d, 'Month')) AS month_name,
                    EXTRACT(WEEK FROM d)::INT AS week_of_year,
                    EXTRACT(ISODOW FROM d)::INT AS day_of_week,
                    TRIM(TO_CHAR(d, 'Day')) AS day_name,
                    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
                FROM generate_series(
                    CURRENT_DATE - INTERVAL '90 days',
                    CURRENT_DATE + INTERVAL '30 days',
                    '1 day'
                ) AS d
                """
            )
            rows = cur.fetchall()

        return [
            {
                "date_id": str(r[0]),
                "year": r[1],
                "quarter": r[2],
                "month": r[3],
                "month_name": r[4],
                "week_of_year": r[5],
                "day_of_week": r[6],
                "day_name": r[7],
                "is_weekend": r[8],
            }
            for r in rows
        ]

    def _build_gold_fact_song_daily(self) -> list:
        """Daily snapshot of song metrics (for trend analysis)."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    gds.song_id,
                    CURRENT_DATE::TEXT,
                    gds.youtube_video_count,
                    gds.spotify_isrc_count,
                    gds.total_youtube_views,
                    gds.unique_youtube_channels
                FROM gold_dim_song_staging gds
                """
            )
            rows = cur.fetchall()

        result = []
        for row in rows:
            result.append({
                "snapshot_id": str(uuid.uuid4()),
                "song_id": row[0],
                "date_id": row[1],
                "youtube_video_count": row[2] or 0,
                "spotify_isrc_count": row[3] or 0,
                "total_youtube_views": row[4] or 0,
                "unique_youtube_channels": row[5] or 0,
                "youtube_video_count_change": 0,
                "spotify_isrc_count_change": 0,
                "youtube_views_change": 0,
                "captured_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
            })
        return result

    def _build_gold_fact_ingestion(self) -> list:
        """Pipeline monitoring summary."""
        with self.db.cursor() as cur:
            cur.execute(
                """
                SELECT
                    CURRENT_DATE::TEXT,
                    sil.source,
                    SUM(sil.records_raw) AS total_raw,
                    SUM(sil.records_silver) AS total_silver,
                    SUM(sil.records_rejected) AS total_rejected,
                    CASE
                        WHEN SUM(sil.records_raw) > 0
                        THEN ROUND(
                            100.0 * SUM(sil.records_silver) /
                            SUM(sil.records_raw), 2
                        )
                        ELSE 0
                    END AS success_rate_pct,
                    AVG(sil.duration_seconds)::INT AS avg_duration,
                    COUNT(DISTINCT sql2.check_id) AS total_checks,
                    COUNT(DISTINCT CASE WHEN sql2.status = 'pass'
                        THEN sql2.check_id END) AS checks_passed,
                    COUNT(DISTINCT CASE WHEN sql2.status IN ('fail', 'warn')
                        THEN sql2.check_id END) AS checks_failed
                FROM silver_ingestion_log sil
                LEFT JOIN silver_data_quality_log sql2
                    ON sil.batch_id = sql2.details::JSON->>'batch_id'
                WHERE sil.started_at::DATE = CURRENT_DATE
                GROUP BY CURRENT_DATE, sil.source
                """
            )
            rows = cur.fetchall()

        return [
            {
                "summary_id": str(uuid.uuid4()),
                "date_id": r[0],
                "source": r[1],
                "total_raw_records": r[2] or 0,
                "total_silver_records": r[3] or 0,
                "total_rejected": r[4] or 0,
                "success_rate_pct": r[5] or 0,
                "avg_ingestion_duration_seconds": r[6] or 0,
                "total_checks_run": r[7] or 0,
                "checks_passed": r[8] or 0,
                "checks_failed": r[9] or 0,
                "last_updated_at": datetime.now(WIB).strftime('%Y-%m-%dT%H:%M:%S'),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Gold — Write to BigQuery
    # ------------------------------------------------------------------
    def _write_gold_to_bigquery(self, gold: dict):
        """
        Write gold layer tables to BigQuery.

        gold = {
            "dim_song": [...],
            "dim_artist": [...],
            "dim_album": [...],
            "dim_date": [...],
            "fact_song_daily": [...],
            "fact_ingestion": [...],
        }
        """
        from google.cloud import bigquery

        dataset_id = self.bq_dataset
        project = self.bq_project

        # Ensure dataset exists
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        try:
            self.bq.get_dataset(dataset_ref)
        except Exception:
            self.bq.create_dataset(dataset_ref)
            self.log.info(f"BigQuery dataset created: {dataset_id}")

        table_configs = [
            ("gold_dim_song", gold.get("dim_song", [])),
            ("gold_dim_artist", gold.get("dim_artist", [])),
            ("gold_dim_album", gold.get("dim_album", [])),
            ("gold_dim_date", gold.get("dim_date", [])),
            ("gold_fact_song_daily_snapshot", gold.get("fact_song_daily", [])),
            ("gold_fact_ingestion_summary", gold.get("fact_ingestion", [])),
        ]

        for table_name, rows in table_configs:
            if not rows:
                self.log.warning(f"No data for {table_name}, skipping")
                continue

            table_ref = dataset_ref.table(table_name)
            try:
                # Delete existing data for the date
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    autodetect=True,
                )
                job = self.bq.load_table_from_json(
                    rows, table_ref, job_config=job_config
                )
                job.result()  # Wait for job to complete
                self.log.info(
                    f"BigQuery ← {table_name}: {len(rows)} row(s)"
                )
            except Exception as e:
                self.log.error(f"BigQuery write failed for {table_name}: {e}")

    def _write_gold_to_postgresql(self, gold: dict):
        """Fallback: write gold data to PostgreSQL staging tables."""
        staging_tables = {
            "dim_song": "gold_dim_song_staging",
            "dim_artist": "gold_dim_artist_staging",
            "dim_album": "gold_dim_album_staging",
            "dim_date": "gold_dim_date_staging",
            "fact_song_daily": "gold_fact_song_daily_snapshot_staging",
            "fact_ingestion": "gold_fact_ingestion_summary_staging",
        }

        for key, table_name in staging_tables.items():
            rows = gold.get(key, [])
            if not rows:
                continue

            try:
                with self.db.cursor() as cur:
                    # Create staging table (simple: store as JSONB)
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            data JSONB,
                            ingested_at TIMESTAMP DEFAULT NOW()
                        )
                        """
                    )
                    for row in rows:
                        cur.execute(
                            f"INSERT INTO {table_name} (data) VALUES (%s)",
                            (json.dumps(row, default=str),),
                        )
                    self.db.commit()
                self.log.info(
                    f"PostgreSQL ← {table_name}: {len(rows)} row(s) "
                    f"(staging — BigQuery not configured)"
                )
            except Exception as e:
                self.log.error(f"Gold staging write failed for {table_name}: {e}")
                self.db.rollback()

    # ------------------------------------------------------------------
    # Helper Utilities
    # ------------------------------------------------------------------
    def _normalize(self, text: str) -> str:
        """Normalize text for matching: lowercase, remove special chars."""
        if not text:
            return ""
        text = text.lower().strip()
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\s+", " ", text)
        return text

    def _generate_song_id(self, title_norm: str, artist_norm: str) -> str:
        """Generate deterministic song_id from normalized title + artist."""
        raw = f"{title_norm}|{artist_norm}"
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def _parse_view_count(self, view_str: str) -> int:
        """Parse '138,106,461 views' → 138106461."""
        try:
            digits = re.sub(r"[^\d]", "", view_str)
            return int(digits) if digits else 0
        except (ValueError, TypeError):
            return 0

    def _parse_duration(self, duration_str: str) -> int:
        """Parse '4:05' → 245 seconds."""
        try:
            parts = duration_str.split(":")
            if len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            return 0
        except (ValueError, TypeError):
            return 0

    def _log_ingestion(
        self,
        source: str,
        status: str,
        records_raw: int,
        records_silver: int,
        records_rejected: int,
        started_at: datetime,
        finished_at: datetime,
        duration: int,
        error_message: str = None,
    ):
        """Write to silver_ingestion_log."""
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO silver_ingestion_log
                        (batch_id, source, status, records_raw,
                         records_silver, records_rejected, error_message,
                         started_at, finished_at, duration_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        self.batch_id,
                        source,
                        status,
                        records_raw,
                        records_silver,
                        records_rejected,
                        error_message,
                        started_at,
                        finished_at,
                        duration,
                    ),
                )
                self.db.commit()
        except Exception as e:
            self.log.error(f"Ingestion log failed: {e}")

    def _log_data_quality(
        self,
        table_name: str,
        check_type: str,
        status: str,
        affected_rows: int,
        details: str,
    ):
        """Write to silver_data_quality_log."""
        try:
            with self.db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO silver_data_quality_log
                        (table_name, check_type, status, affected_rows,
                         details, checked_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (table_name, check_type, status, affected_rows, details),
                )
                self.db.commit()
        except Exception as e:
            self.log.error(f"Data quality log failed: {e}")

    # ------------------------------------------------------------------
    # Main Entry Point
    # ------------------------------------------------------------------
    async def main(self):
        """Route to the appropriate ETL layer based on mode."""
        mode = self.mode

        self.log.info(f"ETL mode: {mode}")

        if mode in ("bronze", "all"):
            await self.run_bronze()
        if mode in ("silver", "all"):
            await self.run_silver()
        if mode in ("gold", "all"):
            await self.run_gold()

        self.log.info(f"ETL pipeline complete — batch_id={self.batch_id}")
        self.db.close()

    async def handler(self, job: dict):
        """Dummy handler (required by base class)."""
        pass
