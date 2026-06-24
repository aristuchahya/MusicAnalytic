# Music Analytics Pipeline

Pipeline data medallion architecture (Bronze → Silver → Gold) untuk menganalisis lagu dari **Spotify** dan **YouTube** — menjawab berapa banyak video YouTube dan ISRC Spotify untuk setiap lagu.

## Arsitektur

```
┌──────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│  catalog_data.xlsx   Spotify API   YouTube (web scraping)       │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                      CRAWLER (Python)                            │
│  controllers/crawling/base.py                                    │
│  • Search Spotify API → dapatkan track metadata + ISRC           │
│  • Scrape YouTube search → dapatkan video + channel              │
│  • Redis caching → skip lagu yang sudah diproses                 │
│  • Publish ke Kafka: topic raw_spotify, raw_youtube              │
└──────────────────────────┬───────────────────────────────────────┘
                           │ Kafka
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                     ETL — Medallion Architecture                  │
│  controllers/etl/base.py                                         │
│                                                                  │
│  ┌─ BRONZE (PostgreSQL) ──────────────────────────────────────┐  │
│  │  Consume dari Kafka → bronze_spotify_raw / bronze_youtube_raw│  │
│  │  1:1 copy raw JSON + audit trail (batch_id, ingested_at)   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                           │                                      │
│                           ▼                                      │
│  ┌─ SILVER (BigQuery) ────────────────────────────────────────┐  │
│  │  Parse JSON → silver_spotify_{artist,album,track,isrc}     │  │
│  │             → silver_youtube_{channel,video}                │  │
│  │             → silver_song_mapping (cross-source matching)   │  │
│  │  Clean, deduplicate, validate, normalize                   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                           │                                      │
│                           ▼                                      │
│  ┌─ GOLD (BigQuery) ──────────────────────────────────────────┐  │
│  │  Aggregate → gold_dim_song    (✅ Q1 & Q2 answered here)   │  │
│  │           → gold_dim_artist                                │  │
│  │           → gold_dim_album                                 │  │
│  │           → gold_fact_song_daily_snapshot                   │  │
│  │           → gold_fact_ingestion_summary                     │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                     VISUALIZATION (Grafana)                       │
│  Dashboard: Music Analytics — Medallion Dashboard                 │
│  • Q1: Top songs by YouTube video count (bar chart)              │
│  • Q2: Top songs by Spotify ISRC count (bar chart)               │
│  • Platform presence (donut)                                     │
│  • Summary stats (total songs, videos, ISRCs, views)             │
│  • Full detail table                                             │
└──────────────────────────────────────────────────────────────────┘
```

## Business Questions

| Q | Pertanyaan | Jawaban di |
|---|-----------|-----------|
| Q1 | How many YouTube videos does each song have? | `gold_dim_song.youtube_video_count` |
| Q2 | How many ISRCs does each song have in Spotify? | `gold_dim_song.spotify_isrc_count` |

```sql
-- Q1 + Q2 sekaligus
SELECT
  song_title,
  original_artist,
  song_code,
  youtube_video_count,
  spotify_isrc_count,
  isrc_list,
  total_youtube_views
FROM `project.gold.gold_dim_song`
ORDER BY youtube_video_count DESC;
```

## Struktur Project

```
spotify-analytic/
├── main.py                          # CLI entry point
├── config.ini                       # PostgreSQL, BigQuery, Kafka config
├── pyproject.toml                   # Python dependencies (uv)
├── Dockerfile                       # Container untuk crawler
├── docker-compose.yaml              # Kafka, Zookeeper, Redis, Grafana
│
├── controllers/
│   ├── crawling/base.py             # Crawler: Spotify API + YouTube scrape
│   ├── crawling/catalog_data.xlsx   # Katalog lagu (CODE, ARTIST, SONG TITLE)
│   ├── etl/base.py                  # ETL: Bronze → Silver → Gold
│   └── ...
│
├── helpers/
│   ├── database.py                  # PostgreSQL driver (psycopg)
│   ├── kafka.py                     # Kafka consumer
│   ├── output/driver/kafka.py       # Kafka producer
│   └── ...
│
├── library/
│   ├── youtube_web.py               # YouTube web scraping (no API key)
│   └── endpoint.py                  # API endpoint builder
│
├── dbml/
│   └── erd-medallion-architecture.dbml  # Database schema design
│
├── raw/
│   ├── raw_spotify.json             # Sample Spotify API response
│   └── raw_youtube.json             # Sample YouTube scrape response
│
├── grafana/
│   ├── datasources/bigquery.yml     # BigQuery datasource provisioning
│   └── dashboards/
│       ├── dashboards.yml           # Dashboard provider config
│       └── music-analytics-gold.json # Dashboard definition
│
└── setup-grafana-env.sh             # Generate .env untuk Grafana
```

## Quick Start

### Prasyarat

- Python 3.12 + uv
- Docker + Docker Compose
- PostgreSQL (local atau Supabase)
- Google Cloud Service Account (untuk BigQuery)
- Spotify API Client ID & Secret

### 1. Install dependencies

```bash
uv sync
```

### 2. Konfigurasi

Edit `config.ini`:

```ini
[postgresql]
username = your_user
password = your_password
dbname = local
host = localhost
port = 5432

[bigquery]
project = your-gcp-project-id
dataset = gold
silver_dataset = silver
credentials_path = service_account.json

[kafka]
bootstrap_servers = localhost:9092
topic_spotify = raw_spotify
topic_youtube = raw_youtube
group_id = etl-bronze-consumer
```

Taruh `service_account.json` di root project (sudah di `.gitignore`).

### 3. Jalankan infrastruktur

```bash
# Kafka + Zookeeper + Redis
docker compose up -d zookeeper kafka redis

# Grafana (opsional, untuk dashboard)
./setup-grafana-env.sh
docker compose up -d grafana
```

### 4. Jalankan crawler

```bash
# Crawling Spotify + YouTube, publish ke Kafka
python main.py crawler --mode music \
  --destination kafka \
  --bootstrap-servers localhost:9092
```

### 5. Jalankan ETL

```bash
# Bronze: consume dari Kafka → PostgreSQL
python main.py etl --mode bronze

# Silver: transform PostgreSQL → BigQuery
python main.py etl --mode silver

# Gold: aggregate di BigQuery
python main.py etl --mode gold

# Atau semua sekaligus
python main.py etl --mode all
```

### 6. Dashboard

Buka `http://localhost:3000` → login `admin/admin` → Dashboard **"Music Analytics — Medallion Dashboard"**.

## Data Lineage

```
catalog_data.xlsx ──┐
                     ├──► Crawler ──► Kafka ──► Bronze (PG) ──► Silver (BQ) ──► Gold (BQ) ──► Grafana
Spotify API ────────┤
YouTube (scrape) ───┘
```

Setiap record di-track dengan `batch_id` dan `ingested_at` dari Bronze sampai Gold.

## Gold Layer Tables

| Table | Grain | Deskripsi |
|-------|-------|-----------|
| `gold_dim_song` | 1 lagu | Master lagu — **Q1 & Q2 answered here** |
| `gold_dim_artist` | 1 artis | Profil artis terpadu (Spotify + YouTube) |
| `gold_dim_album` | 1 album | Metadata album + aggregate YouTube |
| `gold_dim_date` | 1 tanggal | Kalender dimension |
| `gold_fact_song_daily_snapshot` | 1 lagu × 1 hari | Snapshot harian untuk time-series |
| `gold_fact_ingestion_summary` | 1 hari × 1 source | Monitoring pipeline ETL |

## ERD — Entity Relationship Diagram

> Full DBML: `dbml/erd-medallion-architecture.dbml`

### Bronze Layer — PostgreSQL (Raw Ingestion)

Data mentah 1:1 dari API, disimpan sebagai raw JSON + metadata ingestion.

| Table | Key | Grain | Deskripsi |
|-------|-----|-------|-----------|
| `bronze_spotify_raw` | `raw_id` (UUID) | 1 API response | Full JSON response dari Spotify `/search` |
| `bronze_youtube_raw` | `raw_id` (UUID) | 1 API response | Full JSON response dari YouTube scrape |

**`bronze_spotify_raw`**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `raw_id` | VARCHAR(36) PK | UUID |
| `batch_id` | VARCHAR(64) | Batch run ID untuk lineage |
| `search_query` | VARCHAR(512) | Query pencarian |
| `search_offset` | INTEGER | Pagination offset |
| `search_limit` | INTEGER | Pagination limit |
| `search_total` | INTEGER | Total hasil pencarian |
| `search_href` | VARCHAR | URL endpoint API |
| `search_next` | VARCHAR | URL halaman berikutnya |
| `raw_json` | TEXT | Full JSON response |
| `ingested_at` | TIMESTAMP | Waktu ingest (WIB) |
| `source` | VARCHAR(64) | `spotify_api` |

**`bronze_youtube_raw`**

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `raw_id` | VARCHAR(36) PK | UUID |
| `batch_id` | VARCHAR(64) | Batch run ID |
| `search_query` | VARCHAR(512) | Query pencarian |
| `video_id` | VARCHAR(32) | YouTube Video ID |
| `raw_json` | TEXT | Full JSON response |
| `ingested_at` | TIMESTAMP | Waktu ingest (WIB) |
| `source` | VARCHAR(64) | `youtube_api` |

---

### Silver Layer — BigQuery (Cleaned & Validated)

Data dibersihkan, deduplikasi, nested JSON dipecah ke tabel relasional.

#### Spotify Side

| Table | Key | Grain | Keterangan |
|-------|-----|-------|-----------|
| `silver_spotify_artist` | `artist_id` | 1 artis | Nama, URI, URL |
| `silver_spotify_album` | `album_id` | 1 album | Nama, tipe, rilis, cover image |
| `silver_spotify_album_artist` | (`album_id`, `artist_id`) | many-to-many | Junction album ↔ artis |
| `silver_spotify_track` | `track_id` | **1 ISRC** | Track + ISRC code — kunci Q2 |
| `silver_spotify_track_artist` | (`track_id`, `artist_id`) | many-to-many | Junction track ↔ artis |

**`silver_spotify_track`** (kunci Q2)

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `track_id` | VARCHAR(64) PK | Spotify Track ID |
| `track_name` | VARCHAR(512) | Judul lagu |
| `isrc_code` | VARCHAR(32) | **ISRC** — kunci untuk Q2 |
| `album_id` | VARCHAR(64) FK | → `silver_spotify_album` |
| `disc_number` | INTEGER | Nomor disc |
| `track_number` | INTEGER | Nomor track |
| `duration_ms` | INTEGER | Durasi (ms) |
| `explicit` | BOOLEAN | Explicit content |
| `track_uri` | VARCHAR | Spotify URI |
| `spotify_url` | VARCHAR | URL Spotify |

#### YouTube Side

| Table | Key | Grain | Keterangan |
|-------|-----|-------|-----------|
| `silver_youtube_channel` | `channel_id` | 1 channel | Nama channel |
| `silver_youtube_video` | `video_id` | **1 video** | Video + statistics — kunci Q1 |

**`silver_youtube_video`** (kunci Q1)

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `video_id` | VARCHAR(32) PK | YouTube Video ID |
| `channel_id` | VARCHAR(64) FK | → `silver_youtube_channel` |
| `title` | VARCHAR(1024) | Judul video |
| `title_artist_name` | VARCHAR(512) | Artis dari judul |
| `title_song_name` | VARCHAR(512) | Lagu dari judul |
| `published_at_raw` | VARCHAR | Raw publish string |
| `duration_seconds` | INTEGER | Durasi (detik) |
| `view_count` | BIGINT | Jumlah views |
| `thumbnail_url` | VARCHAR | URL thumbnail |
| `song_code` | VARCHAR | Catalog code |
| `song_writers` | VARCHAR | Penulis lagu |
| `recordings_title` | VARCHAR | Judul rekaman |

#### Cross-Source Mapping

**`silver_song_mapping`** — tabel inti yang menyatukan Spotify + YouTube.

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `mapping_id` | VARCHAR(36) PK | UUID |
| `song_id` | VARCHAR(36) | Surrogate key — identitas lagu kanonikal |
| `song_title_normalized` | VARCHAR(512) | Judul dinormalisasi (lowercase) |
| `artist_name_normalized` | VARCHAR(512) | Artis dinormalisasi |
| `spotify_track_id` | VARCHAR FK | → `silver_spotify_track` (nullable) |
| `spotify_isrc_code` | VARCHAR | ISRC (denormalized) |
| `youtube_video_id` | VARCHAR FK | → `silver_youtube_video` (nullable) |
| `youtube_channel_id` | VARCHAR FK | → `silver_youtube_channel` |
| `youtube_view_count` | BIGINT | Views (denormalized) |
| `match_confidence` | DECIMAL(3,2) | Confidence score |
| `match_method` | VARCHAR | `direct_spotify` / `title_artist_fuzzy` |

**Grain**: 1 row = 1 asosiasi lagu ↔ track Spotify **ATAU** lagu ↔ video YouTube.  
Satu lagu bisa punya **banyak** track Spotify (ISRC berbeda) dan **banyak** video YouTube.

#### Audit Tables

| Table | Keterangan |
|-------|------------|
| `silver_data_quality_log` | Hasil validasi (schema, duplicate, anomaly) |
| `silver_ingestion_log` | Audit trail setiap ingestion run (batch_id, source, status, row counts) |

---

### Gold Layer — BigQuery (Business Views)

Semua metrics di-precompute — query tanpa JOIN/GROUP BY.

| Table | Grain | Keterangan |
|-------|-------|-----------|
| `gold_dim_song` | 1 lagu | **Master lagu — Q1 & Q2 answered here** |
| `gold_dim_artist` | 1 artis | Profil artis terpadu |
| `gold_dim_album` | 1 album | Metadata + aggregate YouTube |
| `gold_dim_date` | 1 tanggal | Kalender dimension |
| `gold_fact_song_daily_snapshot` | 1 lagu × 1 hari | Time-series snapshot |
| `gold_fact_ingestion_summary` | 1 hari × 1 source | Pipeline monitoring |

**`gold_dim_song`** — tabel paling penting:

| Kolom | Tipe | Keterangan |
|-------|------|------------|
| `song_id` | VARCHAR PK | Surrogate key |
| `song_title` | VARCHAR | Judul lagu |
| `original_artist` | VARCHAR | Nama artis |
| `song_code` | VARCHAR | Catalog code |
| `song_writers` | VARCHAR | Penulis lagu |
| `recordings_title` | VARCHAR | Judul rekaman |
| `album_name` | VARCHAR | Nama album |
| `release_date` | STRING | Tanggal rilis |
| `cover_image_url` | VARCHAR | Cover image URL |
| `isrc_list` | STRING | ISRC comma-separated |
| **`youtube_video_count`** | INTEGER | **✅ Q1 answer** |
| **`spotify_isrc_count`** | INTEGER | **✅ Q2 answer** |
| `has_spotify` | BOOLEAN | Platform flag |
| `has_youtube` | BOOLEAN | Platform flag |
| `total_youtube_views` | BIGINT | Total views |
| `unique_youtube_channels` | INTEGER | Unique channels |

### Relationship Diagram

```
bronze_spotify_raw                bronze_youtube_raw
       │                                    │
       ▼                                    ▼
silver_spotify_artist ◄──────┐    ┌────── silver_youtube_channel
       │                     │    │              │
       ▼                     │    │              ▼
silver_spotify_album         │    │       silver_youtube_video
       │                     │    │              │
       ▼                     │    │              │
silver_spotify_track ────────┼────┼──────────────┘
       │                     │    │
       ▼                     ▼    ▼
silver_spotify_track_artist  silver_song_mapping
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             gold_dim_song   gold_dim_artist  gold_dim_album
                    │
                    ▼
      gold_fact_song_daily_snapshot
```

---

## Kolom `gold_dim_song`

| Kolom | Sumber | Keterangan |
|-------|--------|------------|
| `song_id` | Generated | Surrogate key |
| `song_title` | Spotify track name | |
| `original_artist` | Spotify artist name | |
| `song_code` | Excel CODE | Catalog identifier |
| `song_writers` | Spotify artist | |
| `recordings_title` | Spotify track name | |
| `album_name` | Spotify album | |
| `release_date` | Spotify | |
| `cover_image_url` | Spotify album image | |
| `isrc_list` | Spotify ISRC | Comma-separated |
| `youtube_video_count` | Pre-computed | **Q1 answer** |
| `spotify_isrc_count` | Pre-computed | **Q2 answer** |
| `total_youtube_views` | Aggregate | |
| `has_spotify` / `has_youtube` | Boolean | Platform presence |

---

## Data Quality & Validation Strategy

### 1. Multi-Layer Validation

| Layer | Validasi | Implementasi |
|-------|----------|--------------|
| **Crawler** | Cache validity check | `_is_valid_spotify()`, `_is_valid_youtube()` — data kosong tidak di-cache, di-fetch ulang |
| **Bronze** | Schema enforcement | DDL `CREATE TABLE IF NOT EXISTS` memastikan struktur tabel |
| | Deduplication | `ON CONFLICT (raw_id) DO NOTHING` mencegah duplikasi ingest |
| | Audit trail | `batch_id`, `ingested_at`, `source` di setiap record |
| **Silver** | ISRC extraction | Validasi `external_ids.isrc` dari Spotify JSON |
| | Type casting | `duration_seconds`, `view_count` diparse dari string |
| | Missing values | `_check_null_isrc`, `_check_null_channel`, `_check_empty_video_title` |
| | Duplicate detection | `_check_duplicate_mapping` — cegah duplikasi asosiasi |
| | Normalization | `_normalize()` — lowercase, hapus special chars, trim whitespace |
| **Gold** | Aggregate validation | `youtube_video_count` + `spotify_isrc_count` di-precompute dari source |
| | Platform flags | `has_spotify`, `has_youtube` boolean untuk filter valid |

### 2. Data Quality Log

Semua hasil pengecekan dicatat di `silver_data_quality_log`:

```sql
SELECT table_name, check_type, status, checked_at
FROM silver_data_quality_log
WHERE checked_at >= CURRENT_DATE()
ORDER BY checked_at DESC;
```

| Status | Arti |
|--------|------|
| `pass` | Semua record valid |
| `warn` | Ditemukan anomali, data tetap diproses |
| `fail` | Data ditolak, tidak masuk Silver |

### 3. Ingestion Audit Trail

Setiap ETL run tercatat di `silver_ingestion_log`:

```sql
SELECT batch_id, source, status,
       records_raw, records_silver, records_rejected,
       duration_seconds, started_at
FROM silver_ingestion_log
ORDER BY started_at DESC
LIMIT 20;
```

### 4. Error Recovery

| Skenario | Recovery |
|----------|----------|
| API rate limit (429) | Exponential backoff: 5s → 10s → 20s, max 4 retry |
| Kafka mati | Fallback ke `raw/*.json` files |
| BigQuery mati | Fallback ke PostgreSQL staging tables |
| Data kosong | Cache di-skip, re-fetch; items kosong tidak dipublish |
| Duplikasi ingest | `ON CONFLICT DO NOTHING` / `ON CONFLICT DO UPDATE` |
---

## Storage Layer Design

### Why This Stack?

| Layer | Storage | Alasan |
|-------|---------|--------|
| **Bronze** | PostgreSQL | ACID compliance, audit trail, low volume (append-only raw JSON) |
| **Silver** | BigQuery | Columnar storage, auto-scaling, nested JSON support, terpisah dari operational DB |
| **Gold** | BigQuery | Pre-computed aggregates, query sub-detik, langsung connect ke Grafana |

### Scalability

| Aspek | Desain |
|-------|--------|
| **Bronze** | Append-only — bisa di-partition by `ingested_at::DATE` untuk query range |
| **Silver** | BigQuery auto-partition by `_PARTITIONTIME` atau `ingested_at` |
| **Gold** | Materialized view pattern — `gold_dim_song` di-refresh setiap ETL run |
| **Kafka** | Decoupling crawler dari ETL — backpressure di-handle oleh consumer group |

### Query-Friendly Design

| Kebutuhan | Implementasi |
|-----------|--------------|
| **Q1 & Q2 tanpa JOIN** | `youtube_video_count`, `spotify_isrc_count` pre-computed di `gold_dim_song` |
| **Filter by platform** | `has_spotify`, `has_youtube` boolean flags |
| **Time-series** | `gold_fact_song_daily_snapshot` dengan `date_id` FK → `gold_dim_date` |
| **Drill-down** | `gold_dim_song` → JOIN `silver_song_mapping` → `silver_youtube_video` / `silver_spotify_track` |
| **Dashboard** | Grafana connect langsung ke BigQuery tanpa intermediate cache |

### Data Retention

| Layer | Retention | Kebijakan |
|-------|-----------|-----------|
| Bronze (PG) | 30 hari | Hapus row >30 hari via cron `DELETE WHERE ingested_at < NOW() - INTERVAL '30 days'` |
| Silver (BQ) | 90 hari | Table expiration di BigQuery dataset |
| Gold (BQ) | Unlimited | Gold adalah source of truth — tidak dihapus |
| Redis cache | 24 jam (data), 7 hari (done flag) | Auto-expire via TTL |

---

## Monitoring & Maintenance

### 1. Pipeline Health Dashboard (Grafana)

```
gold_fact_ingestion_summary ──► Grafana
  • Success rate per source per hari
  • Total records raw → silver → gold
  • Durasi ingestion (tracking degradasi performa)
  • Data quality checks pass/fail ratio
```

### 2. Alerting Rules

| Kondisi | Severity | Action |
|---------|----------|--------|
| `success_rate_pct < 95%` | Warning | Cek `error_message` di ingestion log |
| `records_rejected > 0` | Warning | Investigasi DQ log untuk root cause |
| `duration_seconds > 300` | Warning | Optimasi query atau scale resource |
| `records_raw = 0` 2 hari berturut-turut | Critical | Crawler mati / API key expired |
| BigQuery job failed | Critical | Cek service account quota |

### 3. Maintenance Tasks

| Task | Frekuensi | Command |
|------|-----------|---------|
| Bronze cleanup | Harian | `DELETE FROM bronze_*_raw WHERE ingested_at < NOW() - INTERVAL '30 days'` |
| Redis cache flush | Mingguan | `docker exec yt-redis redis-cli FLUSHDB` |
| BQ table optimize | Bulanan | `bq query --use_legacy_sql=false 'CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE()'` |
| ETL re-run (backfill) | On-demand | `python main.py etl --mode all --batch-id backfill-YYYYMMDD` |
| Service account key rotation | 6 bulan | Update `service_account.json`, restart Grafana |

### 4. Backfill Strategy

```bash
# Re-process data dari Bronze untuk tanggal tertentu
python main.py etl --mode silver --batch-id backfill-20260601

# Re-build Gold dari Silver yang sudah ada
python main.py etl --mode gold --batch-id rebuild-20260601
```

### 5. Monitoring Query

```sql
-- Pipeline health 7 hari terakhir
SELECT
  date_id,
  source,
  total_raw_records,
  total_silver_records,
  total_rejected,
  success_rate_pct,
  avg_ingestion_duration_seconds
FROM gold_fact_ingestion_summary
WHERE date_id >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY date_id DESC, source;

-- Data quality checks terbaru
SELECT table_name, check_type, status, checked_at
FROM silver_data_quality_log
ORDER BY checked_at DESC
LIMIT 20;
```
