from controllers import Controllers
from httpx import AsyncClient, Response
import json
import hashlib
import asyncio
import pandas as pd
from library.youtube_web import YoutubeWeb
from helpers.output import Output
import redis


class BaseCrawl(Controllers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session: AsyncClient = AsyncClient()
        self.youtube_web = YoutubeWeb()
        spotify_kwargs = {**kwargs, "output": "raw_spotify"}
        self.spotify_output = Output(*args, **spotify_kwargs)

        youtube_kwargs = {**kwargs, "output": "raw_youtube"}
        self.youtube_output = Output(*args, **youtube_kwargs)

        # Redis client untuk caching
        try:
            self.redis = redis.Redis(
                host="127.0.0.1",
                port=6379,
                decode_responses=True,
                socket_connect_timeout=3,
            )
            self.redis.ping()
            self.log.info("Redis connected — caching enabled")
        except Exception as e:
            self.log.warning(f"Redis unavailable, caching disabled: {e}")
            self.redis = None

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------
    def _cache_key(self, prefix: str, identifier: str) -> str:
        """Generate consistent cache key dengan MD5 hash."""
        return f"crawler:{prefix}:{hashlib.md5(identifier.encode()).hexdigest()}"

    def _cache_get(self, key: str):
        """Ambil data dari cache. Return None jika tidak ada atau Redis mati."""
        if not self.redis:
            return None
        try:
            data = self.redis.get(key)
            return json.loads(data) if data else None
        except Exception:
            return None

    def _cache_set(self, key: str, value, ttl: int = 86400):
        """Simpan data ke cache dengan TTL (default 24 jam)."""
        if not self.redis:
            return
        try:
            self.redis.setex(key, ttl, json.dumps(value))
        except Exception:
            pass

    def _is_song_done(self, song: str) -> bool:
        """Cek apakah lagu sudah selesai diproses (Spotify + YouTube)."""
        if not self.redis:
            return False
        try:
            done_key = f"crawler:done:{hashlib.md5(song.encode()).hexdigest()}"
            return self.redis.exists(done_key) == 1
        except Exception:
            return False

    def _mark_song_done(self, song: str):
        """Tandai lagu sudah selesai diproses (TTL 7 hari)."""
        if not self.redis:
            return
        try:
            done_key = f"crawler:done:{hashlib.md5(song.encode()).hexdigest()}"
            self.redis.setex(done_key, 604800, "1")  # 7 hari
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Retry helper
    # ------------------------------------------------------------------
    async def _retry_call(self, func, *args, max_retries=3, base_delay=1.0, **kwargs):
        """Panggil async function dengan exponential backoff retry.

        Retry jika:
        - Exception terjadi (network error, timeout, HTTP error, dll)
        - Function return None (gagal tanpa raise)

        Untuk 429 (rate limit), delay otomatis lebih panjang.
        """
        delay = base_delay
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                last_error = e
                result = None

            # Sukses — return immediately
            if result is not None:
                return result

            # Gagal — tentukan delay berdasarkan jenis error
            reason = str(last_error) if last_error else "returned None"
            is_rate_limit = "429" in reason

            # 429 rate limit → delay lebih panjang (mulai dari 5s)
            if is_rate_limit:
                delay = max(delay, 5.0)

            if attempt < max_retries:
                self.log.warning(
                    f"[attempt {attempt + 1}/{max_retries + 1}] "
                    f"{func.__name__} gagal ({reason}). Retry in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
                delay *= 2
                last_error = None
            else:
                self.log.error(
                    f"[attempt {attempt + 1}/{max_retries + 1}] "
                    f"{func.__name__} gagal total ({reason})"
                )

        return None

    # ------------------------------------------------------------------
    # Spotify Token
    # ------------------------------------------------------------------
    async def get_token(self):
        try:
            data = {
                'grant_type': 'client_credentials',
                'client_id': 'ac8c3a5a7af04b68b254fa8e4be3d8b0',
                'client_secret': 'a9cc1509c99c4dcdbc82c603bf55192f',
            }

            response: Response = await self.session.post(
                'https://accounts.spotify.com/api/token',
                data=data, timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.log.error(f"Error fetching token from Spotify: {e}")
            return None

    async def get_valid_token(self):
        try:
            with open("downloads/token.json", "r") as f:
                data = json.load(f)
            return data
        except Exception as e:
            self.log.error(f"Error fetching valid token from Spotify: {e}")
            return None

    async def refresh_token(self):
        try:
            token = await self.get_token()
            with open("downloads/token.json", "w") as f:
                json.dump(token, f)
            return token
        except Exception as e:
            self.log.error(f"Error refreshing token from Spotify: {e}")
            return None

    # ------------------------------------------------------------------
    # Spotify — dengan cache + retry
    # ------------------------------------------------------------------
    async def get_data_spotify(self, query: str):
        """
        Ambil data Spotify dengan:
        1. Cek cache Redis dulu — return jika sudah ada DAN valid
        2. Jika belum, fetch dari API dengan retry (max 3x)
        3. Simpan hasil ke cache (TTL 24 jam) — hanya jika ada data
        """
        cache_key = self._cache_key("spotify", query)
        cached = self._cache_get(cache_key)
        if cached and self._is_valid_spotify(cached):
            self.log.info(f"Cache HIT  Spotify → {query}")
            return cached
        if cached:
            self.log.info(f"Cache STALE Spotify → {query}, re-fetching...")

        self.log.info(f"Cache MISS Spotify → {query}, fetching...")
        result = await self._retry_call(
            self._fetch_spotify, query,
            max_retries=3, base_delay=1.0,
        )

        if result and self._is_valid_spotify(result):
            self._cache_set(cache_key, result, ttl=86400)
        return result

    def _is_valid_spotify(self, data: dict) -> bool:
        """Cek apakah response Spotify punya items yang valid."""
        try:
            items = data.get("tracks", {}).get("items", [])
            if isinstance(items, dict):
                items = [items]
            return len(items) > 0
        except Exception:
            return False
    
    async def get_label_spotify(self, id: str):
        token = await self.get_valid_token()
        try:
            headers = {
            'Authorization': f'Bearer {token["access_token"]}',
            }
            response: Response = await self.session.get(
                f'https://api.spotify.com/v1/albums/{id}', headers=headers,
                timeout=30
            )
            if response.status_code == 401:
                token = await self.refresh_token()
                headers['Authorization'] = f'Bearer {token["access_token"]}'
                response = await self.session.get(
                    f'https://api.spotify.com/v1/albums/{id}',
                    headers=headers, timeout=30
                )
            res_json = response.json()
            print(res_json)
            label = res_json['label']
            return label
        except Exception as e:
            self.log.error(f"Error fetching label from Spotify: {e}")
            return None

    async def _fetch_spotify(self, query: str):
        """Panggil Spotify API (dipanggil oleh get_data_spotify via retry)."""
        token = await self.get_valid_token()
        headers = {
            'Authorization': f'Bearer {token["access_token"]}',
        }

        params = {
            'q': query,
            'type': 'track',
        }

        response: Response = await self.session.get(
            'https://api.spotify.com/v1/search',
            headers=headers, params=params, timeout=30
        )

        # Token expired — refresh & retry sekali
        if response.status_code == 401:
            token = await self.refresh_token()
            headers['Authorization'] = f'Bearer {token["access_token"]}'
            response = await self.session.get(
                'https://api.spotify.com/v1/search',
                headers=headers, params=params, timeout=30
            )

        response.raise_for_status()
        result = response.json()
        # label = await self.get_label_spotify(result.get('tracks', {}).get('items', [])[0].get('album', {}).get('id', ''))

        return result

    # ------------------------------------------------------------------
    # YouTube — dengan cache + retry
    # ------------------------------------------------------------------
    async def get_data_youtube(self, keyword: str):
        """
        Ambil data YouTube dengan:
        1. Cek cache Redis dulu — return jika sudah ada
        2. Jika belum, fetch dari API dengan retry (max 3x)
        3. Simpan hasil ke cache (TTL 24 jam)
        """
        cache_key = self._cache_key("youtube", keyword)
        cached = self._cache_get(cache_key)
        if cached:
            self.log.info(f"Cache HIT  YouTube → {keyword}")
            return cached

        self.log.info(f"Cache MISS YouTube → {keyword}, fetching...")
        result = await self._retry_call(
            self.youtube_web.get_detail_keyword, keyword,
            max_retries=4, base_delay=2.0,
        )

        if result:
            self._cache_set(cache_key, result, ttl=86400)
        return result

    # ------------------------------------------------------------------
    # YouTube (scraping) — tanpa API key, dengan cache + retry
    # ------------------------------------------------------------------
    async def get_data_youtube_scrape(self, keyword: str):
        """
        Ambil data YouTube TANPA API key — scraping halaman hasil pencarian.

        1. Cek cache Redis dulu — return jika valid
        2. Jika cache kosong/stale, scrape dengan retry (max 3x)
        3. Simpan ke cache (TTL 24 jam) — hanya jika items tidak kosong
        """
        cache_key = self._cache_key("youtube_scrape_v2", keyword)
        cached = self._cache_get(cache_key)
        if cached and self._is_valid_youtube(cached):
            self.log.info(f"Cache HIT  YouTube(scrape) → {keyword}")
            return cached
        if cached:
            self.log.info(f"Cache STALE YouTube(scrape) → {keyword}, re-scraping...")

        self.log.info(f"Cache MISS YouTube(scrape) → {keyword}, scraping...")
        result = await self._retry_call(
            self.youtube_web.get_detail_keyword_no_api, keyword,
            max_retries=3, base_delay=3.0,
        )

        if result and self._is_valid_youtube(result):
            self._cache_set(cache_key, result, ttl=86400)
        return result

    def _is_valid_youtube(self, data: dict) -> bool:
        """Cek apakah response YouTube punya items yang valid."""
        try:
            items = data.get("items", [])
            return isinstance(items, list) and len(items) > 0
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    async def main(self):
        df = pd.read_excel("controllers/crawling/catalog_data.xlsx", sheet_name="Data")
        # Baca semua kolom yang ada (dinamis)
        excel_columns = list(df.columns)
        datas = df.to_dict('records')

        success_count = 0
        fail_count = 0

        for data in datas:
            song = data.get('SONG TITLE', '')
            song_code = str(data.get('CODE', ''))
            original_artist = str(data.get('ORIGINAL ARTIST', '') or '')
            

            # Skip jika lagu sudah pernah selesai diproses
            if self._is_song_done(song):
                self.log.info(f"SKIP (done) → {song}")
                success_count += 1
                continue

            # Spotify — dengan cache + retry
            data_spotify = await self.get_data_spotify(song)
            if data_spotify is None:
                self.log.error(f"Gagal fetch Spotify: {song}, skip.")
                fail_count += 1
                continue

            items = data_spotify["tracks"]["items"][0]
            items['song_code'] = song_code
            # items['original_artist'] = original_artist
            data_spotify['tracks']['items'] = items

            artists = items['album']['artists'][0]
            artist = artists['name']

            # YouTube — scraping tanpa API key (cache + retry)
            data_youtube = await self.get_data_youtube_scrape(f"{song} {artist}")

            # Proteksi: skip jika YouTube kosong setelah semua retry
            if data_youtube is None or not data_youtube.get("items"):
                self.log.error(f"Gagal fetch YouTube: {song} (empty), skip.")
                fail_count += 1
                continue

            yt_items = data_youtube["items"]
            if isinstance(yt_items, list) and len(yt_items) > 0:
                yt_item = yt_items[0]
            elif isinstance(yt_items, dict):
                yt_item = yt_items
            else:
                self.log.error(f"YouTube items invalid: {song}, skip.")
                fail_count += 1
                continue

            data_youtube['items'] = yt_item
            snippet = yt_item['snippet']
            snippet['artist'] = artist
            snippet['song_name'] = song
            # Metadata catalog
            snippet['song_code'] = song_code
            snippet['original_artist'] = original_artist
            snippet['song_writers'] = artist              # dari Spotify artist name
            snippet['recordings_title'] = song             # dari Spotify track name
            # snippet['label'] = label                       # dari Excel (jika ada)
            yt_item['snippet'] = snippet
            data_youtube['items'] = yt_item

            
            self.spotify_output.put(json.dumps(data_spotify).encode('utf-8'))
            self.log.success(f"Spotify  → {data_spotify['tracks']['items']['name']}")

            # self.log.debug(json.dumps(data_youtube, indent=4))
            self.youtube_output.put(json.dumps(data_youtube).encode('utf-8'))
            self.log.success(f"YouTube  → {yt_item['snippet']['title']}")

            # Tandai lagu sudah selesai — next run langsung skip
            self._mark_song_done(song)
            success_count += 1

            # Jeda antar lagu supaya tidak kena rate limit (429)
            await asyncio.sleep(2.0)

        self.log.info(
            f"Done. Success: {success_count}, Failed: {fail_count}"
        )
