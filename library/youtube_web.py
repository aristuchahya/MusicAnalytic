import json
import random
import re
import unicodedata
import asyncio
import requests
import errno
import socket
import urllib3
import configparser

from loguru import logger
from httpx import AsyncClient, Response
from urllib.parse import urlparse, urljoin
from helpers.html_parser import HtmlParser
import dateparser
from library.endpoint import Endpoints

class YoutubeWeb:
    def __init__(self):
        # self.config = configparser.ConfigParser()
        # self.config.read("config.ini")
        # self.app = self.config["Youtube"]["application_project"]
        self.session: AsyncClient = AsyncClient()
        self.requests = requests.Session()
        self.parser = HtmlParser()
        self.log = logger
        self.BASE_URL = "https://www.youtube.com"
        self.KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"
        self.PAYLOAD = {
            "context": {
                "client": {
                    "userAgent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.9 Safari/537.36,gzip(gfe)",
                    "clientName": "WEB",
                    "clientVersion": "2.20210201.07.01",
                }
            }
        }

        self.cookies = {}

        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,id;q=0.8",
            "cache-control": "max-age=0",
            # 'cookie': 'VISITOR_PRIVACY_METADATA=CgJJRBIEGgAgVQ%3D%3D; _ga=GA1.2.1353492130.1704960229; _ga_M0180HEFCY=GS1.1.1704960228.1.1.1704960231.0.0.0; VISITOR_PRIVACY_METADATA=CgJJRBIEGgAgVQ%3D%3D; VISITOR_INFO1_LIVE=LkKO-Uf9I98; HSID=AtFwsa9lMEhEBoXwh; SSID=ADCl6QNvT4DCbMVnk; APISID=M6YRruAqAQ0PDuMq/AriVu3DQeR26hS--o; SAPISID=p8F_uSY70MBo2C_K/AgsQxOp279yQyQxXu; __Secure-1PAPISID=p8F_uSY70MBo2C_K/AgsQxOp279yQyQxXu; __Secure-3PAPISID=p8F_uSY70MBo2C_K/AgsQxOp279yQyQxXu; NID=514=myXyz_wSQD0u954Ch6el5XiQnYp8D_-SA_rnGRkgqWUX5EZhRwuvsarBI_1RuHBQDGEEUW4F45SLFQmWmIPyaRv0rz3EdS8XzP7izybIrfaW2qfYkQRn3QHjGI4N_oRaubz0gnxhBEjMkOp6RzH1rVR20Z0S7TsdCMme3R5tLxaLnAI_D6O6F5NAiAtM1h1RZrBw5VNUXPoDHJvO91MmJQ; YSC=3mqCkO0Yvr0; SID=g.a000lAhshz8xqgw6jccl7XHTqS2tqv_uhHe_zvXw2wTd6xx0KYPBhNwGt0gQFEDtxXE5XMuG1gACgYKAU0SARASFQHGX2MibLtcmv6rHMWfdfXOtzV7zBoVAUF8yKoyXkqxG_bOjMbN4X4najhb0076; __Secure-1PSIDTS=sidts-CjIB3EgAEgkdynoQgs4VL8cESW7_BtLlyxP2JhYkQ7udtMBlOlrNfun_-A7byZoN3sY8yhAA; __Secure-3PSIDTS=sidts-CjIB3EgAEgkdynoQgs4VL8cESW7_BtLlyxP2JhYkQ7udtMBlOlrNfun_-A7byZoN3sY8yhAA; __Secure-1PSID=g.a000lAhshz8xqgw6jccl7XHTqS2tqv_uhHe_zvXw2wTd6xx0KYPBglS-xdQFwJBi31sCeuK8ywACgYKAbYSARASFQHGX2MiTVp4ifo2pvpC8Go2DVhmXBoVAUF8yKq35iB9QMXncb5P0RZHHgoL0076; __Secure-3PSID=g.a000lAhshz8xqgw6jccl7XHTqS2tqv_uhHe_zvXw2wTd6xx0KYPBCa1MaIGX6qd8_yC7NyRpnQACgYKAXMSARASFQHGX2MiirvCChS4EWVjSC9v5cu9BxoVAUF8yKqSQ3Lntw1JnNk4x9-HGmoq0076; LOGIN_INFO=AFmmF2swRQIgd8Wq9dJ-es7e8qpug0tuiD30xVBGY9kOVDfMQpzaTD8CIQCGdiAxaI3tt7JACsXmXZDWs1yGtCNMECMjQ5riDCOPvg:QUQ3MjNmeGVrUXZ3SGMtdG1GQnRMVE94amNJTlBOejMtcmEwd1ZZb0ZUQnEyd2hNR1JWbVkteEZhWHpXZDh4R0xELVFDOVBadzRSTlV5NWVlc3d1MTR6MTVCeDQ4dHRiYXJkVERBUlBUR3FZaVZnWmxQZXNmVXZaYUloVEwxZkdHV3JDT296TnpGaklnR3lPZnRZYXpoS25qUDJjVUQwWUFn; PREF=f6=40000400&f7=4150&tz=Asia.Jakarta&autoplay=true; SIDCC=AKEyXzUkN4t6vyjBsSbMrq9damqPfO-MTYD_xUivcUJ8_u1enpiIZfOKPHtmcS_Mkty61k1i51Y; __Secure-1PSIDCC=AKEyXzUwkZu6VPG21Xq0ABBnEpbpULiDEEWKch4WdpDZo644F4d57NMIpOXV-IZgy3V0KTg-dv0; __Secure-3PSIDCC=AKEyXzV7e1uufCYotIFRQB0EbGvvFKvAaSWL9sVMzSxOLheqAqjGPoXdSXc7ACuqLw1CMsrZ7z4',
            "priority": "u=0, i",
            "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            "sec-ch-ua-arch": '"x86"',
            "sec-ch-ua-bitness": '"64"',
            "sec-ch-ua-form-factors": '"Desktop"',
            "sec-ch-ua-full-version": '"126.0.6478.127"',
            "sec-ch-ua-full-version-list": '"Not/A)Brand";v="8.0.0.0", "Chromium";v="126.0.6478.127", "Google Chrome";v="126.0.6478.127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": '""',
            "sec-ch-ua-platform": '"Windows"',
            "sec-ch-ua-platform-version": '"15.0.0"',
            "sec-ch-ua-wow64": "?0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "service-worker-navigation-preload": "true",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "x-client-data": "CJC2yQEIo7bJAQipncoBCNHuygEIlaHLAQj9mM0BCIWgzQEI2/zNAQjpk84BCMWdzgEIqp7OAQizn84BCJ6izgEI26fOARjX680BGKCdzgE=",
        }

        self.header = dict()
        self.header["authority"] = "www.youtube.com"
        self.header["content-type"] = "application/json"
        self.header["user-agent"] = (
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0"
        )
        self.header["cookie"] = (
            "VISITOR_INFO1_LIVE=a7yMpE8rpzs; LOGIN_INFO=AFmmF2swRQIgfZWO72tAWT8lhqG90fZVhNmnXteJQC2Yv1K148JHjNECIQC5amiyqRFIJSZnmCCD4br6934RaAScuRPIqpyBLe-CQA:QUQ3MjNmelF5ejJqc0h0U1VaSURVUjl2YXM0aDFMNnI0cFYtUE94SkdOcWlod0VGRG5JT19SZHNsdmFOQ05BRnVuNVowR3RQRHdGZHhPWUVEY2lLRVJELXVFV2taU0tWdUhGblI1blMxalBrLTBLYUlGdmh2NzVVUFpLZ1NORTRjRVNGVmVuT0gyS1JCakNzdld0RS1lRWROaS0wSDdKVVpzYl8wMTltV1dVYWRrS0JSSEF4Z3JR; PREF=tz=Asia.Jakarta&al=en; HSID=AxLULLeuiJrbjGMUW; SSID=AobN152mTsbhfYeL3; APISID=Edv9sp7dArE5hUn7/AfasFVpiaBuafrWam; SAPISID=ipoEYUk4dikEGt3y/AZAn_Nc2pIn3H7ZgT; __Secure-3PAPISID=ipoEYUk4dikEGt3y/AZAn_Nc2pIn3H7ZgT; SID=6QcItjfqMAfl7W-06OBzIDR1qjSFlfY1lU-S1A6OwLBxCGn64HwAz7YQS-ULc4x_cBzDzA.; __Secure-3PSID=6QcItjfqMAfl7W-06OBzIDR1qjSFlfY1lU-S1A6OwLBxCGn6rJ2AcxyrTi6mkpeQfGhlsQ.; YSC=lIzCv4TRIPQ; SIDCC=AJi4QfHiBwkqLz8g6gOqT5VgKcrAWmZuosVxSMPp3whXVMAXvFY2VhoeZ3A5r4MVFe1kXdeYXA; __Secure-3PSIDCC=AJi4QfEG7xjDrQ7_hr1LQpix8tpBOTg8TZNcI3wr-ywhxfkcPocdZAtOJ4aVXEXXzoZafzH573Q"
        )
        self.header["accept"] = "*/*"
        self.header["accept-language"] = "en-US,en;q=0.9,id-ID;q=0.8,id;q=0.7"
        self.header["origin"] = "https://www.youtube.com"
        self.header["sec-fetch-site"] = "same-origin"
        self.header["sec-fetch-mode"] = "cors"
        self.header["sec-fetch-dest"] = "empty"
        self.header["x-client-data"] = (
            "CIi2yQEIo7bJAQipncoBCIa1ygEImbXKAQisx8oBCKidywE="
        )
        self.header["x-youtube-client-name"] = "1"
        self.header["x-youtube-identity-token"] = (
            "QUFFLUhqbnZCM0ZRU0s4cHQ2WV9oUDBtT0xHWS1WeW1tZ3w="
        )
        self.header["x-youtube-time-zone"] = "Asia/Jakarta"
        self.header["x-youtube-utc-offset"] = "420"
        self.header["x-youtube-client-version"] = "2.20210201.07.01"

    
    async def get_channel_id(self, username):
        try:
            _type = "c"
            username =  username.lstrip("@")
            while True:
                # path = "{}/@{}/videos".format(_type, username)
                path = f"@{username}/videos"
                url = urljoin(self.BASE_URL, path)
                response = await self.session.get(url=url, headers=self.headers, timeout=60)
                code = response.status_code
                if code != 200:
                    if code == 404 and _type == "c":
                        _type = "channel"
                    elif code == 404 and _type == "channel":
                        _type = ""
                    elif code == 404 and _type == "":
                        raise Exception("channel not found")
                    else:
                        raise Exception(
                            "[{}] {}".format(response.status_code, response.reason_phrase)
                        )
                else:
                    break

            scripts = self.parser.pyq_parser(
                response.text, 'script:contains("channelMetadataRenderer")'
            )

            for script in scripts:
                data = script.text
                if re.search("var\sytInitialData", data):
                    data = unicodedata.normalize("NFKD", data).encode("ascii", "ignore")
                    data = re.sub(
                        "var\sytInitialData\s?\=\s?", "", data.decode("utf-8")
                    )
                    data = re.sub("\;$", "", data)
                    data = json.loads(data)
                    data = data.get("metadata", {}).get("channelMetadataRenderer", {})
                    return {"id": data["externalId"]}
        except Exception as e:
            raise e
    

    async def get_channel_detail(self, channel_id):
        try:
            keys = [
                "AIzaSyB6AZKb77eOYwjlAnSqzJcMNL-enL88xB8"
            ]
            api_key = random.choice(keys)


            # token_data = self._get_token()
            # api_key = token_data["token_api"]
            # print(api_key)

            endpoint = Endpoints.channel_detail_info(
                id=channel_id,
                apikey=api_key
            )

            response = await self.session.get(endpoint, timeout=60)

            
            res_json = response.json()

            return res_json
        except Exception as e:
            self.log.error(f"Error get channel", e)
            return
            

    
    async def get_post_detail(self, video_id):
        
        try:
            keys = [
                "AIzaSyB6AZKb77eOYwjlAnSqzJcMNL-enL88xB8",
                "AIzaSyBYT98jOlT96U9mOAMy2gkfHxPUVUG-M3U"
            ]
            api_key = random.choice(keys)


            # token_data = self._get_token()
            # api_key = token_data["token_api"]
            print(api_key)

            endpoint = Endpoints.video_raw_data(
                video_id=video_id,
                apikey=api_key
            )

            response = await self.session.get(endpoint, timeout=60)

            
            
            res_json = response.json()


            return res_json
        except Exception as e:
            self.log.error("Error Fetch video detail", e)
            return
    
    async def get_detail_keyword(self, keyword):
        try:
            keys = [
                "AIzaSyB6AZKb77eOYwjlAnSqzJcMNL-enL88xB8",
                "AIzaSyBYT98jOlT96U9mOAMy2gkfHxPUVUG-M3U"
            ]
            api_key = random.choice(keys)

            endpoint = Endpoints.keywords(
                query=keyword,
                api_key=api_key
            )

            response = await self.session.get(endpoint, timeout=60)

            if response.status_code != 200:
                raise Exception(
                    "[{}] {}".format(response.status_code, response.reason_phrase)
                )

            res_json = response.json()

            return res_json
        except Exception as e:
            self.log.error(f"Error Fetch video detail by keyword: {e}")
            raise

    # ------------------------------------------------------------------
    # Search tanpa API key — scraping halaman hasil pencarian YouTube
    # ------------------------------------------------------------------
    async def get_detail_keyword_no_api(self, keyword: str):
        """
        Cari video YouTube berdasarkan keyword TANPA API key.
        Scraping langsung dari halaman https://www.youtube.com/results.

        Return format kompatibel dengan YouTube Data API (items[]).
        """
        try:
            url = Endpoints.search(keyword)
            self.log.info(f"Scraping YouTube search: {url}")

            # Ambil halaman hasil pencarian dengan headers web browser
            response = await self.session.get(
                url,
                headers=self.headers,
                timeout=30,
            )

            if response.status_code != 200:
                raise Exception(
                    "[{}] {}".format(response.status_code, response.reason_phrase)
                )

            # Parse ytInitialData dari HTML
            html = response.text
            
            data = self._extract_yt_initial_data(html)
            
            if data is None:
                raise Exception("ytInitialData tidak ditemukan di halaman")

            # Ekstrak video renderer dari struktur JSON
            items = self._parse_search_results(data)

            return {"items": items}

        except Exception as e:
            self.log.error(f"Error scraping YouTube search: {e}")
            raise

    def _extract_yt_initial_data(self, html: str) -> dict | None:
        """Ekstrak objek ytInitialData dari HTML YouTube."""
        try:
            # Simpan HTML mentah untuk debug
            import os
            os.makedirs("/tmp/yt_debug", exist_ok=True)
            with open("/tmp/yt_debug/response.html", "w", encoding="utf-8") as f:
                f.write(html)
            self.log.info(f"HTML saved to /tmp/yt_debug/response.html ({len(html)} bytes)")

            # Coba beberapa pola regex
            patterns = [
                r"var\s+ytInitialData\s*=\s*",
                r"window\[\"ytInitialData\"\]\s*=\s*",
                r"ytInitialData\s*=\s*",
            ]

            scripts = self.parser.pyq_parser(
                html, 'script:contains("ytInitialData")'
            )
            self.log.info(f"Found {len(scripts)} script tag(s) containing ytInitialData")

            for script in scripts:
                text = script.text
                self.log.info(f"Script text length: {len(text)}, has 'var ytInitialData': {'var ytInitialData' in text}")

                for pattern in patterns:
                    if re.search(pattern, text):
                        # Bersihkan tanpa menghilangkan non-ASCII
                        data = re.sub(pattern, "", text)
                        data = re.sub(r"\s*;\s*$", "", data)

                        # Coba parse
                        try:
                            parsed = json.loads(data)
                            # Simpan hasil parse untuk debug
                            with open("/tmp/yt_debug/ytInitialData.json", "w", encoding="utf-8") as f:
                                json.dump(parsed, f, indent=2, ensure_ascii=False)
                            self.log.info(f"ytInitialData parsed successfully, saved to /tmp/yt_debug/ytInitialData.json")

                            # Log top-level keys
                            keys = list(parsed.keys())
                            self.log.info(f"Top-level keys: {keys}")
                            return parsed
                        except json.JSONDecodeError as e:
                            self.log.warning(f"JSON parse failed with pattern '{pattern}': {e}")
                            # Simpan data yang gagal diparse
                            with open("/tmp/yt_debug/failed_parse.txt", "w", encoding="utf-8") as f:
                                f.write(data[:5000])
                            continue

            self.log.error("No ytInitialData found in any script tag")
            return None

        except Exception as e:
            self.log.error(f"Gagal parse ytInitialData: {e}")
            return None
    
    def channel_search(self, channel_name):
        try:
            path = "results?search_query={}&sp=EgIQAg%253D%253D".format(channel_name)
            url = urljoin(self.BASE_URL, path)
            response = requests.get(url=url, headers=self.headers, timeout=60)
            code = response.status_code
            if code != 200:
                if code == 404:
                    raise Exception("channel not found")
                else:
                    raise Exception(
                        "[{}] {}".format(response.status_code, response.reason)
                    )
            scripts = self.parser.pyq_parser(
                response.text, 'script:contains("twoColumnSearchResultsRenderer")'
            )
            channel_list = []
            for script in scripts:
                data = script.text
                if re.search("var\sytInitialData", data):
                    data = unicodedata.normalize("NFKD", data).encode("ascii", "ignore")
                    data = re.sub(
                        "var\sytInitialData\s?\=\s?", "", data.decode("utf-8")
                    )
                    data = re.sub("\;$", "", data)
                    data = json.loads(data)
                    primaryContents = (
                        data.get("contents", {})
                        .get("twoColumnSearchResultsRenderer", {})
                        .get("primaryContents", {})
                    )
                    sectionListRenderer = primaryContents.get(
                        "sectionListRenderer", {}
                    ).get("contents", [])
                    contents = (
                        sectionListRenderer[0]
                        .get("itemSectionRenderer", {})
                        .get("contents", [])
                    )
                    if contents:
                        for content in contents:
                            content = content.get("channelRenderer", {})
                            if content:
                                channel_list.append(content)
            return channel_list
        except Exception as e:
            raise e

    def _parse_search_results(self, data: dict) -> list:
        """
        Parse struktur ytInitialData hasil search → list item.
        Support multiple YouTube layouts (lama & baru).
        """
        items = []
        try:
            primary = (
                data.get("contents", {})
                .get("twoColumnSearchResultsRenderer", {})
                .get("primaryContents", {})
            )
            

            # Jalur 1: sectionListRenderer (layout klasik)
            sections = primary.get("sectionListRenderer", {}).get("contents", [])

            # Jalur 2: richGridRenderer (layout baru YouTube)
            if not sections:
                sections = primary.get("richGridRenderer", {}).get("contents", [])

            self.log.info(f"DEBUG: got {len(sections)} section(s)")

            for section in sections:
                # itemSectionRenderer (klasik)
                entries = section.get("itemSectionRenderer", {}).get("contents", [])

                # richItemRenderer (layout baru)
                if not entries:
                    rich = section.get("richItemRenderer", {})
                    if rich:
                        entries = [rich.get("content", {})]

                for entry in entries:
                    video = entry.get("videoRenderer")
                    if not video:
                        # richItemRenderer membungkus videoRenderer
                        video = (
                            entry.get("richItemRenderer", {})
                            .get("content", {})
                            .get("videoRenderer")
                        )
                    if not video:
                        continue

                    video_id = video.get("videoId", "")
                    title_runs = video.get("title", {}).get("runs", [])
                    title = "".join(r.get("text", "") for r in title_runs) if title_runs else ""

                    channel_runs = (
                        video.get("ownerText", {})
                        .get("runs", [])
                    )
                    channel_title = (
                        channel_runs[0].get("text", "") if channel_runs else ""
                    )

                    # Channel ID dari navigationEndpoint (primer)
                    channel_id = (
                        channel_runs[0]
                        .get("navigationEndpoint", {})
                        .get("browseEndpoint", {})
                        .get("browseId", "")
                        if channel_runs
                        else ""
                    )

                    # Fallback: channel_search kalau navigationEndpoint kosong
                    if not channel_id and channel_title:
                        try:
                            search_channel = self.channel_search(channel_title)
                            if search_channel:
                                channel_id = search_channel[0].get("channelId", "")
                        except Exception:
                            pass

                    thumbnails = video.get("thumbnail", {}).get("thumbnails", [])
                    thumbnail_url = thumbnails[-1].get("url", "") if thumbnails else ""

                    published = video.get("publishedTimeText", {}).get("simpleText", "")
                    views = video.get("viewCountText", {}).get("simpleText", "")
                    length = video.get("lengthText", {}).get("simpleText", "")

                    items.append({
                        "id": {"videoId": video_id},
                        "snippet": {
                            "title": title,
                            "channelTitle": channel_title,
                            "channelId": channel_id,
                            "publishedAt": published,
                            "thumbnails": {
                                "default": {"url": thumbnail_url},
                            },
                        },
                        "statistics": {
                            "viewCount": views,
                        },
                        "contentDetails": {
                            "duration": length,
                        },
                    })

            self.log.info(f"Parsed {len(items)} video(s) from search results")

        except Exception as e:
            self.log.error(f"Gagal parse search results: {e}")

        return items