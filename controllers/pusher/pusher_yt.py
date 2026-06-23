from controllers import Controllers
import json
import requests
import pandas as pd
import openpyxl

class PusherYT(Controllers):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_username(self, channel_id):
        url = "http://10.11.121.1:30019/youtube/web/account/detail"

        params = {
            'channel_id': channel_id,
        }
        response = requests.get(url, params=params, verify=False)

        res_json = response.json()
        data = res_json.get("data", {})
        content = data.get("content", {}).get("pageHeaderViewModel", {}).get("metadata", {}).get("contentMetadataViewModel", {})


        metadata_rows = content.get("metadataRows", [])

        username = None

        if metadata_rows:
            metadata_parts = metadata_rows[0].get("metadataParts", [])

            if metadata_parts:
                username = (
                    metadata_parts[0]
                    .get("text", {})
                    .get("content")
                )

        username = username.replace("@", "") if username else None
        return username
    
    async def pusher(self):
        try:
            with open("not_found_platform.json", "r", encoding="utf-8") as f:
                data = json.load(f)

            for item in data:
                try:
                    # jika item string langsung pakai
                    if isinstance(item, str):
                        channel_id = item

                    # jika item dict ambil user_id
                    elif isinstance(item, dict):
                        channel_id = item.get("user_id")

                    else:
                        self.log.warning(f"Invalid item format: {item}")
                        continue

                    if not channel_id:
                        self.log.warning(f"Channel ID not found: {item}")
                        continue

                    username = self.get_username(channel_id)

                    if not username:
                        self.log.warning(
                            f"Username not found for channel_id {channel_id}"
                        )
                        continue

                    self.output.put(json.dumps({
                        "channel_id": channel_id,
                        "cache": False,
                        "track_comment": False,
                        "tags": [username],
                        "media_tags": [username, "videos"],
                        "username": username
                    }))

                    self.log.success(
                        f"Get username for channel_id {channel_id}: {username}"
                    )

                except Exception as e:
                    self.log.error(f"Error processing item {item}: {str(e)}")
                    continue

        except Exception as e:
            self.log.error(f"Error in pusher: {str(e)}")
        
    
    