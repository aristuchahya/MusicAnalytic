import urllib
import urllib.parse as urlparse
from urllib.parse import urljoin, urlencode


class  Endpoints:
    BASE_URL = "https://www.googleapis.com"
    API_URL = "https://www.googleapis.com/youtube/v3"
    CHANNEL_SEARCH = "{api_url}/search?part=snippet&q={query}&maxResults={max_result}&order=relevance&relevanceLanguage={language}&type={type}&pageToken={page_token}&key={apikey}"
    CHANNEL_BY_ID_INFO = "{api_url}/channels?part=snippet,brandingSettings,statistics,topicDetails,contentDetails&id={id}&key={apikey}"
    CHANNEL_BY_USERNAME_INFO = "{api_url}/channels?part=snippet,brandingSettings,statistics,topicDetails,contentDetails&forUsername={username}&key={apikey}"
    KEYWORD = "{api_url}/search?part=snippet&maxResults=10&q={query}&key={api_key}"

    CHANNEL_INFO_BY_ID = "{api_url}/channels?part=snippet,contentDetails,statistics&id={id}&key={apikey}"

    __VIDEOS_ALT = "https://www.youtube.com/channel/{channel_id}/videos"
    __VIDEO_DETAIL = "{api_url}/videos?part=snippet,statistics,recordingDetails&id={id}&key={apikey}&access_token={access_token}"

    __SEARCH = "https://www.youtube.com/results?sp=CAISAhAB&search_query={query}"

    __COMMENT_LIST = "{api_url}/commentThreads?part=snippet,replies&videoId={id}&maxResults={max_result}&order=time&pageToken={page_token}&access_token={access_token}&key={apikey}"
    __COMMENT_DETAIL = "{api_url}/comments?part=snippet&id={id}&access_token={access_token}&key={apikey}"
    __VIDEO_RAW_DATA = "{api_url}/videos?part=snippet,contentDetails,statistics,status,topicDetails,recordingDetails,player,localizations&id={id}&key={apikey}"

    def __init__(self):
        pass

    @staticmethod
    def search_channel(
        query, max_result=50, page_token="", language="id", _type="channel", apikey=""
    ):
        if max_result < 1 or max_result > 50:
            max_result = 50
        endpoint = Endpoints.CHANNEL_SEARCH.format(
            api_url=Endpoints.API_URL,
            query=query,
            max_result=max_result,
            page_token=page_token,
            apikey=apikey,
            language=language,
            type=_type,
        )
        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlparse.urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )

    @staticmethod
    def channel_info(id=None, username=None, apikey=""):
        if id:
            endpoint = Endpoints.CHANNEL_BY_ID_INFO.format(
                api_url=Endpoints.API_URL, id=id, apikey=apikey
            )
        else:
            endpoint = Endpoints.CHANNEL_BY_USERNAME_INFO.format(
                api_url=Endpoints.API_URL, username=username, apikey=apikey
            )
        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlparse.urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )
    
    @staticmethod
    def channel_detail_info(id=None, apikey=""):
        endpoint = Endpoints.CHANNEL_INFO_BY_ID.format(
            api_url=Endpoints.API_URL, id=id, apikey=apikey
        )

        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlparse.urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )

    @staticmethod
    def parse_qstring(url):
        return dict(urlparse.parse_qsl(urlparse.urlsplit(url).query))

    @staticmethod
    def videos_alt(channel_id):
        return Endpoints.__VIDEOS_ALT.format(channel_id=channel_id)

    @staticmethod
    def video_detail(video_id, access_token="", apikey=""):
        endpoint = Endpoints.__VIDEO_DETAIL.format(
            api_url=Endpoints.API_URL,
            id=video_id,
            access_token=access_token,
            apikey=apikey,
        )
        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )
    
    @staticmethod
    def keywords(query, api_key=""):
        endpoint = Endpoints.KEYWORD.format(
            api_url=Endpoints.API_URL,
            query=query,
            api_key=api_key,
        )
        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )

    @staticmethod
    def search(query):
        return Endpoints.__SEARCH.format(query=query)

    @staticmethod
    def comments(video_id, max_result=100, page_token="", access_token="", apikey=""):
        if max_result < 1 or max_result > 100:
            max_result = 100
        endpoint = Endpoints.__COMMENT_LIST.format(
            api_url=Endpoints.API_URL,
            id=video_id,
            max_result=max_result,
            page_token=page_token,
            access_token=access_token,
            apikey=apikey,
        )
        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )

    @staticmethod
    def comment_detail(comment_id, access_token="", apikey=""):
        # cid1,cid2,cidn, if multiple comment id
        endpoint = Endpoints.__COMMENT_DETAIL.format(
            api_url=Endpoints.API_URL,
            id=comment_id,
            access_token=access_token,
            apikey=apikey,
        )
        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )

    @staticmethod
    def video_raw_data(video_id, apikey=""):
        endpoint = Endpoints.__VIDEO_RAW_DATA.format(
            api_url=Endpoints.API_URL,
            id=video_id,
            apikey=apikey,
        )
        parseurl = urlparse.urlsplit(endpoint)
        qstring = urlencode(Endpoints.parse_qstring(endpoint))

        return "{base_url}{path}?{query}".format(
            base_url=Endpoints.BASE_URL, path=parseurl.path, query=qstring
        )


if __name__ == "__main__":
    print(
        Endpoints.video_detail(
            video_id="NsD7dqblJJo",
            apikey="AIzaSyBnEEX3BaSlziNvhw4At_u1c8ez5FRsN7c",
        )
    )
