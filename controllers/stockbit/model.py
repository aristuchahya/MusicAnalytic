
from controllers import Controllers

class ModelTable(Controllers):

    def create_table_comment(self):
        try:
            query = """
            CREATE TABLE IF NOT EXISTS stockbit.stockbit_comments (
            id TEXT PRIMARY KEY,
            user_id TEXT REFERENCES stockbit.stockbit_users(id),
            source TEXT,
            company_code TEXT REFERENCES stockbit.stockbit_companies(symbol),
            content TEXT,
            content_original TEXT,
            liked INTEGER,
            likes INTEGER,
            dislikes INTEGER,
            replies INTEGER,
            saved INTEGER,
            trending INTEGER,
            is_pinned INTEGER,
            is_news INTEGER,
            is_pool INTEGER,
            no_reply INTEGER,
            reposted INTEGER,
            reposted_from TEXT,
            reply_to INTEGER,
            total_share INTEGER,
            is_report INTEGER,
            commenter_type TEXT,
            total_view INTEGER,
            trade_share JSONB,
            newsfeed_source TEXT,
            newsfeed_label TEXT,
            newsfeed_img TEXT,
            link_preview TEXT,
            image_frame_type TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            scraped_at TIMESTAMP
            )
            """

            self.conn.excecute_query(query)
            print("Successfull created table comment")
        except Exception as e:
            self.log.error(f"Error creating table comment: {e}")
    
    def create_table_users(self):
        try:
            query = """
            CREATE TABLE IF NOT EXISTS stockbit.stockbit_users (
            id TEXT PRIMARY KEY,
            username varchar,
            fullname varchar,
            avatar TEXT,
            official BOOLEAN,
            user_priv INTEGER,
            is_author INTEGER,
            is_pro INTEGER,
            follow INTEGER,
            country TEXT,
            verified_status TEXT
            )
            """

            self.conn.excecute_query(query)
            print("Successfull created table users")
        except Exception as e:
            self.log.error(f"Error creating table users: {e}")
    
    def create_table_comment_topics(self):
        try:
            query = """
            CREATE TABLE IF NOT EXISTS stockbit.stockbit_comment_topics (
            comment_id TEXT REFERENCES stockbit.stockbit_comments(id),
            company_code TEXT REFERENCES stockbit.stockbit_companies(symbol)
            )
            """

            self.conn.excecute_query(query)
            self.log.success("Successfull created table comment_topics")
        except Exception as e:
            self.log.error(f"Error creating table comment_topics: {e}")
    

    def create_table_comment_images(self):
        try:
            query = """
            CREATE TABLE IF NOT EXISTS stockbit.stockbit_comment_images (
            id TEXT PRIMARY KEY,
            comment_id TEXT REFERENCES stockbit.stockbit_comments(id),
            image_url TEXT,
            width INTEGER,
            height INTEGER,
            ratio FLOAT,
            frame TEXT,
            frame_type TEXT
            )
            """

            self.conn.excecute_query(query)
            self.log.success("Successfull created table comment_images")
        except Exception as e:
            self.log.error(f"Error creating table comment_images: {e}")
