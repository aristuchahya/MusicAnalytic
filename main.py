import argparse
import asyncio

def run_engine(**kwargs):
    if kwargs.get("which") == "crawler":
        if kwargs.get("mode") == "test":
            from controllers.hello.base_crawl import BaseCrawl
            c = BaseCrawl(**kwargs)
            asyncio.run(c.main())
        
        if kwargs.get("mode") == "idx":
            from controllers.idx.ingest import IngestDaily
            c = IngestDaily(**kwargs)
            asyncio.run(c.main())
        
        if kwargs.get("mode") == "idx-historical":
            from controllers.idx.ingest import IngestHistorical
            c = IngestHistorical(**kwargs)
            asyncio.run(c.main())
        
        if kwargs.get("mode") == "ingest-profile":
            from controllers.idx.ingest import IngestProfile
            c = IngestProfile(**kwargs)
            asyncio.run(c.main())
        
        if kwargs.get("mode") == "pusher-yt":
            from controllers.pusher.pusher_yt import PusherYT
            c = PusherYT(**kwargs)
            asyncio.run(c.pusher())
        
        if kwargs.get("mode") == "ingest-stockbit":
            from controllers.stockbit.base import StockbitBase
            c = StockbitBase(**kwargs)
            asyncio.run(c.main())
        
        if kwargs.get("mode") == "music":
            from controllers.crawling.base import BaseCrawl
            c = BaseCrawl(**kwargs)
            asyncio.run(c.main())
        
        if kwargs.get("mode") == "insert":
            from controllers.insert.base import InsertData
            c = InsertData(**kwargs)
            asyncio.run(c.main())

    if kwargs.get("which") == "etl":
        from controllers.etl.base import ETLController
        c = ETLController(**kwargs)
        asyncio.run(c.main())


def main():
    argp = argparse.ArgumentParser()
    argp.add_argument("-c", "--config", dest="config", type=str, default="config.ini")
    argp.add_argument("-s", "--source", dest="source", type=str)
    argp.add_argument("-d", "--destination", dest="destination", type=str)
    argp.add_argument("-i", "--input", dest="input", type=str)
    argp.add_argument("-o", "--output", dest="output", type=str)
    argp.add_argument("--bootstrap-servers", dest="bootstrap_servers", type=str)
    argp.add_argument("--beanstalk-host", dest="beanstalk_host", type=str)
    argp.add_argument("--beanstalk-port", dest="beanstalk_port", type=int)

    argp_sub = argp.add_subparsers(title="action", help="-h / --help to see usage")

    argp_crawler = argp_sub.add_parser("crawler")
    argp_crawler.set_defaults(which="crawler")
    argp_crawler.add_argument("--mode", dest="mode", type=str)
    argp_crawler.add_argument("--type", dest="type", type=str)

    argp_etl = argp_sub.add_parser("etl")
    argp_etl.set_defaults(which="etl")
    argp_etl.add_argument("--mode", dest="mode", type=str, default="all",
                          choices=["bronze", "silver", "gold", "all"])
    argp_etl.add_argument("--batch-id", dest="batch_id", type=str)

    args = argp.parse_args()

    run_engine(**vars(args))


if __name__ == "__main__":
    main()
