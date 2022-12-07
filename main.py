import sys

from CPCBT.crawl.fetch import Fetch

if __name__ == '__main__':

    log_file = open("./log/log_ticker2.txt", mode="a")
    config = {"save_path": r"D:\data",
              "start_date": "2020-12-01",
              "end_date": "2022-11-01",
              "retry": 1,
              "log": {
                  "steams": [sys.stdout, log_file]
                }
              }

    fetch = Fetch(config)
    fetch.fetch_ticker()

