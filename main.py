import sys

from CPCBT.crawl.fetch import Fetch

if __name__ == '__main__':

    log_file = open("./log_ticker1.txt", mode="a")
    config = {"save_path": r"C:/youmi/data/",
              "start_date": "2020-01-01",
              "end_date": "2022-11-01",
              "log": {"steams": [sys.stdout, log_file]
                      }
              }

    fetch = Fetch(config)
    fetch.fetch_ticker()

