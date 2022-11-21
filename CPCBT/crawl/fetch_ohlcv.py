import os
import time

import ccxt
import pandas as pd

from arrow import arrow


def fetch_ohlcv(self):
	"""
	fetch ohlcv of 1m<timeframe> kline
	:return:
	"""
	base_path = os.path.join(self.save_path, "ohlcv")

	# set range of time：[base, end_date)
	base = arrow.get(self.base_time)
	base = int(base.timestamp())

	end_date = arrow.get(self.end_time)
	end_date = int(end_date.timestamp())

	quote_lst = self.quote_lst

	# exchange_class = ccxt.binance
	exchange_class = getattr(ccxt, self.exchanges[0])
	exchange = exchange_class(self.ccxt_config)
	exchange_default = exchange

	# get all symbols in market
	#       data structure{quote: [symbol1, symbol2, ...]}
	symbols = {}
	market_info = exchange.fetchMarkets()

	for item in market_info:
		if item["quote"] not in symbols.keys():
			symbols.update({item["quote"]: []})

		symbols[item["quote"]].append(item["symbol"])

	# set window is 1 day, it means will get 1 day data of 1m for once
	# step = 60 * 60 * 24
	step = self.step * 60

	for start_time in range(base, end_date, step):
		end_time = start_time + step
		limit = step // self.step_tool(self.timeframe)

		for quote in symbols.keys():
			if len(quote) != 0 or quote is not None:
				if quote not in quote_lst:
					continue

			base_dir = os.path.join(base_path, f"quote[{quote}]")
			if not os.path.exists(base_dir):
				os.makedirs(base_dir)

			for symbol in symbols.get(quote):
				# remove '/', like: change BTC/USDT to BTCUSDT
				symbol = symbol.replace("/", '')
				symbol_dir = os.path.join(base_dir, symbol)
				if not os.path.exists(symbol_dir):
					os.makedirs(symbol_dir)

				self.log.info(
					f"""[{symbol}] fetch ohlcv from {arrow.get(start_time).format('YYYY-MM-DD')} to {arrow.get(end_time).format('YYYY-MM-DD')}""", "")

				# fetch main
				ex_index = 0
				no_data = False
				kline_lst = []
				exchange = exchange_default
				while True:
					try:
						# try to fetch data from default exchange
						kline_lst = exchange.fetch_ohlcv(symbol, timeframe=self.timeframe, since=start_time, limit=limit)
						break
					except Exception as e:
						time.sleep(10)
						# if fetch fail
						self.log.warning(
							f"""{self.exchanges[ex_index]}: [{symbol}] [{arrow.get(start_time).format('YYYY-MM-DD')}, {arrow.get(end_time).format('YYYY-MM-DD')}) don't exit""", "\n")

						# try to use next exchange
						try:
							ex_index += 1
							exchange_class = getattr(ccxt, self.exchanges[ex_index])
							exchange = exchange_class(self.ccxt_config)
						except Exception as e:
							# if the last exchange is None, break and set no_data True
							no_data = True
							break

				# if no data is True, do nothing and continue to next code
				if no_data: continue

				# change kline to dataframe
				kline_df = pd.DataFrame(kline_lst)
				kline_df.columns = ["timestamp_raw", "open", "high", "low", "close", "volume"]
				kline_df["symbol"] = symbol
				kline_df["time"] = kline_df["timestamp_raw"].map(
					lambda x: arrow.get(x).format('YYYY-MM-DD HH:mm:ss'))
				kline_df["date"] = kline_df["timestamp_raw"].map(lambda x: arrow.get(x).format())
				kline_df.reset_index(inplace=True, drop=True)

				kline_path = os.path.join(symbol_dir, f"{arrow.get(start_time).format('YYYY-MM-DD')}_{arrow.get(end_time).format('YYYY-MM-DD')}.csv")

				kline_df.to_csv(kline_path)
				self.log.info(f"""[{symbol}] fetch finish""", "\n")

