import hashlib
import os.path
import time
import zipfile

import requests

import ccxt
import arrow


def fetch_ticker(self):
	"""
	获取ticker数据
		字段tradeId	    price       qty     quoteQty    time	isBuyerMaker
	:return:
	"""
	base_path = os.path.join(self.save_path, "ticker")
	save_path = os.path.join(base_path, "quote[USDT]")
	end_time = arrow.get(self.end_time)

	# fetch
	# exchange_class = ccxt.binance
	exchange_class = getattr(ccxt, self.exchanges[0])
	exchange = exchange_class(self.ccxt_config)

	# get all symbols in market
	#       data structure{quote: [symbol1, symbol2, ...]}
	symbols = []
	market_info = exchange.fetchMarkets()

	for item in market_info:
		if item["quote"] != "USDT":
			continue

		symbols.append(item["symbol"].replace("/", "").strip())

	# count0: fetch data network failed times count
	# count1: fetch check file network failed times count
	# count2: data check failed times count
	count0, count1, count2 = 0, 0, 0

	# index of code now
	code_index = 0
	# the day data fetch now
	date = arrow.get(self.base_time)
	while True:
		if date > end_time: return 0

		code = symbols[code_index]
		self.log.info(f"""[{code}] fetch ticker in {date.format('YYYY-MM-DD')}""", "")

		url = f"https://data.binance.vision/data/futures/um/daily/trades/{code}/{code}-trades-{date.format('YYYY-MM-DD')}.zip"
		resp = None
		try:
			resp = requests.get(url, proxies=self.ccxt_config['proxies'])
		except: ...
		# check if resp fetch data success
		if resp is None or resp.status_code != 200:
			time.sleep(10)
			count0 += 1
			if count0 < self.retry:
				self.log.warning(f"""[{code}] ticker[{code}-trades-{date.format('YYYY-MM-DD')}.zip] don't exit or network error, try again[{count0}]""", "")
				if resp is not None:
					resp.close()
				continue

			else:
				self.log.warning(f"""[{code}] ticker[{code}-trades-{date.format('YYYY-MM-DD')}.zip] don't exit or network error, try failed[{count0}]""", "\n")
				code_index += 1
				count0 = 0
				if code_index >= len(symbols):
					date = date.shift(days=1)
					code_index = 0

				if resp is not None:
					resp.close()
				continue

		# check data
		checksum = hashlib.sha256(resp.content).hexdigest()
		resp_cksum = None
		try:
			resp_cksum = requests.get(url + ".CHECKSUM", proxies=self.ccxt_config['proxies'])
		except: ...


		# check if resp_cksum fetch check file[CHECKSUM] success
		if resp_cksum is None or resp_cksum.status_code != 200:
			time.sleep(10)
			count1 += 1
			if count1 < self.retry:
				self.log.warning(f"""[{code}] ticker[{code}-trades-{date.format('YYYY-MM-DD')}.zip.CHECKSUM] don't exit or network error, try again[{count1}]""", "")
				if resp is not None:
					resp.close()

				if resp_cksum is not None:
					resp_cksum.close()
				continue

			else:
				self.log.warning(f"""[{code}] ticker[{code}-trades-{date.format('YYYY-MM-DD')}.zip.CHECKSUM] don't exit or network error, try failed[{count1}]""", "\n")
				code_index += 1
				count1 = 0
				if code_index >= len(symbols):
					date = date.shift(days=1)
					code_index = 0

				if resp is not None:
					resp.close()

				if resp_cksum is not None:
					resp_cksum.close()
				continue

		# check if file has error in route
		cksum = resp_cksum.text.split(" ")[0].strip()
		filename = resp_cksum.text.split(" ")[-1].replace("\n", "").strip()
		if not (filename == f'{code}-trades-{date.format("YYYY-MM-DD")}.zip' and checksum == cksum):
			time.sleep(10)
			count2 += 1
			if count2 < self.retry:
				self.log.warning(f"""[{code}] ticker in {date.format('YYYY-MM-DD')} fetch error, try again[{count2}]""", "")
				if resp_cksum is not None:
					resp_cksum.close()

				if resp is not None:
					resp.close()

				continue

			else:
				self.log.warning(f"""[{code}] ticker in {date.format('YYYY-MM-DD')} fetch error, try failed[{count2}]""", "\n")
				code_index += 1
				count2 = 0
				if code_index >= len(symbols):
					date = date.shift(days=1)
					code_index = 0

				if resp_cksum is not None:
					resp_cksum.close()

				if resp is not None:
					resp.close()
				continue

		# reset if success
		count0, count1, count2 = 0, 0, 0

		# save data
		filepath = os.path.join(save_path, f'{code}')
		if not os.path.exists(filepath):
			os.makedirs(filepath)

		filename = os.path.join(filepath, f"{code}-trades-{date.format('YYYY-MM-DD')}.zip")
		with open(filename, mode="wb") as f:
			f.write(resp.content)

		self.log.info(f"""[{code}] ticker in {date.format('YYYY-MM-DD')} save zip success""", "")

		# decode data
		fs = zipfile.ZipFile(filename, 'r')
		fs.extract(f"{code}-trades-{date.format('YYYY-MM-DD')}.csv", filepath)
		fs.close()

		self.log.info(f"""[{code}] ticker in {date.format('YYYY-MM-DD')} trans to csv success""", "")

		# delete zip file
		os.remove(filename)
		self.log.info(f"""[{code}] ticker in {date.format('YYYY-MM-DD')} rm zip success""", "")

		# close and next data info prepare
		resp_cksum.close()
		resp.close()
		code_index += 1
		if code_index >= len(symbols):
			date = date.shift(days=1)
			code_index = 0

		self.log.info(f"""[{code}] ticker fetch finish""", "\n")

