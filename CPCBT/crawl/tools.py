import sys
from ..tools.tool import dict_deep_update_nobreak


def config_tool(self, params):
	default = {
		"log": {
			"steams": [sys.stdout],
			"msgFormat": "{time} [{level}] {msg0}{msg1}",
		},
		"save_path": "./data/",
		"start_date": "2015-01-01",             # set start date
		"end_date": None,                       # set end date, if None, it means today
		"quote_lst": ['USDT'],                  # set the quote you want, default is USDT.
		"timeframe": "1m",                      # set timeframe, default is 1m
		"step": 60 * 24,                        # set fetch one day data by once (unit: hour)
		"exchanges": ["binance", "okex"],       # set exchange to fetch data, the first one is default exchange,
												# if not find data in the exchanges is used now, it will use next.
		"retry": 10,
		"ccxt": {
			"countries": ['US', 'CN', 'EU'],    # set country default as US
			"rateLimit": 1000,                  # set rate limit in milliseconds
			"timeout": 1000 * 10,               # set timeout in milliseconds

			# set proxies
			"proxies": {
				"http": "http://127.0.0.1:7890",
				"https": "https://127.0.0.1:7890",
			}
		}
	}

	return dict_deep_update_nobreak(default, params)


def step_tool(self, step):
	timeframes = {
		'1s': 1,
		'1m': 60,
		'3m': 3 * 60,
		'5m': 5 * 60,
		'15m': 15 * 60,
		'30m': 30 * 60,
		'1h': 60 * 60,
		'2h': 2 * 60 * 60,
		'4h': 4 * 60 * 60,
		'6h': 6 * 60 * 60,
		'8h': 8 * 60 * 60,
		'12h': 12 * 60 * 60,
		'1d': 24 * 60 * 60,
		'3d': 3 * 24 * 60 * 60,
		'1w': 7 * 24 * 60 * 60,
	}
	return timeframes[step]
