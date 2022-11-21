from arrow import arrow

from CPCBT.tools import LOG


def __init__(self, config):
		config = self.config_tool(config)
		# 日志配置
		self.log = LOG(config["log"])
		self.save_path = config["save_path"]
		self.base_time = config["start_date"] if config["start_date"] is not None else 0
		self.end_time = config["end_date"] if config["end_date"] is not None else arrow.now().format('YYYY-MM-DD')
		self.quote_lst = config["quote_lst"]
		self.ccxt_config = config["ccxt"]
		self.timeframe = config["timeframe"]
		self.exchanges = config["exchanges"]
		self.step = config["step"]

		config["log"]["steams"] = [steam.name for steam in config["log"].get("steams")]
		self.conf = config


def __repr__(self):
	return str(self.conf)
