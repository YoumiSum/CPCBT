import sys
import warnings
from enum import Enum

import arrow

from .tool import dict_deep_update_nobreak


class LOGLEVEL(Enum):
	DEBUG = 10
	INFO = 20
	WARNING = 30
	ERROR = 40
	CRITICAL = 50


class LOG(object):

	def __init__(self, config: dict):
		self.config: dict = self.config_tool(config)

	def __del__(self):
		for steam in self.config["steams"]:
			if steam not in [sys.stdout, sys.stderr]:
				steam.close()

	def __repr__(self):
		return str(self.config)

	def config_tool(self, params):
		default = {
			# 关于msgFormat格式详解
			# 如果你想使用多个msg，需要用int下标标识，数字越小，则表示在你传入的args中越靠前
			# 例如："msgFormat": "{time} [{level}] {msg0} {msg1} {msg0}"
			# 调用log.info("hello", "world")
			# 于是输出如下：
			#       {time} [{level}] hello world hello
			# Note: 特别注意：msgx，x的下标必须从0开始，0表示你传入的第一个msg，1表示你传入的第二个msg
			# Note: time 时间输出的位置，注意：需要在time中配置on: True
			# Note: level 为日志级别输出的位置
			"msgFormat": "{time} [{level}] {msg0}",

			# 时间配置
			"time": {
				# 是否开启当前时间的输出
				"on": True,

				# 时间格式，关于时间的格式参考arrow库，该格式直接继承于arrow.format
				"format": "YYYY-MM-DD HH:mm:ss"
			},

			# 日志级别配置，注意：只能输出比level高的日志
			"level": LOGLEVEL.DEBUG,

			# 输出流，默认为标准输出流
			"steams": [sys.stdout]
		}

		return dict_deep_update_nobreak(default, params)

	def __generator_msg(self, log_level, *args):
		""" generator_msg """
		msg = str(self.config['msgFormat'])

		# set time
		if self.config['time'].get("on"):
			time_str = arrow.now().format(self.config["time"].get('format'))
			msg = msg.replace("{time}", time_str)

		# set log level
		msg = msg.replace("{level}", log_level.name)

		# set msg
		for index, item in enumerate(args):
			msg = msg.replace("{msg%d}" % index, item)

		return msg + "\n"

	def __push_msg(self, log_level, *args):
		msg = self.__generator_msg(log_level, *args)
		for steam in self.config["steams"]:
			steam.writelines(msg)
			steam.flush()

	def debug(self, *args):
		if not LOGLEVEL.DEBUG.value >= self.config["level"].value:
			warnings.warn(f"log level no compile [{args}]", RuntimeWarning, stacklevel=2)
			return -1

		self.__push_msg(LOGLEVEL.DEBUG, *args)
		return 0

	def info(self, *args):
		if not LOGLEVEL.INFO.value >= self.config["level"].value:
			warnings.warn(f"log level no compile [{args}]", RuntimeWarning, stacklevel=2)
			return -1

		self.__push_msg(LOGLEVEL.INFO, *args)
		return 0

	def warning(self, *args):
		if not LOGLEVEL.WARNING.value >= self.config["level"].value:
			warnings.warn(f"log level no compile [{args}]", RuntimeWarning, stacklevel=2)
			return -1

		self.__push_msg(LOGLEVEL.WARNING, *args)
		return 0

	def error(self, *args):
		if not LOGLEVEL.ERROR.value >= self.config["level"].value:
			warnings.warn(f"log level no compile [{args}]", RuntimeWarning, stacklevel=2)
			return -1

		self.__push_msg(LOGLEVEL.ERROR, *args)
		return 0

	def critical(self, *args):
		if not LOGLEVEL.CRITICAL.value >= self.config["level"].value:
			warnings.warn(f"log level no compile [{args}]", RuntimeWarning, stacklevel=2)
			return -1

		self.__push_msg(LOGLEVEL.CRITICAL, *args)
		return 0
