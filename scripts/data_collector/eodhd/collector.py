# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import abc
import sys
import copy
import os
import time
import datetime
import importlib
from abc import ABC
import multiprocessing
from pathlib import Path
from typing import Iterable, Union
import re

import fire
import requests
import numpy as np
import pandas as pd
from loguru import logger
from dateutil.tz import tzlocal

import qlib
from qlib.data import D
from qlib.tests.data import GetData
from qlib.utils import code_to_fname, fname_to_code, exists_qlib_data
from qlib.constant import REG_CN as REGION_CN

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.eodhd.eodhd_client import EodhdClient

from dump_bin import DumpDataUpdate
from data_collector.base import BaseCollector, BaseNormalize, BaseRun, Normalize
from data_collector.utils import (
    deco_retry,
    get_calendar_list,
    get_hs_stock_symbols,
    get_us_stock_symbols,
    get_in_stock_symbols,
    get_br_stock_symbols,
    generate_minutes_calendar_from_daily,
    calc_adjusted_price,
)

api_key = os.environ.get("EODH_API_KEY")

class EodhdCollector(BaseCollector):
    retry = 5  # Configuration attribute.  How many times will it try to re-request the data if the network fails.

    def __init__(
        self,
        save_dir: Union[str, Path],
        start=None,
        end=None,
        interval="1d",
        max_workers=4,
        max_collector_count=2,
        delay=0,
        check_data_length: int = None,
        limit_nums: int = None,
    ):
        """

        Parameters
        ----------
        save_dir: str
            stock save dir
        max_workers: int
            workers, default 4
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1min
        start: str
            start datetime, default None
        end: str
            end datetime, default None
        check_data_length: int
            check data length, by default None
        limit_nums: int
            using for debug, by default None
        """
        period = interval
        if interval == self.INTERVAL_1d:
            period = "d"
        self.eodhd_client = EodhdClient(api_key, region="US", period=period)
        super(EodhdCollector, self).__init__(
            save_dir=save_dir,
            start=start,
            end=end,
            interval=interval,
            max_workers=max_workers,
            max_collector_count=max_collector_count,
            delay=delay,
            check_data_length=check_data_length,
            limit_nums=limit_nums,
        )
        self.init_datetime()

    def init_datetime(self):
        self.start_datetime = self.convert_datetime(self.start_datetime, self._timezone)
        self.end_datetime = self.convert_datetime(self.end_datetime, self._timezone)

    @staticmethod
    def convert_datetime(dt: Union[pd.Timestamp, datetime.date, str], timezone):
        try:
            dt = pd.Timestamp(dt, tz=timezone).timestamp()
            dt = pd.Timestamp(dt, tz=tzlocal(), unit="s")
        except ValueError as e:
            pass
        return dt

    def get_instrument_list(self):
        return ['AAPL']
        instrument_list = self.eodhd_client.get_instrument_list()
        common_stocks = instrument_list[instrument_list['Type'] == 'Common Stock']
        return sorted(common_stocks['Code'].tolist())


    @property
    @abc.abstractmethod
    def _timezone(self):
        raise NotImplementedError("rewrite get_timezone")

    @staticmethod
    def standardize_interval(interval):
        pattern = r'^(\d)(min?|hour?)s?$'
        matches = re.match(pattern, interval)
        if re.match(pattern, interval):
            if matches[2] == 'min':
                unit = 'm'
            elif matches[2] == 'hour':
                unit = 'h'
            else:
                unit = 'd'
            return f'{matches[1]}{unit}'
        return 'd'


    @staticmethod
    def get_data_from_remote(symbol, interval, start, end, show_1min_logging: bool = False):
        error_msg = f"{symbol}-{interval}-{start}-{end}"

        def _show_logging_func():
            if interval == BaseCollector.INTERVAL_1min and show_1min_logging:
                logger.warning(f"{error_msg}:{_resp}")

        interval = EodhdCollector.standardize_interval(interval)

        try:
            eodhd_client = EodhdClient(api_key, region="US", period=interval)
            _resp = eodhd_client.history(symbol, interval=interval, start_date=start, end_date=end)
            if isinstance(_resp, pd.DataFrame):
                return _resp.reset_index()
            elif isinstance(_resp, dict):
                _temp_data = _resp.get(symbol, {})
                if isinstance(_temp_data, str) or (
                    isinstance(_resp, dict) and _temp_data.get("indicators", {}).get("quote", None) is None
                ):
                    _show_logging_func()
            else:
                _show_logging_func()
        except Exception as e:
            logger.warning(
                f"get data error: {symbol}--{start}--{end}"
                + "Your data request fails. This may be caused by your firewall (e.g. GFW). Please switch your network if you want to access Yahoo! data"
            )

    def get_data(
        self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> pd.DataFrame:
        @deco_retry(retry_sleep=self.delay, retry=self.retry)
        def _get_simple(start_, end_):
            self.sleep()
            _remote_interval = "1m" if interval == self.INTERVAL_1min else interval
            resp = self.get_data_from_remote(
                symbol,
                interval=_remote_interval,
                start=start_,
                end=end_,
            )
            if resp is None or resp.empty:
                raise ValueError(
                    f"get data error: {symbol}--{start_}--{end_}" + "The stock may be delisted, please check"
                )
            return resp

        _result = None
        if interval == self.INTERVAL_1d:
            try:
                _result = _get_simple(start_datetime, end_datetime)
            except ValueError as e:
                pass
        elif interval == self.INTERVAL_1min:
            _res = []
            _start = self.start_datetime
            while _start < self.end_datetime:
                _tmp_end = min(_start + pd.Timedelta(days=7), self.end_datetime)
                try:
                    _resp = _get_simple(_start, _tmp_end)
                    _res.append(_resp)
                except ValueError as e:
                    pass
                _start = _tmp_end
            if _res:
                _result = pd.concat(_res, sort=False).sort_values(["symbol", "date"])
        else:
            raise ValueError(f"cannot support {self.interval}")
        return pd.DataFrame() if _result is None else _result

    def collector_data(self):
        """collector data"""
        super(EodhdCollector, self).collector_data()
        self.download_index_data()

    @abc.abstractmethod
    def download_index_data(self):
        """download index data"""
        raise NotImplementedError("rewrite download_index_data")


class EodhdCollectorUS1d(EodhdCollector):

    def download_index_data(self):
        print("calling download index data")

    def normalize_symbol(self, symbol):
        return code_to_fname(symbol).upper()
    @property
    def _timezone(self):
        return "America/New_York"

class Run(BaseRun):
    def __init__(self, source_dir=None, normalize_dir=None, max_workers=1, interval="1d", region=REGION_CN):
        """

        Parameters
        ----------
        source_dir: str
            The directory where the raw data collected from the Internet is saved, default "Path(__file__).parent/source"
        normalize_dir: str
            Directory for normalize data, default "Path(__file__).parent/normalize"
        max_workers: int
            Concurrent number, default is 1; when collecting data, it is recommended that max_workers be set to 1
        interval: str
            freq, value from [1min, 1d], default 1d
        region: str
            region, value from ["CN", "US", "BR"], default "CN"
        """
        super().__init__(source_dir, normalize_dir, max_workers, interval)
        self.region = region

    @property
    def collector_class_name(self):
        return f"EodhdCollector{self.region.upper()}{self.interval}"

    @property
    def normalize_class_name(self):
        return f"EodhdNormalize{self.region.upper()}{self.interval}"
    @property
    def default_base_dir(self) -> Union[Path, str]:
        return CUR_DIR

    def download_data(
        self,
        max_collector_count=2,
        delay=0.5,
        start=None,
        end=None,
        check_data_length=None,
        limit_nums=None,
    ):
        """download data from Internet

        Parameters
        ----------
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0.5
        start: str
            start datetime, default "2000-01-01"; closed interval(including start)
        end: str
            end datetime, default ``pd.Timestamp(datetime.datetime.now() + pd.Timedelta(days=1))``; open interval(excluding end)
        check_data_length: int
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None

        Notes
        -----
            check_data_length, example:
                daily, one year: 252 // 4
                us 1min, a week: 6.5 * 60 * 5
                cn 1min, a week: 4 * 60 * 5

        Examples
        ---------
            # get daily data
            $ python collector.py download_data --source_dir ~/.qlib/stock_data/source --region CN --start 2020-11-01 --end 2020-11-10 --delay 0.1 --interval 1d
            # get 1m data
            $ python collector.py download_data --source_dir ~/.qlib/stock_data/source --region CN --start 2020-11-01 --end 2020-11-10 --delay 0.1 --interval 1m
        """
        if self.interval == "1d" and pd.Timestamp(end) > pd.Timestamp(datetime.datetime.now().strftime("%Y-%m-%d")):
            raise ValueError(f"end_date: {end} is greater than the current date.")

        super(Run, self).download_data(max_collector_count, delay, start, end, check_data_length, limit_nums)

if __name__ == "__main__":
    fire.Fire(Run)
