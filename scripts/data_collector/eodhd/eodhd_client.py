import json
import os
import sys
from functools import wraps
import requests
import pandas as pd
from typing import Optional, Callable, Dict, Any, List
from datetime import datetime, timedelta
import pickle
from pathlib import Path
from loguru import logger

EODHD_BASE_URL = "https://eodhd.com/api/"


from eodhd_cache import cache_data

class EodhdClient:

  DAY_INTERVAL = 'd'

  def __init__(self, api_key: str, region: str = "US", period: str = "d", cached_dir: Optional[str] = None) -> None:
    self.api_key = api_key
    self.region = region.upper()
    self.period = period.lower()
    self.cached_dir = cached_dir or os.path.join(os.path.expanduser("~"), ".cache", "qlib", "eodhd")
    os.makedirs(self.cached_dir, exist_ok=True)

  def get_data(self, endpoint, symbol: Optional[str]=None,
               start_date: Optional[str]=None,
               end_date: Optional[str]=None, **kwargs) -> Dict[str, Any]:

    url = f"{EODHD_BASE_URL}{endpoint}"
    if symbol:
      url += f"/{symbol.upper()}.{self.region}"
    print(url)
    logger.warning(f"getting data from {url}")
    request_data = {
      "api_token": self.api_key,
      "fmt": "json",
      "from": start_date,
      "to": end_date,
      **kwargs
    }

    res = requests.get(url=url, params=request_data)

    if res.status_code == 200:
      data = res.json()
      return data
    else:
      print(f"Failed to retrieve data: {res.status_code}")
      raise requests.exceptions.HTTPError(f"Failed to retrieve data: {res.status_code}")

  def get_instrument_list(self):
    cache_dir = os.path.join(self.cached_dir, f'exchange-symbol-list/{self.region}')
    with cache_data(cache_dir, timedelta(days=10)) as cached_result:
      cached_value = cached_result.get_cache()
      if cached_value is not None:
          return cached_value

      # Call the original function and cache the result
      data = self.get_data(f"exchange-symbol-list/{self.region}")
      df = pd.DataFrame(data)
      cached_result.set_result(df)
      return df


  def eod(self, symbol: str, start_date: str, end_date: str, adj_ohlc: bool = True) -> pd.DataFrame:
    data = self.get_data("eod", symbol, start_date, end_date)
    df = pd.DataFrame()
    if len(data) > 0:
      df = pd.DataFrame(data)
      df.set_index('date', inplace=True)
      df = df.dropna(how='all')
    if adj_ohlc and 'adjusted_close' in df.columns:
      df = self._adjust_ohlc(df)
    return df


  def _adjust_ohlc(self, df):
    adjust = df["close"] / df["adjusted_close"]
    for col in ["open", "high", "low"]:
        df[col] = df[col] / adjust
    del df["close"]
    df.rename(columns={"adjusted_close": "close"}, inplace=True)
    return df

  def splits(self, symbol) -> pd.DataFrame:
    cache_dir = os.path.join(self.cached_dir, f'splits/{symbol}')
    with cache_data(cache_dir, timedelta(days=1)) as cached_result:
      cached_value = cached_result.get_cache()
      if cached_value is not None:
          return cached_value

      # Call the original function and cache the result
      data = self.get_data("splits", symbol)
      df = pd.DataFrame()
      if len(data) > 0:
        df = pd.DataFrame(data)
        df = df.set_index('date').dropna(how='all')
        df['split'] = df['split'].apply(lambda x: round(float(x.split('/')[0]) / float(x.split('/')[1]), 2))
        cached_result.set_result(df)
      return df


  def dividends(self, symbol) -> pd.DataFrame:
    cache_dir = os.path.join(self.cached_dir, f'dividends/{symbol}')
    with cache_data(cache_dir, timedelta(days=1)) as cached_result:
      cached_value = cached_result.get_cache()
      if cached_value is not None:
          return cached_value

      data = self.get_data("dividends", symbol)
      df = pd.DataFrame(data)
      cached_result.set_result(df)
      return df

  def history(self, symbol: str, start_date: str, end_date: str, interval: str = 'd'):
    if interval == self.DAY_INTERVAL:
      df = self.eod(symbol, start_date=start_date, end_date=end_date)
    split_data = self.splits(symbol)
    if split_data.empty:
      return df
    merged_df = pd.merge(df, split_data, on='date', how='left', validate='one_to_one')
    merged_df['split'] = merged_df['split'].fillna(0.0)
    return merged_df

if __name__ == "__main__":
    eodhd_client = EodhdClient(
      api_key="6716c16c496391.45280242",
      region="US",
      period="d"
    )
    data = eodhd_client.splits('AAPL')
    print(data)
