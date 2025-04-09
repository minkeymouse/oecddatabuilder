import os
from pathlib import Path
import logging
import requests
import pandas as pd
from tqdm import tqdm
import time
from io import StringIO
from typing import List, Dict, Optional, Tuple, Any
import xml.etree.ElementTree as ET

# Set up logging configuration.
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class OECDAPI_Databuilder:
    def __init__(self, 
                 config: Dict[str, Dict[str, str]],
                 start: str,
                 end: str,
                 freq: str = "Q",
                 response_format: str = "csv",  # "csv", "json", or "xml"
                 dbpath: str = "../datasets/OECD/",
                 base_url: str = None,
                 request_interval: float = 5.0) -> None:
        """
        Initialize the OECDAPI_Databuilder instance.
        
        :param config: Configuration dictionary with indicator details.
        :param start: Start time period (e.g., "2019-Q1").
        :param end: End time period (e.g., "2020-Q3").
        :param freq: Data frequency: Quarterly ("Q"), Monthly ("M"), or Yearly ("Y").
        :param response_format: Expected response format ("csv", "json", "xml").
        :param dbpath: Path where CSV files will be stored.
        :param base_url: Base URL for the OECD API.
        :param request_interval: Number of seconds to wait between API requests.
        """
        self.config = config
        self.start = start
        self.end = end
        self.freq = freq.upper()
        self.response_format = response_format.lower()
        self.base_url = base_url
        self.indicators = list(self.config.keys())
        self.dbpath = Path(dbpath)
        self.request_interval = request_interval

        # Ensure the storage directory exists.
        self.dbpath.mkdir(parents=True, exist_ok=True)

        # Validate required parameters.
        if not self.start or not self.end:
            raise ValueError("Both start and end periods must be provided.")

        # Collect the union of all REF_AREA values from the configuration.
        countries_set = set()
        for conf in self.config.values():
            ref_area = conf.get("REF_AREA", "")
            if ref_area:
                countries_set.update(ref_area.split("+"))
        self.countries = sorted(list(countries_set))
        logger.info(f"Combined countries from configuration: {self.countries}")

        # Use a session to reuse connections.
        self.session = requests.Session()

    def _build_time_chunks(self, period_range: pd.PeriodIndex, chunk_size: int) -> List[Tuple[str, str]]:
        """
        Build a list of time chunk tuples (start, end) from a given period_range.
        """
        time_chunks = []
        for i in range(0, len(period_range), chunk_size):
            chunk = period_range[i:i + chunk_size]
            if self.freq == "Q":
                chunk_start = f"{chunk[0].year}-Q{chunk[0].quarter}"
                chunk_end = f"{chunk[-1].year}-Q{chunk[-1].quarter}"
            elif self.freq == "Y":
                chunk_start = str(chunk[0])
                chunk_end = str(chunk[-1])
            elif self.freq == "M":
                chunk_start = chunk[0].strftime("%Y-%m")
                chunk_end = chunk[-1].strftime("%Y-%m")
            else:
                raise ValueError(f"Unsupported frequency: {self.freq}")
            time_chunks.append((chunk_start, chunk_end))
        return time_chunks

    def _get_headers(self) -> Dict[str, str]:
        """
        Determine the correct Accept header for the API request.
        """
        headers = {}
        if self.response_format == "csv":
            headers["Accept"] = "application/vnd.sdmx.data+csv; charset=utf-8"
        elif self.response_format == "json":
            headers["Accept"] = "application/vnd.sdmx.data+json; charset=utf-8; version=2"
        elif self.response_format == "xml":
            headers["Accept"] = "application/vnd.sdmx.genericdata+xml; charset=utf-8; version=2.1"
        else:
            raise ValueError("response_format must be one of: csv, json, xml")
        return headers

    def _parse_response(self, resp_text: str) -> pd.DataFrame:
        """
        Parse the API response into a DataFrame based on the specified response_format.
        """
        if self.response_format == "csv":
            return pd.read_csv(StringIO(resp_text))
        elif self.response_format == "json":
            return pd.read_json(StringIO(resp_text))
        elif self.response_format == "xml":
            return pd.read_xml(StringIO(resp_text))
        else:
            raise ValueError("Unsupported response_format")

    def fetch_data(self, chunk_size: int = 100) -> "OECDAPI_Databuilder":
        """
        Fetch data from the OECD API in manageable chunks to avoid rate limits and
        very large queries. Saves the concatenated results as CSV files for each indicator.
        """
        period_range = pd.period_range(start=self.start, end=self.end, freq=self.freq)
        
        for indicator, conf in self.config.items():
            filter_order = list(conf.keys())
            all_chunks: List[pd.DataFrame] = []
            time_chunks = self._build_time_chunks(period_range, chunk_size)
            
            logger.info(f"For indicator '{indicator}', time chunks to process: {time_chunks}")
            
            filter_values = [conf.get(key, "") for key in filter_order]
            filter_url = ".".join(filter_values)
            full_url = f"{self.base_url}{filter_url}"
            logger.info(f"Fetching data for '{indicator}' using URL: {full_url}")
            
            headers = self._get_headers()

            for chunk_start, chunk_end in tqdm(time_chunks, desc=f"Downloading {indicator} Data"):
                query_url = (
                    f"{full_url}?startPeriod={chunk_start}&endPeriod={chunk_end}"
                    "&dimensionAtObservation=TIME_PERIOD"
                )
                try:
                    resp = self.session.get(query_url, headers=headers)
                    resp.raise_for_status()
                    
                    chunk_df = self._parse_response(resp.text)
                    
                    if {"REF_AREA", "TIME_PERIOD", "OBS_VALUE"}.issubset(chunk_df.columns):
                        chunk_df = chunk_df[["REF_AREA", "TIME_PERIOD", "OBS_VALUE"]]
                    else:
                        logger.warning(
                            f"Unexpected columns in the response for {query_url}: {chunk_df.columns.tolist()}"
                        )
                    
                    all_chunks.append(chunk_df)
                    logger.info(f"Chunk {chunk_start} to {chunk_end}: {chunk_df.shape[0]} rows")
                except Exception as e:
                    logger.error(f"Error for chunk {chunk_start} to {chunk_end} for indicator '{indicator}': {e}")
                time.sleep(self.request_interval)
            
            # Concatenate all fetched chunks.
            if all_chunks:
                indicator_df = pd.concat(all_chunks, ignore_index=True)
            else:
                indicator_df = pd.DataFrame(columns=["REF_AREA", "TIME_PERIOD", "OBS_VALUE"])
            
            csv_filename = self.dbpath / f"{indicator}.csv"
            indicator_df.to_csv(csv_filename, index=False)
            logger.info(f"Data for indicator '{indicator}' saved to {csv_filename}")
        return self

    def _convert_date(self, date_str: str) -> Any:
        """
        Convert a date string into a datetime using the appropriate frequency.
        Handles:
          - Quarterly dates in "YYYY-Qn" format.
          - Monthly dates in "YYYY-MM" format.
          - Yearly dates in "YYYY" format.
        """
        try:
            if self.freq == 'Q':
                return pd.Period(date_str, freq="Q").start_time
            elif self.freq == 'M':
                return pd.Period(date_str, freq="M").start_time
            elif self.freq == 'Y':
                # Using 'A' (annual) frequency for yearly dates.
                return pd.Period(date_str, freq="A").start_time
            else:
                logger.warning(f"Unsupported frequency '{self.freq}' for date conversion.")
                return date_str
        except Exception as e:
            logger.error(f"Error converting date string '{date_str}': {e}")
            return date_str

    def create_dataframe(self) -> pd.DataFrame:
        """
        Create a merged DataFrame combining the data for all indicators.
        Each CSV file is expected to contain 'REF_AREA', 'TIME_PERIOD', and 'OBS_VALUE' columns.
        The method:
          - Renames 'TIME_PERIOD' to 'date' and 'REF_AREA' to 'country'.
          - Merges all data on ['date', 'country'] using an outer join.
          - Converts 'date' using _convert_date based on frequency.
          - Ensures 'country' is a string.
          - Converts indicator columns to float.
        """
        if not self.indicators:
            raise ValueError("No indicators found. Please check your configuration.")
    
        merged_df: Optional[pd.DataFrame] = None
        for indicator in self.indicators:
            csv_file = self.dbpath / f"{indicator}.csv"
            try:
                df = pd.read_csv(csv_file)
            except Exception as e:
                logger.error(f"No file fetched for {csv_file}: {e}!")
                continue
            
            df = df.rename(columns={
                "TIME_PERIOD": "date",
                "REF_AREA": "country",
                "OBS_VALUE": indicator
            })
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.merge(merged_df, df, on=["date", "country"], how="outer")
    
        if merged_df is None:
            raise ValueError("No data was loaded from any CSV file.")
    
        # Convert date strings to datetime.
        merged_df["date"] = merged_df["date"].apply(self._convert_date)
    
        # Ensure the 'country' column is of string type.
        merged_df["country"] = merged_df["country"].astype(str)
    
        # Convert all indicator columns to float.
        for col in merged_df.columns:
            if col not in ["date", "country"]:
                merged_df[col] = pd.to_numeric(merged_df[col], errors="coerce")
    
        merged_df = merged_df.sort_values(by=["date", "country"]).reset_index(drop=True)
        return merged_df
