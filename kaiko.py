import argparse
import gzip
import json
import os
import re
import sys
import tomllib
import traceback
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import httpx2

# TODO bulk exchange downloads, cache for reference data api calls, change DEBUG flag to verbosity setting, add file like --single-output that produces a single file as output with add'l cols from filenames, move inst_list aggregation out of memory, parallelize downloads/async redesign, write data to a .part file, integrate fetch_ref into client + combine with fetch_data?
# note for async: "Our standard Rest API is limited to 6000 requests per API key per minute. If you query the API beyond that threshold, a 429 error message will occur."

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "t", "1")
HTTPX2_NUM_RETRIES = 5
HTTPX2_TIMEOUT_S = 30.0

@dataclass
class PathParams:

    # api-wide path params
    api_version: str = "v3"

    # per-endpoint path params
    data_type: str = "trades"
    data_version: str = "v1"
    summary_type: str = "trades"

    # per-request path params
    region: str = "us"
    exchange: str | list[str] = "cbse"
    instrument_class: str | list[str] = "spot"
    instrument: str | list[str] = "btc-usd"

@dataclass
class QueryParams:
    start_time: str
    end_time: str

    # aggregations only
    interval: str | None = None

    # defaults
    page_size: str = "10000"
    sort: str = "desc"

@dataclass
class RefPathParams:
    api_version: str = "v1"
    endpoint: str = "instruments" # assets exchanges instruments

@dataclass
class RefQueryParams:
    exchange_code: str | None = None
    base_asset: str | None = None
    quote_asset: str | None = None
    class_: str | None = None # spot perpetual-future future future_combo option option_combo
    code: str | None = None
    base_asset_class: str | None = None
    quote_asset_class: str | None = None
    trade_start_timestamp: str | None = None # ISO-8601
    trade_end_timestamp: str | None = None # ISO-8601 or "ongoing"
    trade_count_min: str | None = None
    trade_count_max: str | None = None
    limit: str | None = None
    orderBy: str | None = None
    order: str | None = None

class KaikoClient:
    def __init__(self, api_key: str, client = None):
        self.api_key = api_key
        self.client = client or httpx2.Client(timeout=HTTPX2_TIMEOUT_S,transport=httpx2.HTTPTransport(retries=HTTPX2_NUM_RETRIES))
        self.headers = {"Accept": "application/json"}

    def close(self):
        self.client.close()

    def fetch_data(
        self,
        path_params: PathParams,
        query_params: QueryParams
    ) -> Iterator[dict[str, Any]] | None:
        url = "https://{region}.market-api.kaiko.io/{api_version}/data/{data_type}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}/{summary_type}".format(**asdict(path_params))
        headers = self.headers | {"X-Api-Key": self.api_key}

        qp_dict = {k: v for k, v in asdict(query_params).items() if v is not None}
        while url:
            if qp_dict:
                r = self.client.get(url, headers=headers, params=qp_dict)
            else:
                r = self.client.get(url, headers=headers)
            _ = r.raise_for_status()
            if DEBUG:
                print(unquote(str(r.url)),file=sys.stdout)
                print(r.text,file=sys.stderr)

            if not (data := r.json().get("data")):
                return

            yield from data
            url = r.json().get("next_url")
            qp_dict = None

    def download(
            self,
            report_type: str,
            path_params: PathParams,
            query_params: QueryParams,
            download_dir: str = f"{Path(__file__).parent}",
            file_format: str = "jsonl",
            write_gzip: bool = True
        ) -> Path | None:

        filename_root = self.resolve_filename(report_type, path_params, query_params)
        if not (data_iter := self.fetch_data(path_params, query_params)):
            return None

        try:
            first = next(data_iter)
        except StopIteration as err:
            if DEBUG:
                print(f"API likely returned no data for {filename_root}, {err}")
            return None

        if file_format == "csv" and write_gzip:
            file_path = Path(f"{download_dir}/{filename_root}.csv.gz")
            with gzip.open(file_path, 'wt', newline='', encoding="utf-8") as f:
                _ = f.write(f'{",".join([f'"{k}"' for k in first])}\n')
                _ = f.write(f'{",".join([f'"{v}"' for v in first.values()])}\n')
                f.writelines(f'{",".join([f'"{v}"' for v in record.values()])}\n' for record in data_iter)
        elif file_format == "csv":
            file_path = Path(f"{download_dir}/{filename_root}.csv")
            with open(file_path, 'w', newline='', encoding="utf-8") as f:
                _ = f.write(f'{",".join([f'"{k}"' for k in first])}\n')
                _ = f.write(f'{",".join([f'"{v}"' for v in first.values()])}\n')
                f.writelines(f'{",".join([f'"{v}"' for v in record.values()])}\n' for record in data_iter)
        elif file_format in ["json","jsonl"] and write_gzip:
            file_path = Path(f"{download_dir}/{filename_root}.jsonl.gz")
            with gzip.open(file_path, 'wt', newline='', encoding="utf-8") as f:
                _ = f.write(f"{first}\n")
                f.writelines(f"{record}\n" for record in data_iter)
        elif file_format in ["json","jsonl"]:
            file_path = Path(f"{download_dir}/{filename_root}.jsonl")
            with open(file_path, 'w', newline='', encoding="utf-8") as f:
                _ = f.write(f"{first}\n")
                f.writelines(f"{record}\n" for record in data_iter)
        else:
            return None

        return file_path.resolve()

    def resolve_filename(
        self,
        report_type: str,
        path_params: PathParams,
        query_params: QueryParams
    ) -> str:

        # clean ISO-8601 strings for filename
        start_time_utc = (
            datetime.fromisoformat(query_params.start_time)
            .astimezone(ZoneInfo("UTC"))
            .isoformat(timespec="microseconds")
            .replace('+00:00','').replace('T','').replace('.','')
        )
        end_time_utc = (
            datetime.fromisoformat(query_params.end_time)
            .astimezone(ZoneInfo("UTC"))
            .isoformat(timespec="microseconds")
            .replace('+00:00','').replace('T','').replace('.','')
        )

        if report_type in ["agg"]:
            filename_root = "{aggregation_type}_{interval}_{exchange}_{instrument_class}_{instrument}_{start_time}_{end_time}".format(
                aggregation_type=path_params.summary_type.split('/')[1],
                interval=query_params.interval,
                start_time=start_time_utc,
                end_time=end_time_utc,
                **asdict(path_params)
            )
        else:
            filename_root = "{summary_type}_{exchange}_{instrument_class}_{instrument}_{start_time}_{end_time}".format(
                start_time=start_time_utc,
                end_time=end_time_utc,
                **asdict(path_params)
            )
        filename_root = re.sub(r'[:-]', '', filename_root).lower()

        return filename_root

def fetch_ref(
    path_params: RefPathParams,
    query_params: RefQueryParams,
    client: httpx2.Client | None = None
) -> Iterator[dict[str, Any]] | None:
    client = client or httpx2.Client(timeout=HTTPX2_TIMEOUT_S,transport=httpx2.HTTPTransport(retries=HTTPX2_NUM_RETRIES))
    headers = {"Accept": "application/json"}
    url = "https://reference-data-api.kaiko.io/{api_version}/{endpoint}".format(**asdict(path_params))

    qp_dict = {
        (k[:-1] if k == "class_" else k): v
        for k,v in asdict(query_params).items()
        if v is not None
    }
    while url:
        if qp_dict:
            r = client.get(url, headers=headers, params=qp_dict)
        else:
            r = client.get(url, headers=headers)

        _ = r.raise_for_status()
        if DEBUG:
            print(unquote(str(r.url)),file=sys.stdout)
            print(r.text,file=sys.stderr)

        if not (data := r.json().get("data")):
            return

        yield from data
        url = r.json().get("next_url")
        qp_dict = None

def generate_config_default(
    config_path: str = f"{Path(__file__).parent}/kaiko.toml",
    aggregation_type: str = "ohlcv"
) -> None:

    # get reasonable dynamic default start and end time, using the Tuesday before Thanksgiving of the current or previous year
    et_tz = ZoneInfo("America/New_York")
    today = datetime.now(tz=et_tz).date()
    if today < date(today.year, 11, 28):
        year_default = today.year - 1
    else:
        year_default = today.year

    offset_to_first_thurs = (3 - date(year_default, 11, 1).weekday()) % 7
    tues_before_thanksgiving = date(year_default, 11, 22) + timedelta(days=offset_to_first_thurs) - timedelta(days=2)
    start_time_default = datetime.combine(tues_before_thanksgiving, time(10, 30, 0), tzinfo=et_tz).isoformat(timespec="microseconds")
    end_time_default = datetime.combine(tues_before_thanksgiving, time(11, 0, 0), tzinfo=et_tz).isoformat(timespec="microseconds")

    # write file with default params
    default_path_params = asdict(PathParams(
        exchange=["cbse","bnus"],
        instrument=["btc-usd","eth-usd"]
    ))
    default_query_params = asdict(QueryParams(
        start_time=start_time_default,
        end_time=end_time_default
    ))

    default_path_params["summary_type"] = "trades"
    default_path_params["api_version"] = "v3"
    trades_toml = "\n".join([
        "[trades]",
        *[f"{k} = {v!r}" for k,v in default_path_params.items() if k not in ["interval"]],
        *[f"{k} = {v!r}" for k,v in default_query_params.items() if k not in ["interval"]]
    ])

    default_path_params["summary_type"] = f"aggregations/{aggregation_type}"
    default_path_params["api_version"] = "v2"
    agg_toml = "\n".join([
        "[agg]",
        *[f"{k} = {v!r}" for k,v in default_path_params.items()],
        *[f"{k} = {v!r}" for k,v in default_query_params.items()]
    ])

    with open(config_path, "x") as f:
        f.write(trades_toml)
        f.write("\n\n")
        f.write(agg_toml)

def load_config(
        report_type,
        config_path: str = f"{Path(__file__).parent}/kaiko.toml",
        start_time_override=None,
        end_time_override=None,
        exchange_override=None,
        instrument_class_override=None,
        instrument_override=None,
        aggregation_type_override=None,
        interval_override=None,
    ) -> Iterable[tuple[PathParams,QueryParams]]:

    # read .toml or .json config file
    ext = Path(config_path).suffix.lower()
    if ext == ".toml":
        with open(config_path, "rb") as f:
            conf_dict = tomllib.load(f)[report_type]
    elif ext == ".json":
        with open(config_path, "r") as f:
            conf_dict = json.load(f)[report_type]
    else:
        raise ValueError("incorrect file extension for config file")

    # CLI overrides
    if start_time_override:
        conf_dict["start_time"] = datetime.fromisoformat(start_time_override).isoformat(timespec="microseconds")
    if end_time_override:
        conf_dict["end_time"] = datetime.fromisoformat(end_time_override).isoformat(timespec="microseconds")
    if exchange_override:
        conf_dict["exchange"] = exchange_override
    if instrument_class_override:
        conf_dict["instrument_class"] = instrument_class_override
    if instrument_override:
        conf_dict["instrument"] = instrument_override
    if report_type == "agg" and aggregation_type_override:
        conf_dict["summary_type"] = f"aggregations/{aggregation_type_override}"
    if report_type == "agg" and interval_override:
        conf_dict["interval"] = interval_override

    # config list may only contain a single config
    conf_list = [conf_dict]

    # split out exchanges, if provided as a list; supports multiple exchanges per config file
    if isinstance(conf_dict.get("exchange"), list):
        conf_list = [
          {**d, "exchange": e}
          for d in conf_list
          for e in d["exchange"]
        ]

    # split out instruments, if provided as a list; supports multiple instruments per config file
    if isinstance(conf_dict.get("instrument_class"), list) and isinstance(conf_dict.get("instrument"), list):
        if len(conf_dict.get("instrument_class")) != len(conf_dict.get("instrument")):
            raise ValueError("instrument and instrument_class config lists (which are aligned) have a length mismatch")
        conf_list = [
            {**{k: v for k, v in d.items() if k not in ("instrument_class", "instrument")}, "instrument_class": ic, "instrument": i}
            for d in conf_list
            for ic, i in zip(d["instrument_class"], d["instrument"])
        ]
    elif isinstance(conf_dict.get("instrument"), list): # broadcast the single instrument_class value
        conf_list = [
            {**d, "instrument": i}
            for d in conf_list
            for i in d["instrument"]
        ]

    # instrument, date range resolution
    resolved_conf_list = []

    # for each request config, split instrument to base/quote assets and look up all instruments by instrument class
    # each returned instrument becomes its own config; perhaps extra work for class "spot" but necessary for derivatives
    ref_path_params = RefPathParams(endpoint="instruments")
    for c in conf_list:
        base_asset, quote_asset = c["instrument"].split("-")
        ref_query_params = RefQueryParams(
            exchange_code = c["exchange"],
            base_asset = base_asset,
            quote_asset = quote_asset,
            class_ = c["instrument_class"],
            trade_start_timestamp = c["start_time"],
            trade_end_timestamp = c["end_time"],
        )
        if not (inst_iter := fetch_ref(ref_path_params, ref_query_params)):
            print(f"no {c["instrument_class"]} instruments found for {c["instrument"]} on {c["exchange"]}", file=sys.stdout)
            continue

        for inst in inst_iter:
            # reset query start_time/end_time per instrument
            query_start_time = datetime.fromisoformat(c["start_time"]).astimezone(ZoneInfo("UTC"))
            query_end_time = datetime.fromisoformat(c["end_time"]).astimezone(ZoneInfo("UTC"))

            # check that query range is within trading timeframe and tighten query start_time/end_time to match, if needed
            if not inst["trade_start_time"]:
                if DEBUG:
                    print(f"no {inst["code"]} ({inst["class"]}) trades within query range on {inst["exchange_code"]}",file=sys.stdout)
                continue
            trade_start_time = datetime.fromisoformat(inst["trade_start_time"]).astimezone(ZoneInfo("UTC"))
            if query_end_time < trade_start_time:
                if DEBUG:
                    print(f"no {inst["code"]} ({inst["class"]}) trades within query range on {inst["exchange_code"]}",file=sys.stdout)
                continue

            if inst["trade_end_time"]:
                trade_end_time = datetime.fromisoformat(inst["trade_end_time"]).astimezone(ZoneInfo("UTC"))
                if query_start_time > trade_end_time:
                    if DEBUG:
                        print(f"no {inst["code"]} ({inst["class"]}) trades within query range on {inst["exchange_code"]}",file=sys.stdout)
                    continue
                query_end_time = min(query_end_time, trade_end_time)
            query_start_time = max(query_start_time, trade_start_time)

            # limiter for download restart - remove after
            limit = "2025-04-01T00:00:00.000000+00:00"
            if c["instrument_class"] == "option_combo" and query_start_time < datetime.fromisoformat(limit).astimezone(ZoneInfo("UTC")):
                continue

            resolved_conf = c.copy() | {
                "instrument": inst["code"],
                "start_time": query_start_time.isoformat(timespec="milliseconds"),
                "end_time": query_end_time.isoformat(timespec="milliseconds"),
            }
            _ = resolved_conf_list.append(resolved_conf)

    # dataclass fields for filtering
    path_fields = [f.name for f in fields(PathParams)]
    query_fields = [f.name for f in fields(QueryParams)]

    # convert resolved configs to request parameters
    request_list = [
        (
            PathParams(**{k:v for k,v in d.items() if k in path_fields}),
            QueryParams(**{k:v for k,v in d.items() if k in query_fields})
        )
        for d in resolved_conf_list
    ]

    return request_list

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        description="CLI function to run.",
        dest="action",
        required=True,
        help="",
        )

    # config
    parser_conf = subparsers.add_parser(
        "genconf",
        description="Generate an editable TOML config file with reasonable defaults.",
        )
    _ = parser_conf.add_argument(
        "--config-dir",
        default=f"{Path(__file__).parent}",
        dest="config_dir",
        help="Directory for default config file."
        )
    _ = parser_conf.add_argument(
        *["--agg","--agg-type","--aggregation-type"],
        choices=["count_ohlcv_vwap", "ohlcv", "vwap"],
        default="ohlcv",
        dest="aggregation_type",
        help="Aggregation type; defaults to ohlcv."
    )

    # dl (download)
    parser_download = subparsers.add_parser(
        "dl",
        description="Download data.",
        )
    _ = parser_download.add_argument(
        "report_type",
        choices=["trades", "agg"],
        type=str,
        help="The type of data to download."
    )

    _ = parser_download.add_argument(  # TODO support path to .env file
        *["-e","--env","--env-var"],
        dest="env_var",
        default="KAIKO_API_KEY",
        help="Environment variable holding Kaiko API key. Defaults to KAIKO_API_KEY."
    )
    _ = parser_download.add_argument(
        *["-a","--key","--api-key"],
        dest="api_key",
        help="API key value"
    )

    _ = parser_download.add_argument(
        *["--conf","--config-path"],
        dest="config_path",
        default=f"{Path(__file__).parent}/kaiko.toml",
        help="Path to TOML or JSON config file."
    )
    _ = parser_download.add_argument(
        *["-d","--download-dir"],
        dest="download_dir",
        default=f"{Path(__file__).parent}/tmp",
        help="Directory for downloaded files."
    )
    _ = parser_download.add_argument(
        *["-f","--format","--file-format"],
        dest="file_format",
        choices=["csv", "json"],
        default="csv",
        help="Download format."
    )
    _ = parser_download.add_argument(
        *["--gzip","--write-gzip"],
        dest="write_gzip",
        action="store_true",
        help="Download gzip-encoded file."
    )

    # dl/download optional overrides
    _ = parser_download.add_argument(
        *["--agg","--agg-type","--aggregation-type"],
        choices=["count_ohlcv_vwap", "ohlcv", "vwap"],
        dest="aggregation_type",
        help="Aggregation type."
    )
    _ = parser_download.add_argument(
        *["--exch","--exchange"],
        dest="exchange",
        help="Exchange code config override."
    )
    _ = parser_download.add_argument(
        *["--class","--instrument-class"],
        dest="instrument_class",
        help="Instrument class config override."
    )
    _ = parser_download.add_argument(
        *["--inst","--instrument"],
        dest="instrument",
        help="Instrument code config override."
    )
    _ = parser_download.add_argument(
        *["--start","--start-time"],
        dest="start_time",
        help="Start time config override."
    )
    _ = parser_download.add_argument(
        *["--end","--end-time"],
        dest="end_time",
        help="End time config override."
    )
    _ = parser_download.add_argument(
        *["--interval"],
        dest="interval",
        help="Summary interval config override."
    )

    args = parser.parse_args()
    try:
        if args.action == "genconf":
            generate_config_default(config_path=f"{args.config_dir}/kaiko.toml", aggregation_type=args.aggregation_type)
        elif args.action == "dl":
            # load api key
            if args.api_key:
                api_key = args.api_key
            elif (api_key := os.environ.get(args.env_var)):
                pass
            else:
                raise ValueError("no API key found")

            # temp directory for downloading files
            Path(args.download_dir).mkdir(parents=True, exist_ok=True)

            # load config and CLI overrides
            if not Path(args.config_path).exists() and not (args.start_time and args.end_time):
                raise ValueError("not enough info provided - generate a config file or provide start and end times")
            elif not Path(args.config_path).exists(): # and (args.start_time and args.end_time)
                if args.report_type == "trades":
                    default_api_version = "v3"
                elif args.report_type == "agg":
                    default_api_version = "v2"
                else:
                    default_api_version = "v3"

                pq_params = [(
                    PathParams(api_version=default_api_version),
                    QueryParams(start_time=args.start_time,end_time=args.end_time)
                )]

                # path and query param overrides; CLI overrides don't support lists
                if args.exchange: pq_params[0][0].exchange = args.exchange
                if args.instrument_class: pq_params[0][0].instrument_class = args.instrument_class
                if args.instrument: pq_params[0][0].instrument = args.instrument
                if args.aggregation_type: pq_params[0][0].summary_type = f"aggregations/{args.aggregation_type}"
                if args.interval: pq_params[0][1].interval = args.interval
            else:
                pq_params = load_config(
                    args.report_type,
                    args.config_path,
                    args.start_time,
                    args.end_time,
                    args.exchange,
                    args.instrument_class,
                    args.instrument,
                    args.aggregation_type,
                    args.interval
                )

            # download data
            client = KaikoClient(api_key=api_key)
            for path_params,query_params in pq_params:
                try:
                    filepath = client.download(
                        report_type=args.report_type,
                        path_params=path_params,
                        query_params=query_params,
                        download_dir=args.download_dir,
                        file_format=args.file_format,
                        write_gzip=args.write_gzip
                    )
                    if not filepath:
                        continue
                    print(filepath,file=sys.stdout)
                except httpx2.HTTPStatusError as err:
                    match err.response.status_code:
                        case 403:
                            print(f"403 client forbidden, skipping: {err}")
                            continue
                        case _:
                            print(f"unexpected status code {err.response.status_code}, skipping: {err}")
                            continue
                except StopIteration as err:
                    print(f"error: no data returned, {err=}, {path_params}, {query_params}", file=sys.stdout)
                    continue
            client.close()
        else:
            parser.print_help()

    except FileNotFoundError as err:
        print(f"error: file not found at {err.filename}")
        sys.exit(1)
    except FileExistsError as err:
        print(f"error: file already exists at {err.filename}")
        sys.exit(1)
    except PermissionError as err:
        print(f"error: insufficient permissions, {err}")
        sys.exit(1)
    except httpx2.HTTPStatusError as err:
        print(f"error: unsuccessful request, {unquote(str(err))}")
        sys.exit(1)
    except tomllib.TOMLDecodeError as err:
        print(f"error: failed to parse TOML, {err}")
        sys.exit(1)
    except ValueError as err:
        print(f"error: missing or invalid value, {err}")
        sys.exit(1)
    except (AttributeError, KeyError, TypeError, OSError) as err:
        print(f"{err=}, {type(err)=}")
        traceback.print_exc()
        sys.exit(1)
    except KeyboardInterrupt as err:
        print(f" user interrupt, exiting...")
        sys.exit(1)
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        traceback.print_exc()
        sys.exit(1)
