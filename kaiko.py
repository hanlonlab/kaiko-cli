import os
import sys
import traceback
import gzip
import re
import json
import tomllib
import argparse
from pathlib import Path
from dataclasses import dataclass, asdict, fields
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
from urllib.parse import unquote

import httpx2

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "t", "1")

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
    exchange: str = "cbse"
    instrument_class: str = "spot"
    instrument: str = "btc-usd"

@dataclass
class QueryParams:

    # non-default query params
    start_time: str
    end_time: str

    # aggregations only
    interval: str = "1m"

    # default query params
    page_size: str = "10000"
    sort: str = "desc"

# TODO add support for metadata retrieval from reference data endpoint, support for bulk exchange downloads, support for derivatives download by base or quote asset
class KaikoClient:
    def __init__(self, api_key: str, client = None):
        self.api_key = api_key
        self.client = client or httpx2.Client()
        self.headers = {"Accept": "application/json"}

    def close(self):
        self.client.close()

    def fetch(self, path_params, query_params):
        url = "https://{region}.market-api.kaiko.io/{api_version}/data/{data_type}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}/{summary_type}".format(**asdict(path_params))
        headers = self.headers | {"X-Api-Key": self.api_key}

        query_params = asdict(query_params)
        while url:
            r = self.client.get(url, headers=headers, params=query_params)
            _ = r.raise_for_status()
            if DEBUG:
                print(unquote(str(r.url)),file=sys.stdout)
                print(r.text,file=sys.stderr)

            yield from r.json().get("data")
            url = r.json().get("next_url")
            query_params = None

    def resolve_filename(self, report_type: str, path_params, query_params) -> str:

        # clean ISO-8601 strings for filename
        start_time_utc = datetime.fromisoformat(query_params.start_time).astimezone(ZoneInfo("UTC")).isoformat(timespec="microseconds").replace('+00:00','').replace('T','').replace('.','')
        end_time_utc = datetime.fromisoformat(query_params.end_time).astimezone(ZoneInfo("UTC")).isoformat(timespec="microseconds").replace('+00:00','').replace('T','').replace('.','')

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

    def download(
            self, report_type, path_params, query_params,
            download_dir: str = f"{Path(__file__).parent}",
            file_format: str = "jsonl",
            write_gzip: bool = True
        ) -> str:

        filename_root = self.resolve_filename(report_type, path_params, query_params)
        if file_format == "csv" and write_gzip:
            file_path = Path(f"{download_dir}/{filename_root}.csv.gz")
            with gzip.open(file_path, 'wt', newline='', encoding="utf-8") as f:
                record_iter = self.fetch(path_params, query_params)
                first = next(record_iter)
                f.write(f'{",".join([f'"{k}"' for k,_ in first.items()])}\n')
                f.write(f'{",".join([f'"{v}"' for _,v in first.items()])}\n')
                f.writelines(f'{",".join([f'"{v}"' for _,v in record.items()])}\n' for record in record_iter)
        elif file_format == "csv":
            file_path = Path(f"{download_dir}/{filename_root}.csv")
            with open(file_path, 'w', newline='', encoding="utf-8") as f:
                record_iter = self.fetch(path_params, query_params)
                first = next(record_iter)
                f.write(f'{",".join([f'"{k}"' for k,_ in first.items()])}\n')
                f.write(f'{",".join([f'"{v}"' for _,v in first.items()])}\n')
                f.writelines(f'{",".join([f'"{v}"' for _,v in record.items()])}\n' for record in record_iter)
        elif file_format in ["json","jsonl"] and write_gzip:
            file_path = Path(f"{download_dir}/{filename_root}.jsonl.gz")
            with gzip.open(file_path, 'wt', newline='', encoding="utf-8") as f:
                f.writelines(f"{record}\n" for record in self.fetch(path_params, query_params))
        elif file_format in ["json","jsonl"]:
            file_path = Path(f"{download_dir}/{filename_root}.jsonl")
            with open(file_path, 'w', newline='', encoding="utf-8") as f:
                f.writelines(f"{record}\n" for record in self.fetch(path_params, query_params))

        return file_path.resolve()

def generate_config_default(config_path: str = f"{Path(__file__).parent}/kaiko.toml", aggregation_type: str = "ohlcv"):

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
    ) -> list[tuple[PathParams,QueryParams]]:

    # read .toml or .json config file
    ext = Path(config_path).suffix.lower()
    if ext == ".toml":
        with open(config_path, 'rb') as f:
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

    # split out exchanges, if provided as a list; supports multiple exchanges per config file
    conf_list = [conf_dict]
    if isinstance(conf_dict.get("exchange"), list):
        conf_list = [{**conf_dict, "exchange": e} for e in conf_dict["exchange"]]

    # split out instruments, if provided as a list; supports multiple instruments per config file
    if isinstance(conf_dict.get("instrument_class"), list) and isinstance(conf_dict.get("instrument"), list):
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

    # get a list of fields dataclass fields for filtering
    path_fields = [f.name for f in fields(PathParams)]
    query_fields = [f.name for f in fields(QueryParams)]

    conf_param_list = [
        (
            PathParams(**{k:v for k,v in d.items() if k in path_fields}),
            QueryParams(**{k:v for k,v in d.items() if k in query_fields})
        )
        for d in conf_list
    ]

    return conf_param_list

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
    parser_conf.add_argument(
        "--config-dir",
        default=f"{Path(__file__).parent}",
        dest="config_dir",
        help="Directory for default config file."
        )
    parser_conf.add_argument(
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
    parser_download.add_argument(
        "report_type",
        choices=["trades", "agg"],
        type=str,
        help="The type of data to download."
    )

    parser_download.add_argument(  # TODO support path to .env file
        *["-e","--env","--env-var"],
        dest="env_var",
        default="KAIKO_API_KEY",
        help="Environment variable holding Kaiko API key. Defaults to KAIKO_API_KEY."
    )
    parser_download.add_argument(
        *["-a","--key","--api-key"],
        dest="api_key",
        help="API key value"
    )

    parser_download.add_argument(
        *["--conf","--config-path"],
        dest="config_path",
        default=f"{Path(__file__).parent}/kaiko.toml",
        help="Path to TOML or JSON config file."
    )
    parser_download.add_argument(
        *["-d","--download-dir"],
        dest="download_dir",
        default=f"{Path(__file__).parent}/tmp",
        help="Directory for downloaded files."
    )
    parser_download.add_argument(
        *["-f","--format","--file-format"],
        dest="file_format",
        choices=["csv", "json"],
        default="csv",
        help="Download format."
    )
    parser_download.add_argument(
        *["--gzip","--write-gzip"],
        dest="write_gzip",
        action="store_true",
        help="Download gzip-encoded file."
    )

    # dl/download optional overrides
    parser_download.add_argument(
        *["--agg","--agg-type","--aggregation-type"],
        choices=["count_ohlcv_vwap", "ohlcv", "vwap"],
        dest="aggregation_type",
        help="Aggregation type."
    )
    parser_download.add_argument(
        *["--exch","--exchange"],
        dest="exchange",
        help="Exchange code config override."
    )
    parser_download.add_argument(
        *["--class","--instrument-class"],
        dest="instrument_class",
        help="Instrument class config override."
    )
    parser_download.add_argument(
        *["--inst","--instrument"],
        dest="instrument",
        help="Instrument code config override."
    )
    parser_download.add_argument(
        *["--start","--start-time"],
        dest="start_time",
        help="Start time config override."
    )
    parser_download.add_argument(
        *["--end","--end-time"],
        dest="end_time",
        help="End time config override."
    )
    parser_download.add_argument(
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
                if args.exchange: pq_params[0].exchange = args.exchange
                if args.instrument_class: pq_params[0].instrument_class = args.instrument_class
                if args.instrument: pq_params[0].instrument = args.instrument
                if args.aggregation_type: pq_params[0].aggregation_type = args.aggregation_type
                if args.interval: pq_params[1].interval = args.interval
            else:
                pq_params = load_config(args.report_type, args.config_path, args.start_time, args.end_time, args.exchange, args.instrument_class, args.instrument, args.aggregation_type, args.interval)

            # download data
            client = KaikoClient(api_key=api_key)
            for path_params,query_params in pq_params:
                filepath = client.download(report_type=args.report_type, path_params=path_params, query_params=query_params, download_dir=args.download_dir, file_format=args.file_format, write_gzip=args.write_gzip)
                print(filepath,file=sys.stdout)
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
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        traceback.print_exc()
        sys.exit(1)
