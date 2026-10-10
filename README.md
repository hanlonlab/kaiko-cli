## Kaiko Market Data API
Kaiko is a cryptocurrency spot and derivative market data product. The Lab subscribes to Kaiko's centralized market data product through their REST API. Kaiko has an unauthenticated [Reference Data API](https://docs.kaiko.com/rest-api/reference-data/asset-codes) which can be used to answer questions about what exchanges/instruments are available. (CLI support for reference data access and bulk downloads by exchange will be re-added in the near future.)

## Using the CLI

The `kaiko.py` CLI simplifies data downloads from the Kaiko API; it has the following options:
- `genconf`: generate an editable TOML config file containing data request parameters
- `dl <data-type>`: download data of the desired type 

When running `python kaiko.py dl <data-type>`, the CLI will read request parameters from a `kaiko.toml` or `kaiko.json` configuration file in the project root; to download the desired data, users should edit the config file or use the CLI args documented in `python kaiko.py dl <data-type> --help` to set the desired exchanges/instruments/date range/etc. The `<data-type>` can be one of: `trades` or `agg`. By default downloaded files will be placed in a `tmp` directory in the project root. Data downloads are paginated, and the CLI will block while downloading data.

To install dependencies, use [uv](https://docs.astral.sh/uv/getting-started/) to create and activate a virtual environment, then run `uv sync` from the project's root.

Run `python kaiko.py --help` or `python kaiko.py <args> --help` for more info.
