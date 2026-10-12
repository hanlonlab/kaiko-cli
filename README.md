## Kaiko CLI
Kaiko is a cryptocurrency market data product. The Lab subscribes to Kaiko's centralized market data product through their [REST API](https://docs.kaiko.com/rest-api). 

The `kaiko.py` CLI can be used to download data, and has the following options:
- `genconf`: generate an editable TOML config file describing the data to be downloaded
- `dl <data-type>`: download data of the desired type 

Running `dl <data-type>` will read the config file, then download data and save it to a `tmp` directory in the project root. The `<data-type>` can be one of: `trades` or `agg`. Users should modify the config file, named `kaiko.toml` by default, to download the desired data. The `instrument` config value(s) will be parsed as base_asset-quote_asset; setting `instrument_class` to a derivative class will download the whole chain. 

To install dependencies, use [uv](https://docs.astral.sh/uv/getting-started/) to create and activate a virtual environment, then run `uv sync` from the project's root.

Run `python kaiko.py --help` or `python kaiko.py <args> --help` for more info.
