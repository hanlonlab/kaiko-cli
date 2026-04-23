#!/usr/bin/env python3
# coding: utf-8

import requests
import http
import time
import json
import os
import sys
import dateutil.parser

"""
API key permits the following access:
Data types -> All (Trades, Order Books, OHLCV/VWAP Aggregates)
Endpoints Access -> All
Exchanges/Pairs -> All
Delivery -> Rest API
"""

CRED_PATH = '../.kaikopass'  # api credentials stored as string in format "kaikoapi:apikey"
with open(CRED_PATH, 'r') as f:
    valid_creds = [line for line in f if line.startswith('kaiko')][0].split(':')
    API_KEY = valid_creds[1].rstrip('\n')

MKT_API_CONFIG = {
    'base_url': 'https://us.market-api.kaiko.io/',
    'version': 'v1',
    'headers': {'Accept': 'application/json','X-Api-Key': API_KEY}
}
REF_API_CONFIG = {
    'base_url': 'https://reference-data-api.kaiko.io/',
    'version': 'v1',
    'headers': {'Accept': 'application/json'}
}
VALID_REFERENCE_TYPES = ['assets','exchanges','instruments','pools']
VALID_MKT_DATA_TYPES = ['trades','ohlcv','order_book']
VALID_ARG_LENS = {
    'new': [3],
    'bulk': [3],
    'comp': [3],
    'list': [3,4]
}
ERROR_CODE_MAPPING = {
    '400': 'Bad Request',
    '401': 'Unauthorized -- You are not authenticated properly. See Authentication.',
    '403': 'Forbidden -- You don\'t have access to the requested resource.',
    '404': 'Not Found',
    '405': 'Method Not Allowed',
    '406': 'Not Acceptable',
    '429': 'Too Many Requests -- Rate limit exceeded. Contact us if you think you have a need for more.',
    '500': 'Internal Server Error -- We had a problem with our service. Try again later.',
    '503': 'Service Unavailable -- We\'re temporarily offline for maintenance.'
}

TMP_DIR = '../tmp/'
TIMEOUT_S = 60
os.makedirs(TMP_DIR, exist_ok=True)

KAIKO_CONFIG_PATH = './config/kaiko.conf'
with open(KAIKO_CONFIG_PATH) as file:
    kaiko_config = json.load(file)

def download_data(params: dict, inst: dict, get_url: str, export_path: str, ref_cols: bool = True) -> None:

    # hacky workaround for export_path - fix this TODO
    export_path_rel = f"{os.path.dirname(export_path)}/{os.path.splitext(os.path.basename(export_path))[0].replace(".","")}.csv"
    export_path = os.path.abspath(export_path_rel)

    # ref_cols | default action
    with requests.Session() as s:
        while True: # loop on ValueError
            try:
                with s.request(method='GET', url=get_url, headers=MKT_API_CONFIG['headers'], stream=True) as r:
                    # download data
                    #print('.',params['exchange'],get_url)
                    response = json.loads(r.content)
                    # print(get_url)
                    # print(json.dumps(response,indent=2))

                    # check for lack of authorization for requested data
                    if ('message' in response and response['message'] == 'Not Authorized'):
                        return

                    # general metadata headers
                    ref_headers = 'commodity,exchange,instrument_class,instrument,'
                    ref_data = '{commodity},{exchange},{instrument_class_override},{instrument_override},'.format(**params,instrument_class_override=inst['class'],instrument_override=inst['code'])

                    if os.path.isfile(export_path):
                        print(f"append to {export_path}")
                        with open(export_path, 'a') as file:
                            for record in response['data']:
                                file.write(ref_data + ','.join(['"'+str(val)+'"' for val in record.values()]) + '\n')
                    else:
                        # print(f"write new file at {export_path}")
                        with open(export_path, 'w') as file:
                            file.write(ref_headers + ','.join(list(response['data'][0].keys())) + '\n')
                            for record in response['data']:
                                file.write(ref_data + ','.join(['"'+str(val)+'"' for val in record.values()]) + '\n')
                    break # break out of _while True_ loop
            except (ValueError, requests.exceptions.ChunkedEncodingError) as e:
                print(e)
                print(get_url)
                print("Retrying...")
            except IndexError as e:
                err_msg = f"ERROR {e}: {get_url}\nNo data downloaded from response: {response}"
                print(err_msg)
                with open("./log.txt", "a") as f:
                    f.write(f"{err_msg}\n")
            except (ConnectionError, http.client.RemoteDisconnected) as e:
                print(e)
                print(get_url)
                print(f"Remote disconnected, waiting {TIMEOUT_S} seconds and retrying...")
                time.sleep(TIMEOUT_S)
            except Exception as e:
                print('Uncaught exception')
                print(get_url)
                try:
                    print(response)
                except Exception as e:
                    pass
                sys.exit(e)

        # write subsequent pages
        while 'next_url' in response:
            try:
                with s.request(method='GET', url=response['next_url'], headers=MKT_API_CONFIG['headers'], stream=True) as r:
                    response = json.loads(r.content)
                    with open(export_path, 'a') as file:
                        # print(f"append page to {export_path}")
                        for record in response['data']:
                            file.write(ref_data + ','.join(['"'+str(val)+'"' for val in record.values()]) + '\n')
            except (ValueError, requests.exceptions.ChunkedEncodingError) as e:
                print(e)
                print(response['next_url'],r.url,r.status_code,r.reason,'\n',r.request)
                print("Retrying...")
            except Exception as e:
                print('Uncaught exception on')
                print(response['next_url'],r.url,r.status_code,r.reason,'\n',r.request)
                sys.exit(e)

    print(export_path)


def bulk_mkt_data_request_exch(mkt_data_type: str, exchange_override: bool = False, exchange_override_value: str = '') -> int:
    r"""
    Pull data for all instruments traded on one exchange

    :return: 0 on success
    """

    params = kaiko_config[mkt_data_type]
    if exchange_override:
        params['exchange'] = exchange_override_value

    query_start = dateutil.parser.parse(params['start_time'])
    query_end = dateutil.parser.parse(params['end_time'])
    bulk_export_path = TMP_DIR + mkt_data_type + '_{exchange}_bulk_{start_time}_{end_time}.csv'.format(**params).replace(':','')
    
    # Deleting the file if it already exists
    if os.path.isfile(bulk_export_path):
        os.remove(bulk_export_path)

    with requests.Session() as s:
        # get all instruments for exchange
        ref_url = REF_API_CONFIG['base_url'] + REF_API_CONFIG['version'] + '/' + 'instruments?exchange_code={exchange}'.format(**params)
        r = s.request(method='GET', url=ref_url, headers=REF_API_CONFIG['headers'])

        # get a list of json objects representing instruments traded on exchange
        exch_inst_list = json.loads(r.content)['data']

        # iterate over instruments, query and download if within query range
        for inst in exch_inst_list:

            # get datetimes for first and last trade
            if inst['trade_start_time'] is not None:
                first_trade = dateutil.parser.parse(inst['trade_start_time'])
            else:
                first_trade = inst['trade_start_time']
            if inst['trade_end_time'] is not None:
                last_trade = dateutil.parser.parse(inst['trade_end_time'])
            else:
                last_trade = inst['trade_end_time']
            
            # if no trades occured within query range, do nothing
            if (first_trade is None) or (first_trade >= query_end) or ((last_trade is not None) and (last_trade <= query_start)):
                pass
            else:
                # set up GET url
                if mkt_data_type == 'trades':
                    get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class_override}/{instrument_override}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}'.format(**params,instrument_class_override=inst['class'],instrument_override=inst['code'])
                elif mkt_data_type == 'ohlcv':
                    get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class_override}/{instrument_override}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}&interval={interval}'.format(**params,instrument_class_override=inst['class'],instrument_override=inst['code'])
                elif mkt_data_type == 'order_book':
                    get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class_override}/{instrument_override}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}&limit_orders={limit_orders}&slippage={slippage}&slippage_ref={slippage_ref}'.format(**params,instrument_class_override=inst['class'],instrument_override=inst['code'])
                else:
                    get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class_override}/{instrument_override}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}'.format(**params,instrument_class_override=inst['class'],instrument_override=inst['code'])
                
                base_asset = inst['base_asset']
                quote_asset = inst['quote_asset']
                class_asset_export_path = TMP_DIR + mkt_data_type + '_{exchange}_{base_asset}_{quote_asset}_{instrument_class_override}_{start_time}_{end_time}.csv'.format(**params,instrument_class_override=inst['class'],base_asset=base_asset, quote_asset=quote_asset).replace(':','')
                if base_asset == "btc" and quote_asset in ("usd","usdt") and inst['class'] == "perpetual-future": #delete this line when complete
                    download_data(params, inst, get_url, class_asset_export_path)

    return 0


def bulk_mkt_data_comprehensive(mkt_data_type: str) -> int:
    r"""
    Pull all instruments traded on all exchanges

    :return: 0 on success
    """

    with requests.Session() as s:
        # get all exchanges
        all_exch_url = REF_API_CONFIG['base_url'] + REF_API_CONFIG['version'] + '/exchanges'
        r = s.request(method='GET', url=all_exch_url, headers=REF_API_CONFIG['headers'])

        # get a list of json objects representing exchanges supported by Kaiko
        exch_list = sorted([exch['code'] for exch in json.loads(r.content)['data']])
        #exch_list = sorted([exch['code'] for exch in json.loads(r.content)['data'] if exch['code'] >= "delt"]) # for restarts
        print(exch_list)

    # iterate over list of exchanges, running bulk download for each exchange from config file parameters
    for exch in exch_list:
        bulk_mkt_data_request_exch(mkt_data_type=mkt_data_type, exchange_override=True, exchange_override_value=exch)

    return 0


def new_mkt_data_request(mkt_data_type: str, ref_cols: bool = True) -> int:
    r"""
    Send a new request to the Kaiko Market Data API
    :return: 0 on success
    """

    params = kaiko_config[mkt_data_type]
    if mkt_data_type == 'trades':
        get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}'.format(**params)
        export_path = TMP_DIR + mkt_data_type + '_{exchange}_{instrument_class}_{instrument}_{start_time}_{end_time}.csv'.format(**params).replace(':','')
        # print(get_url)
    elif mkt_data_type == 'ohlcv':
        get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}&interval={interval}'.format(**params)
        export_path = TMP_DIR + mkt_data_type + '_{interval}_{exchange}_{instrument_class}_{instrument}_{start_time}_{end_time}.csv'.format(**params).replace(':','')
    elif mkt_data_type == 'order_book':
        get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}&limit_orders={limit_orders}&slippage={slippage}&slippage_ref={slippage_ref}'.format(**params)
        export_path = TMP_DIR + mkt_data_type + '_{exchange}_{instrument_class}_{instrument}_{start_time}_{end_time}.csv'.format(**params).replace(':','')
    else: # rely on API defaults
        get_url = MKT_API_CONFIG['base_url'] + MKT_API_CONFIG['version'] + '/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}/{request_type}?start_time={start_time}&end_time={end_time}&page_size={page_size}'.format(**params)
        export_path = TMP_DIR + mkt_data_type + '_{exchange}_{instrument_class}_{instrument}_{start_time}_{end_time}.csv'.format(**params).replace(':','')

    # Deleting the file if it already exists
    if os.path.isfile(export_path):
        os.remove(export_path)

    download_data(params, {"class": params["instrument_class"], "code": params["instrument"]}, get_url, export_path, True)
    return 0


def new_ref_data_request(request_type: str, query_filter: str = '') -> int:
    r"""
    Send a new request to the Kaiko Reference Data API. See https://docs.kaiko.com/#reference-data-api

    :param request_type: Type of reference data to request
    :type request_type: str
    :return: 0 on success
    """

    get_url = REF_API_CONFIG['base_url'] + REF_API_CONFIG['version'] + '/' + request_type + query_filter

    with requests.Session() as s:
        # get ref data
        r = s.request(method='GET', url=get_url, headers=REF_API_CONFIG['headers'])

        # print result
        print('Status code: ', r.status_code, '\n\nContent:\n', json.dumps(json.loads(r.content), indent=4), '\n\n')

    return 0


def usage():
    r"""
    Print a descriptive usage message
    """

    msg = """usage: {0} func opt [query]
    func: "new" for market data, "list" for metadata
    opt:
        if func is new|bulk|comp: type of market data to request, one of {1}
        if func is list: type of metadata to request, one of {2}
    query:
        if func is new|bulk|comp: omit
        if func is list: query string in the form \\?param=val&param=val - more info at https://docs.kaiko.com/""".format(
            sys.argv[0],
            '|'.join(VALID_MKT_DATA_TYPES),
            '|'.join(VALID_REFERENCE_TYPES)
            )

    sys.exit(msg)


if __name__ == "__main__":

    if sys.argv[1] == 'new' and len(sys.argv) in VALID_ARG_LENS['new'] and sys.argv[2] in VALID_MKT_DATA_TYPES:
        new_mkt_data_request(sys.argv[2])
    elif sys.argv[1] == 'bulk' and len(sys.argv) in VALID_ARG_LENS['bulk'] and sys.argv[2] in VALID_MKT_DATA_TYPES:
        bulk_mkt_data_request_exch(mkt_data_type=sys.argv[2])
    elif sys.argv[1] == 'comp' and len(sys.argv) in VALID_ARG_LENS['comp'] and sys.argv[2] in VALID_MKT_DATA_TYPES:
        bulk_mkt_data_comprehensive(mkt_data_type=sys.argv[2])
    elif sys.argv[1] == 'list'  and len(sys.argv) in VALID_ARG_LENS['list'] and sys.argv[2] in VALID_REFERENCE_TYPES:
        if len(sys.argv) >= 4:
            new_ref_data_request(sys.argv[2], sys.argv[3])
        else:
            new_ref_data_request(sys.argv[2])
    else:
        usage()
