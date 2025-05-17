
from collections import defaultdict
import os
import json
import asyncio
import aiohttp

# Returns a nested dictionary for a ticker; { 'year1': {'ids': 'titles'}, 'year2' : {}, ...  }
def fetch_ids_for_ticker(ticker, save_ids_folder, year_until, year_since):
    year_id2title = defaultdict(dict)
    total_numIds = 0
    year_ids = 0
    ticker = ticker.lower()
    ticker_id_folder = os.path.join(save_ids_folder, ticker)
    if not os.path.exists(ticker_id_folder):
        os.makedirs(ticker_id_folder)
        
    for year in range(year_until, year_since - 1, -1): # 2024,..., 2006
        id2title = defaultdict(lambda: 'title')
        year_file_path = os.path.join(ticker_id_folder, f"{year}.json") # 2024.json, ..., 2006.json
        if not os.path.exists(year_file_path):
            continue
        with open(year_file_path, 'r') as json_file:
            json_data = json.load(json_file)
            if 'data' in json_data and json_data['data']:                
                for i in range(len(json_data['data'])):
                    id = json_data['data'][i]['id']
                    title = json_data['data'][i]['attributes']['title']
                    id2title[id] = title
        year_id2title[year] = id2title # 2024,...,2006. eg) 2024: 2024's data, ..., 2006: 2006's data
        year_ids_counts = len(id2title)
        total_numIds += year_ids_counts

    return year_id2title, total_numIds

async def fetch_reports(ticker, session, rate_limiter, save_folder, api_key, api_host, INITIAL_BACKOFF, MAX_RETRIES, year_until, year_since):
    # Local variables
    total_len = 0
    valid = 0
    total_requests = 0
    save_ids_folder = os.path.join(save_folder, "ids")
    
    
    year_id2title, total_numIds = fetch_ids_for_ticker(ticker, save_ids_folder, year_until, year_since) # 2024,...,2006. eg) 2024: 2024's data, ..., 2006: 2006's data
    years = year_id2title.keys()
    ticker = ticker.lower()
    sub_save_folder = os.path.join(save_folder, 'json')
    os.makedirs(sub_save_folder, exist_ok=True)
    report_save_folder = os.path.join(sub_save_folder, ticker)
    if not os.path.exists(report_save_folder):
        os.makedirs(report_save_folder)
        
    for year in years: # 2024, ... 2006
        year_folder_path = os.path.join(report_save_folder, str(year))
        if not os.path.exists(year_folder_path):
            os.makedirs(year_folder_path)
        
        for id in year_id2title[year].keys():
            title = year_id2title[year][id]
            report_file_path = os.path.join(year_folder_path, f"{id}.json")
            if os.path.exists(report_file_path):
                with open(report_file_path, 'r') as json_file:
                    existing_data = json.load(json_file)
                    if existing_data is not None and 'data' in existing_data and existing_data['data']:
                        # print(f"File for {ticker}, year {year}, id {id} already exists and contains data. Skipping download.")
                        continue
            url = "https://seeking-alpha.p.rapidapi.com/analysis/v2/get-details"
            querystring = {
                "id": id
            }
            headers = {
                "x-rapidapi-key": f"{api_key}",
                "x-rapidapi-host": f"{api_host}"
            }
            
            retries = 0
            backoff = INITIAL_BACKOFF
            
            while retries < MAX_RETRIES:
                try:
                    async with rate_limiter:
                        async with session.get(url, headers=headers, params=querystring, timeout=30) as response:
                            if response.status == 429:
                                print(f"Rate limit hit for {ticker}, retrying in {backoff} seconds...")
                                await asyncio.sleep(backoff)
                                backoff *= 2
                                retries += 1
                                continue
                            response.raise_for_status()
                            try:
                                response_json = await response.json()

                                total_requests += 1
                                    
                                if response_json.get('data'):
                                    total_len += len(response_json['data'])
                                    valid += 1
                                
                            except (aiohttp.ContentTypeError, json.JSONDecodeError):
                                print(f"Failed to decode JSON for {ticker} id {id}")
                                response_json = None    
                            
                            with open(report_file_path, 'w') as json_file:
                                json.dump(response_json, json_file)
                                
                            break
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                    error_message = f"Failed to fetch data for {ticker} id {id} with error {e}"
                    print(error_message)
                    retries *= 1
                    backoff *= 2
                    await asyncio.sleep(backoff)
                    continue
                


    
    
    
    
    
    

                
        