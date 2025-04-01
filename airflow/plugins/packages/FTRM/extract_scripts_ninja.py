import requests
import os
import json
import asyncio
import aiohttp



async def fetch_reports(ticker, session, rate_limiter, save_folder, api_key, INITIAL_BACKOFF, MAX_RETRIES, year_until, year_since):
    # Local variables
    total_len = 0
    valid = 0
    total_requests = 0


    ticker_lower = ticker.lower()
    sub_save_folder = os.path.join(save_folder, 'json')
    os.makedirs(sub_save_folder, exist_ok=True)
    report_save_folder = os.path.join(sub_save_folder, ticker_lower)
    if not os.path.exists(report_save_folder):
        os.makedirs(report_save_folder)
    
    for year in range(year_until, year_since - 1, -1):
        year_folder_path = os.path.join(report_save_folder, str(year))
        if not os.path.exists(year_folder_path):
            os.makedirs(year_folder_path)
        
        
        for quarter in range(1, 5):
        
            report_file_path = os.path.join(year_folder_path, f"calls_{ticker}_{year}_{quarter}.json")
            if os.path.exists(report_file_path):
                with open(report_file_path, 'r') as json_file:
                    existing_data = json.load(json_file)
                    if existing_data is not None and 'transcript' in existing_data and existing_data['transcript']:
                        continue
                    
            url = 'https://api.api-ninjas.com/v1/earningstranscript?ticker={}&year={}&quarter={}'.format(ticker, year, quarter)
            headers = {'X-Api-Key': api_key}
            response = requests.get(url, headers=headers)
                
            retries = 0
            backoff = INITIAL_BACKOFF
            
            while retries < MAX_RETRIES:
                try:
                    async with rate_limiter:
                        async with session.get(url, headers=headers, timeout=30) as response:
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

                                if isinstance(response_json, list) and not response_json:
                                    break
                                
                                if response_json and response_json.get('transcript'):
                                    total_len += len(response_json['transcript'])
                                    valid += 1
                                
                            except (aiohttp.ContentTypeError, json.JSONDecodeError):
                                print(f"Failed to decode JSON for {ticker} id {id}")
                                response_json = None    
                            
                            with open(report_file_path, 'w') as json_file:
                                json.dump(response_json, json_file)
                                
                            break
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                    error_message = f"Failed to fetch data for {ticker} id {id} with error {e}"
                    print('Error:', error_message)
                    retries *= 1
                    backoff *= 2
                    await asyncio.sleep(backoff)
                    continue
                



        
    
    
    
    
    
    

                
        