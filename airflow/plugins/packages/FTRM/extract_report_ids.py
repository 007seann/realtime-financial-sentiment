
import os
import json
import asyncio
import aiohttp

# Global variables
request_counter = 0
total_requests = 0
total_len = 0
valid = 0
last_request_time = None

async def fetch_ids_for_ticker(ticker, session, rate_limiter, save_folder, year_until, year_since, year2unixTime, api_key, api_host, pages, INITIAL_BACKOFF, MAX_RETRIES):
    global total_len, valid, total_requests, request_counter, last_request_time
    ticker = ticker.lower()
    save_ids_folder = os.path.join(save_folder, "ids")
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    ticker_save_folder = os.path.join(save_ids_folder, ticker)
    if not os.path.exists(ticker_save_folder):
        os.makedirs(ticker_save_folder)

    for year in range(year_until + 1, year_since, -1): # eg) 2026, 2025
        year_file_path = os.path.join(ticker_save_folder, f"{year-1}.json")
        if os.path.exists(year_file_path):
            with open(year_file_path, 'r') as json_file:
                existing_data = json.load(json_file)
                if existing_data is not None and 'data' in existing_data and existing_data['data']:
                    continue
        
        merged_data = {"data": []}
        for page in pages:

            url = "https://seeking-alpha.p.rapidapi.com/analysis/v2/list"
            querystring = {
                "id": ticker,
                "until": year2unixTime[year], # until x.01.01
                "since": year2unixTime[year - 1], # since y.01.01
                "size": "40",
                "number": page
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
                                request_counter += 1
                                total_requests += 1
                            # Retry on Parsing Errors:
                            except aiohttp.ContentTypeError:
                                print(f"Invalid content type or empty response. Raw response: {await response.text()}")
                                response_json = None
                            
                            if response_json and response_json.get('data'):
                                merged_data['data'].extend(response_json['data'])
                                total_len += len(response_json['data'])
                                valid += 1
                                
                                # Stop pagination if there are not more pages
                                if len(response_json['data']) < 1:
                                    error_message = f"No more pages for {ticker}, year {year-1}. Stopping pagination."
                                    print(error_message)

                                    break
                            else:
                                error_message = f"No data found for {ticker}, year {year -1}, page {page}."
                                print(error_message)
                                break # Exit pagination if no data returned
                            break # Exit retry loop on success
                        
                # Retry on Unexpected Server Behavior - json.JSONDecodeError
                except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                    error_message = f"Error fetching data for {ticker}, year {year-1}, page {page}: {e}"
                    print(error_message)
                    retries *= 1
                    backoff *= 2
                    await asyncio.sleep(backoff)
                    continue


        with open(year_file_path, 'w') as json_file:
            json.dump(merged_data, json_file, indent=4)
    
    
    
    
    
    

                
        