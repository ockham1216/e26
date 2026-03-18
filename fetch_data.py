import os
import json
import asyncio
import aiohttp
import datetime

API_KEY = os.environ.get("OPEN_API_KEY")

async def fetch_page(session, url):
    try:
        async with session.get(url, timeout=30) as response:
            if response.status != 200:
                print(f"HTTP Error {response.status} for url: {url}")
                return []
            data = await response.json(content_type=None)
            if not data or not data.get("response"):
                return []
            header = data["response"].get("header")
            if not header or header.get("resultCode") != "INFO-00":
                if header:
                    print(f"API Error Msg: {header.get('resultMsg')}")
                return []
            
            body = data["response"].get("body", {})
            items = body.get("items", {})
            if not items:
                return []
            
            item_list = items.get("item", [])
            if isinstance(item_list, dict):
                item_list = [item_list]
            return item_list
    except Exception as e:
        print(f"Error fetching page: {e}")
        return []

async def fetch_type(session, type_code):
    query_params = {
        "serviceKey": API_KEY,
        "numOfRows": "100",
        "sgId": "20260603",
        "sgTypecode": type_code,
        "resultType": "json",
        "pageNo": "1"
    }
    encoded_params = "&".join([f"{k}={v}" for k, v in query_params.items()])
    url = f"http://apis.data.go.kr/9760000/PofelcddInfoInqireService/getPoelpcddRegistSttusInfoInqire?{encoded_params}"
    
    print(f"Starting fetch for typeCode: {type_code}")
    
    # 1. First page
    try:
        async with session.get(url, timeout=30) as response:
            if response.status != 200:
                print(f"HTTP Error {response.status} for typeCode {type_code}")
                return []
            data = await response.json(content_type=None)
            if not data or not data.get("response"):
                return []
            header = data["response"].get("header")
            if not header or header.get("resultCode") != "INFO-00":
                if header:
                    print(f"API Error for type {type_code}: {header.get('resultMsg')}")
                return []
            
            body = data["response"].get("body", {})
            total_count = int(body.get("totalCount", 0))
            items = body.get("items", {})
            item_list = items.get("item", []) if items else []
            if isinstance(item_list, dict):
                item_list = [item_list]
            
            all_items = item_list
            print(f"Type {type_code} totalCount: {total_count}")
            
            if total_count > 100:
                total_pages = (total_count - 1) // 100 + 1
                tasks = []
                for page in range(2, total_pages + 1):
                    p_query = query_params.copy()
                    p_query["pageNo"] = str(page)
                    # stagger requests slightly to avoid abusing the API synchronously
                    p_encoded = "&".join([f"{k}={v}" for k, v in p_query.items()])
                    p_url = f"http://apis.data.go.kr/9760000/PofelcddInfoInqireService/getPoelpcddRegistSttusInfoInqire?{p_encoded}"
                    tasks.append(fetch_page(session, p_url))
                
                results = await asyncio.gather(*tasks)
                for res in results:
                    all_items.extend(res)
            
            for item in all_items:
                item["_typeCode"] = str(type_code)
                
            print(f"Completed fetching {len(all_items)} items for typeCode {type_code}")
            return all_items
    except Exception as e:
        print(f"Error for type {type_code} main fetch: {e}")
        return []

async def main():
    if not API_KEY:
        print("Error: OPEN_API_KEY environment variable is missing.")
        # Create a dummy data for testing if no key? No, just exit.
        return
        
    type_codes = ['2', '3', '4', '5', '6', '11']
    all_candidates = []
    
    connector = aiohttp.TCPConnector(limit=10) # 10 limits concurrent connections
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_type(session, tc) for tc in type_codes]
        results = await asyncio.gather(*tasks)
        for res in results:
            all_candidates.extend(res)
            
    if all_candidates:
        # Deduplicate to remove identical candidates that the API might return multiple times with different huboids
        unique_candidates = []
        seen = set()
        for c in all_candidates:
            key = (
                c.get("_typeCode", ""),
                c.get("sdName", ""),
                c.get("wiwName", ""),
                c.get("sggName", ""),
                c.get("name", ""),
                c.get("birthday", "")
            )
            if key not in seen:
                seen.add(key)
                unique_candidates.append(c)
        all_candidates = unique_candidates

        # Convert to Korean Time (KST is UTC+9)
        now = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
        output = {
            "last_updated": now.strftime("%Y-%m-%d %H:%M:%S") + " (KST)",
            "data": all_candidates
        }
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False)
        print(f"Successfully saved {len(all_candidates)} candidates to data.json")
    else:
        print("No candidates fetched or API failed. data.json not updated.")

if __name__ == "__main__":
    asyncio.run(main())
