import requests
import json

def test_youtube_api():
    url = "http://127.0.0.1:8000/collect/youtube_comments"
    data = {
        "keyword": "노트북",
        "max_results": 5
    }
    
    try:
        response = requests.post(url, json=data, timeout=60)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Success: {json.dumps(result, ensure_ascii=False, indent=2)}")
        else:
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"Exception: {str(e)}")

if __name__ == "__main__":
    test_youtube_api()