import requests

proxies = {
    'http': 'http://sofascore_dMkbW:+Sofascoremanga10@pr.oxylabs.io:7777',
    'https': 'http://sofascore_dMkbW:+Sofascoremanga10@pr.oxylabs.io:7777'
}

try:
    response = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=30)
    if response.status_code == 200:
        print(f"✅ Proxy is working! Status: {response.status_code}")
        print(f"Response: {response.text}")
    else:
        print(f"❌ Proxy failed with status: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"❌ Proxy test failed: {e}")