import requests

# 1. HARDCODE YOUR KEY DIRECTLY JUST FOR THIS TEST
# Paste the exact 64-character string from your dashboard here
TEST_KEY = "6d610eb6b6bc809f9c21bde0f9d985daaeb0c6db85c75c5fcac94519984524d4"  

url = "https://apiv3.apifootball.com/"
params = {
    "action": "get_standings",
    "league_id": "152",
    "APIkey": TEST_KEY
}

print(f"Testing URL: {url}")
print(f"With Key: {TEST_KEY[:5]}...{TEST_KEY[-5:]}") # Prints part of the key to verify it loaded

response = requests.get(url, params=params)

print(f"\nStatus Code: {response.status_code}")
print(f"Response: {response.text[:500]}") # Print first 500 characters