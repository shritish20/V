import sys
import os
import urllib.parse
import requests
from dotenv import load_dotenv

# Load env from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

API_KEY = os.getenv("UPSTOX_API_KEY")
API_SECRET = os.getenv("UPSTOX_API_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")

def get_access_token():
    print("🚀 VOLGUARD TOKEN GENERATOR")
    print("---------------------------")
    
    if not API_KEY or not API_SECRET:
        print("❌ Error: UPSTOX_API_KEY or UPSTOX_API_SECRET missing in .env")
        return

    # 1. Generate Login URL
    params = {
        "response_type": "code",
        "client_id": API_KEY,
        "redirect_uri": REDIRECT_URI,
        "state": "volguard_init"
    }
    login_url = f"https://api-v2.upstox.com/v2/login/authorization/dialog?{urllib.parse.urlencode(params)}"
    
    print(f"\n1️⃣  Open this URL in your browser:\n")
    print(f"\033[94m{login_url}\033[0m")
    print("\n2️⃣  Login -> 'Accept' -> You will be redirected to a 'localhost' URL.")
    print("3️⃣  Copy the 'code' parameter from that URL (e.g., ?code=ELk5...).")
    
    auth_code = input("\n🔑 Paste the Auth Code here: ").strip()
    
    if not auth_code:
        print("❌ Code cannot be empty.")
        return

    # 2. Exchange Code for Token
    token_url = "https://api-v2.upstox.com/v2/login/authorization/token"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "code": auth_code,
        "client_id": API_KEY,
        "client_secret": API_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }

    print("\n🔄 Fetching Token...")
    response = requests.post(token_url, headers=headers, data=data)
    
    if response.status_code == 200:
        resp_json = response.json()
        access_token = resp_json.get("access_token")
        print("\n✅ SUCCESS! Here is your new Access Token:\n")
        print(f"\033[92m{access_token}\033[0m")
        print("\n👉 Copy this to your .env file or use the /api/token/refresh endpoint.")
    else:
        print(f"\n❌ FAILED: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    get_access_token()
