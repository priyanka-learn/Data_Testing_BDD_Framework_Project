"""
Simple Databricks connection test.
Run this after filling in HOST and TOKEN in application.properties.

Expected output on success:
  Connected! Logged in as: your.email@example.com (user id: 12345)
"""

import requests
from configs.config import Config

cfg = Config()

host  = cfg.get("DATABRICKS", "HOST",  fallback="").rstrip("/")
token = cfg.get("DATABRICKS", "TOKEN", fallback="")

if not host or host.startswith("https://<"):
    print("\nERROR: You have not updated HOST in application.properties.")
    print("Replace:  HOST=https://<your-workspace-url>.azuredatabricks.net")
    print("With:     HOST=https://adb-1234567890123456.7.azuredatabricks.net  (your real URL)")
    exit(1)

if not token or token.startswith("dapi<"):
    print("\nERROR: You have not updated TOKEN in application.properties.")
    print("Replace:  TOKEN=dapi<your-personal-access-token>")
    print("With:     TOKEN=dapi...  (your real token from Databricks Settings)")
    exit(1)

# Call the 'who am I' endpoint — works on all Databricks tiers
url = f"{host}/api/2.0/preview/scim/v2/Me"
response = requests.get(url, headers={"Authorization": f"Bearer {token}"})

if response.status_code == 200:
    data = response.json()
    display_name = data.get("displayName", "unknown")
    user_id      = data.get("id", "unknown")
    print(f"\nConnected! Logged in as: {display_name} (user id: {user_id})")
elif response.status_code == 401:
    print("\nERROR: Invalid token. Check TOKEN in application.properties.")
elif response.status_code == 403:
    print("\nERROR: Token is valid but access is denied. Check permissions.")
else:
    print(f"\nERROR: HTTP {response.status_code} - {response.text}")
