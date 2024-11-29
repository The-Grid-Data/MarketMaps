# MarketMaps

# Example usage of the endpoint

```python
import requests
import json

url = "https://beta.node.thegrid.id/graphql"

query = """
query MyQuery {
  ProfileInfos(limit: 100) {
    name
    descriptionShort
  }
}
"""

payload = {
    "query": query
}
headers = {
    "Content-Type": "application/json",
}

try:
    response = requests.post(url, headers=headers, json=payload)

    response.raise_for_status()

    data = response.json()

    pretty_data = json.dumps(data, indent=4)
    print(pretty_data)
except requests.exceptions.RequestException as e:
    print("An error occurred:", e)
```
