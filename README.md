# MarketMaps

# Overview

The MM Data Export Tool is a Python repository designed to query, process, and export profile data from The Grid GraphQL API. 

# Goals

Fetch data about Solana profiles, their logos, and related information from a GraphQL endpoint.
Process and categorize profiles based on sectors and product types.
Generate outputs in CSV and summary formats for: 
- General (all data)
- Sector-specific data

# Modules

MM_GENERATION_TGS7.py 
- The main script to execute the tool. 
- Handles user input, fetches data, processes it, and triggers output generation.

data_processor.py 
- Processes the raw data retrieved from the API. 
- Manages the organization of profiles and downloading of logos.

helpers.py
- Provides utility functions for: 
- Generating CSV content.
- Creating ZIP archives. 
- Generating summary results and sector-specific outputs. 

# Follow the prompts:

- Enter the version number for the export. 
- Choose between "General" (all data) or "Sector" (specific sector) modes. 
- For Sector mode, select the desired sector from the list.

# Key Features

- The Grid API GraphQL Integration: Fetches profile data dynamically. 
- Data Categorization: Profiles are categorized by sectors and product types. 
- Export Formats: Generates summaries, CSV files, and organized logos. 

# Example usage of the endpoint

```python
import requests
import json

url = "https://beta.node.thegrid.id/graphql"

query = """
query MyQuery {
  profileInfos(limit: 100) {
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
