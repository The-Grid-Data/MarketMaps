import requests
import json
import traceback

from MarketMap_generation.mtndao.data_processor_mtndao import process_data
from MarketMap_generation.mtndao.helpers_mtndao import generate_results_content, generate_csv_content, create_zip_file

url = "https://beta.node.thegrid.id/graphql"

query = """
query GetLogosForMM {
  profileInfos(where: {root: {profileTags: {tag: {name: {_contains: "mtndao"}}}}}) {
    id
    name
    logo
    tagLine
    descriptionShort
    profileStatus {
      name
    }
    profileSector {
      name
    }
    root {
      products {
        id
        name
        isMainProduct
        productType {
          name
        }
      }
      assets {
        id
        name
        assetType {
          name
        }
      }
    }
  }
}
"""

def fetch_data(url, query):

    response = requests.post(url, json={'query': query})
    print(f"HTTP Status Code: {response.status_code}")
    #print("Raw Response Content:", response.text[:500])

    if response.status_code == 200:
        try:
            data = response.json()
            if "data" in data and "profileInfos" in data["data"]:
                return data
            else:
                print("Unexpected response structure:", data)
                raise Exception("Missing 'profileInfos' in response")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {str(e)}")
            print(f"Response content: {response.text}")
            raise
    else:
        print(f"Query failed with status code {response.status_code}")
        print(f"Response content: {response.text}")
        raise Exception(f"Query failed with status code: {response.status_code}")


def main():
    version = input("Please enter the version: ").strip()

    try:
        data = fetch_data(url, query)
        tree, skipped_items, logos, results, csv_data, sector_counts = process_data(data)

        results_content = generate_results_content(tree, results, skipped_items, len(logos), sector_counts)
        csv_content = generate_csv_content(csv_data)
        zip_filename = create_zip_file(logos, results_content, csv_content, version)

        print(f"Export completed successfully. Zip file created: {zip_filename}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
