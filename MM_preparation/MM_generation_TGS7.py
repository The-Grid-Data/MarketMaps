import requests
import json
import traceback

from MM_preparation.data_processor import process_data
from MM_preparation.helpers import generate_results_content, generate_csv_content, create_zip_file, create_sector_based_output, filter_by_sector

url = "https://beta.node.thegrid.id/graphql"

query = """
query GetLogosForMM {
  profileInfos(
    where: {
      _and: [
        {
          _or: [
            {
              root: {
                assets: {
                  assetDeployments: {
                    smartContractDeployment: {
                      deployedOnProduct: {
                        id: {_eq: "22"}
                      }
                    }
                  }
                }
              }
            },
            {
              root: {
                products: {
                  _or: [
                    {
                      productDeployments: {
                        smartContractDeployment: {
                          deployedOnProduct: {
                            id: {_eq: "22"}
                          }
                        }
                      }
                    },
                    {
                      supportsProducts: {
                        supportsProduct: {
                          id: {_eq: "22"}
                        }
                      }
                    }
                  ]
                }
              }
            }
          ]
        },
        {
          _or: [
            { profileStatusId: {_eq: 1} },
            { profileStatusId: {_eq: 2} },
            { profileStatusId: {_eq: 30} }
          ]
        }
      ]
    }
  ) {
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
      socials(where: {name: {}, socialType: {name: {_eq: "Twitter / X"}}}) {
        name
        urls {
          url
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
    generation_mode = input("Choose generation mode ('General' or 'Sector'): ").strip().lower()

    try:
        data = fetch_data(url, query)
        tree, skipped_items, logos, results, csv_data, sector_counts = process_data(data)

        if generation_mode == "general":
            results_content = generate_results_content(tree, results, skipped_items, len(logos), sector_counts)
            csv_content = generate_csv_content(csv_data)
            zip_filename = create_zip_file(logos, results_content, csv_content, version)
        elif generation_mode == "sector":
            print("\nAvailable sectors:")
            available_sectors = list(sector_counts.keys())
            for idx, sector in enumerate(available_sectors, 1):
                print(f"{idx}. {sector}")
            sector_choice = int(input("\nEnter the number corresponding to your chosen sector: ").strip())

            if 1 <= sector_choice <= len(available_sectors):
                specific_sector = available_sectors[sector_choice - 1]
                print(f"\nYou selected: {specific_sector}")
                filtered_tree, filtered_data, filtered_logos, filtered_results = filter_by_sector(tree, csv_data, logos, results, specific_sector)
                results_content = generate_results_content(filtered_tree, filtered_results, skipped_items, len(filtered_logos), {specific_sector: len(filtered_results)})
                csv_content = generate_csv_content(filtered_data)
                zip_filename = create_sector_based_output(filtered_logos, results_content, csv_content, filtered_tree, version, specific_sector)
            else:
                print("Invalid choice. Exiting.")
                return
        else:
            print("Invalid generation mode. Please choose 'general' or 'Sector'.")
            return

        print(f"Export completed successfully. Zip file created: {zip_filename}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
