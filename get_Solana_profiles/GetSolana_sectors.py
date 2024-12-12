import requests

url = "https://beta.node.thegrid.id/graphql"

query = """
query GetSolana {
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
    }
  }
}
"""

def fetch_profile_infos(url, query, chosen_sector=None):
    try:
        response = requests.post(url, json={'query': query})
        if response.status_code == 200:
            data = response.json()
            if "data" in data and "profileInfos" in data["data"]:
                profile_infos = data["data"]["profileInfos"]

                if chosen_sector:
                    filtered_profiles = [
                        profile for profile in profile_infos
                        if profile.get('profileSector', {}).get('name', '').lower() == chosen_sector.lower()
                    ]
                    print(f"Number of profileInfos in sector '{chosen_sector}': {len(filtered_profiles)}")
                    return filtered_profiles
                else:
                    print(f"Number of profileInfos retrieved: {len(profile_infos)}")
                    return profile_infos
            else:
                print("Unexpected response structure:", data)
        else:
            print(f"Query failed with status code {response.status_code}")
            print(f"Response content: {response.text}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def main():
    print("Fetching all profiles...")
    profiles = fetch_profile_infos(url, query)
    if not profiles:
        print("No profiles retrieved.")
        return

    print("\nAvailable sectors:")
    sectors = {profile.get('profileSector', {}).get('name', 'Uncategorized') for profile in profiles}
    for idx, sector in enumerate(sorted(sectors), 1):
        print(f"{idx}. {sector}")

    sector_choice = input("\nEnter the number of the sector to filter by (or press Enter to skip): ").strip()
    if sector_choice.isdigit():
        sector_choice = int(sector_choice)
        if 1 <= sector_choice <= len(sectors):
            chosen_sector = sorted(sectors)[sector_choice - 1]
            print(f"\nFiltering profiles by sector: {chosen_sector}")
            fetch_profile_infos(url, query, chosen_sector=chosen_sector)
        else:
            print("Invalid sector choice.")
    else:
        print("\nNo sector filtering applied.")

if __name__ == "__main__":
    main()
