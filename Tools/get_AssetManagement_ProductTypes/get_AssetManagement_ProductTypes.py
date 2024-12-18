import requests
import csv

GRAPHQL_ENDPOINT = "https://beta.node.thegrid.id/graphql"
HEADERS = {
    "Content-Type": "application/json",
}

QUERY = """
query GetFilteredProfiles {
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
                      deployedOnProduct: { id: { _eq: \"22\" } }
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
                          deployedOnProduct: { id: { _eq: \"22\" } }
                        }
                      }
                    },
                    {
                      supportsProducts: {
                        supportsProduct: { id: { _eq: \"22\" } }
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
            { profileStatusId: { _eq: 1 } },
            { profileStatusId: { _eq: 2 } },
            { profileStatusId: { _eq: 30 } }
          ]
        },
        {
          root: {
            products: { productType: { id: { _in: [692, 472, 20, 49, 48] } } }
          }
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
      products(where: { productType: { id: { _in: [692, 472, 20, 49, 48] } } }) {
        id
        name
        isMainProduct
        productType {
          id
          name
        }
      }
      socials(where: { socialType: { name: { _eq: \"Twitter / X\" } } }) {
        name
        urls {
          url
        }
      }
    }
  }
}
"""


def fetch_graphql_data():
    response = requests.post(GRAPHQL_ENDPOINT, json={"query": QUERY}, headers=HEADERS)
    if response.status_code == 200:
        return response.json()["data"]["profileInfos"]
    else:
        print(f"Error {response.status_code}: {response.text}")
        return []


def transform_data(data):
    transformed = []
    for profile in data:
        name = profile.get("name", "")
        gridid = profile.get("id", "")
        tagLine = profile.get("tagLine", "")
        descriptionShort = profile.get("descriptionShort", "")
        sector = profile.get("profileSector", {}).get("name", "")
        status_name = profile.get("profileStatus", {}).get("name", "")
        logo_url = profile.get("logo", "")

        product_type = ""
        has_main_product = "No"
        for product in profile.get("root", {}).get("products", []):
            if product.get("productType"):
                product_type = product["productType"]["name"]
            if product.get("isMainProduct"):
                has_main_product = "Yes"
                break

        twitter_handle = ""
        twitter_url = ""
        socials = profile.get("root", {}).get("socials", [])
        if socials:
            for social in socials:
                if social.get("urls"):
                    twitter_handle = social.get("name", "")
                    twitter_url = social["urls"][0].get("url", "")

        transformed.append({
            "name": name,
            "gridid": gridid,
            "tagLine": tagLine,
            "descriptionShort": descriptionShort,
            "sector": sector,
            "status_name": status_name,
            "product_type": product_type,
            "has_main_product": has_main_product,
            "logo_url": logo_url,
            "Twitter handle": twitter_handle,
            "Twitter URL": twitter_url
        })
    return transformed


def write_to_csv(data, filename="filtered_profiles.csv"):
    if not data:
        print("No data to write to CSV.")
        return

    fieldnames = [
        "name", "gridid", "tagLine", "descriptionShort", "sector", "status_name",
        "product_type", "has_main_product", "logo_url", "Twitter handle", "Twitter URL"
    ]
    with open(filename, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Data written to {filename}")


if __name__ == "__main__":
    raw_data = fetch_graphql_data()
    processed_data = transform_data(raw_data)
    write_to_csv(processed_data)
