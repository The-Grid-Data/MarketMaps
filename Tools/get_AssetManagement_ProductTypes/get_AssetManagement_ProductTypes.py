import requests
import csv
import os
import zipfile
from urllib.parse import urlparse

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

def download_logo(logo_url):
    if not logo_url:
        return None
    try:
        response = requests.get(logo_url)
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to download logo from {logo_url}")
            return None
    except Exception as e:
        print(f"Error downloading logo from {logo_url}: {str(e)}")
        return None

def process_data(profiles):
    logos = {}
    csv_data = []
    for profile in profiles:
        try:
            profile_name = profile.get('name', 'Unknown')
            profile_id = profile.get('id', 'Unknown')
            tag_line = profile.get('tagLine', '')
            short_description = profile.get('descriptionShort', '')
            logo_url = profile.get('logo', '')
            profile_status = profile.get('profileStatus', {}).get('name', 'Unknown')
            sector = profile.get('profileSector', {}).get('name', 'Uncategorized')
            product_type = "N/A"

            products = profile.get('root', {}).get('products', [])
            if products and isinstance(products, list):
                main_product = next((p for p in products if p.get('isMainProduct')), None)
                if main_product:
                    product_type = main_product.get('productType', {}).get('name', 'N/A')

            twitter_handle = ""
            twitter_url = ""
            socials = profile.get('root', {}).get('socials', [])
            if socials and isinstance(socials, list):
                twitter = socials[0]
                if twitter:
                    twitter_handle = twitter.get("name", "")
                    twitter_urls = twitter.get("urls", [])
                    if twitter_urls and isinstance(twitter_urls, list):
                        twitter_url = twitter_urls[0].get("url", "")

            logo_content = download_logo(logo_url)
            if logo_content:
                parsed_url = urlparse(logo_url)
                ext = os.path.splitext(parsed_url.path)[1] or ".jpg"
                safe_name = "".join([c if c.isalnum() else "_" for c in profile_name])
                logo_filename = f"{safe_name}_{profile_id}{ext}"
                logos[f"{sector}/{product_type}/{logo_filename}"] = logo_content

            csv_data.append({
                "name": profile_name,
                "gridid": profile_id,
                "tagLine": tag_line,
                "descriptionShort": short_description,
                "sector": sector,
                "status_name": profile_status,
                "product_type": product_type,
                "has_main_product": "Yes" if products else "No",
                "logo_url": logo_url,
                "Twitter handle": twitter_handle,
                "Twitter URL": twitter_url,
            })
        except Exception as e:
            print(f"Error processing profile {profile.get('id', 'Unknown ID')}: {e}")
    return logos, csv_data


def write_csv(data, output_folder="output", filename="profiles.csv"):
    os.makedirs(output_folder, exist_ok=True)
    csv_path = os.path.join(output_folder, filename)
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    return csv_path

def save_logos(logos, output_folder="output/logos"):
    for path, content in logos.items():
        full_path = os.path.join(output_folder, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as logo_file:
            logo_file.write(content)

def create_zip(output_folder="output", zip_name="output.zip"):
    zip_path = os.path.join(output_folder, zip_name)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(output_folder):
            for file in files:
                if file != zip_name:
                    zipf.write(
                        os.path.join(root, file),
                        os.path.relpath(os.path.join(root, file), output_folder),
                    )

if __name__ == "__main__":
    profiles = fetch_graphql_data()
    logos, csv_data = process_data(profiles)
    csv_path = write_csv(csv_data)
    save_logos(logos)
    create_zip()
