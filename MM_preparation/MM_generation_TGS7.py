import requests
import json
import os
from urllib.parse import urlparse
import zipfile
import io
from datetime import datetime
from collections import defaultdict
import csv
import traceback

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


def process_data(data):
    profiles = data['data']['profileInfos']
    tree = defaultdict(lambda: defaultdict(list))
    skipped_items = []
    logos = {}
    results = []
    csv_data = []
    sector_counts = defaultdict(int)

    for profile in profiles:
        try:
            profile_name = profile.get('name', 'Unknown')
            profile_id = profile.get('id', 'Unknown')
            tag_line = profile.get('tagLine', '')
            short_description = profile.get('descriptionShort', '')
            logo_url = profile.get('logo')
            profile_status = profile.get('profileStatus', {})
            status_name = profile_status.get('name', 'Unknown')
            sector = profile.get('profileSector', {}).get('name', 'Uncategorized')

            socials = profile.get('root', {}).get('socials', [])
            twitter_handle = ''
            twitter_url = ''
            if socials:
                twitter_entry = socials[0]
                twitter_name = twitter_entry.get('name', '')
                if twitter_name:
                    twitter_handle = f"@{twitter_name}"
                urls = twitter_entry.get('urls', [])
                if urls:
                    twitter_url = urls[0].get('url', '')

            root_data = profile.get('root', {})
            products = root_data.get('products', [])
            has_main_product = False
            product_type = "ASSETS"  # Default to ASSETS if no products exist

            if isinstance(products, list) and products:
                has_main_product = any(product.get('isMainProduct') == 1 for product in products)
                if has_main_product:
                    main_product = next((product for product in products if product.get('isMainProduct') == 1), None)
                    product_type = main_product.get('productType', {}).get('name', 'N/A') if main_product else "N/A"
                else:
                    product_type = products[0].get('productType', {}).get('name', 'N/A')

            # Handle logo download
            logo_content = download_logo(logo_url)
            if logo_content:
                parsed_url = urlparse(logo_url)
                file_ext = os.path.splitext(parsed_url.path)[1]
                safe_filename = "".join([c for c in profile_name if c.isalnum() or c == ' ']).rstrip()
                new_filename = f"{safe_filename}_{profile_id}{file_ext}"
                logos[f"{sector}/{new_filename}"] = logo_content
            else:
                new_filename = None

            # Add to tree structure
            tree[sector]['profiles'].append({
                'id': profile_id,
                'name': profile_name,
                'tagLine': tag_line,
                'descriptionShort': short_description,
                'status': status_name,
                'logo': new_filename,
                'product_type': product_type,
                'has_main_product': "Yes" if has_main_product else "No",
                'twitter_handle': twitter_handle,
                'twitter_url': twitter_url
            })

            # Add to results for summary
            results.append((profile_name, profile_id, status_name, sector, product_type, bool(new_filename)))

            # Add to CSV data
            csv_data.append({
                'name': profile_name,
                'gridid': profile_id,
                'tagLine': tag_line,
                'descriptionShort': short_description,
                'sector': sector,
                'status_name': status_name,
                'product_type': product_type,
                'has_main_product': "Yes" if has_main_product else "No",
                'logo_url': logo_url,
                'Twitter handle': twitter_handle,
                'Twitter URL': twitter_url
            })

            # Update sector counts
            sector_counts[sector] += 1

        except Exception as e:
            skipped_items.append({
                'id': profile.get('id', 'Unknown ID'),
                'name': profile.get('name', 'Unknown Name'),
                'reason': str(e)
            })

    return tree, skipped_items, logos, results, csv_data, sector_counts

def generate_csv_content(csv_data):
    """Generate CSV content from the collected data."""
    output = io.StringIO()
    fieldnames = ['name', 'gridid', 'tagLine', 'descriptionShort', 'sector', 'status_name', 'product_type', 'has_main_product', 'logo_url', 'Twitter handle', 'Twitter URL']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in csv_data:
        writer.writerow(row)
    return output.getvalue()

def create_zip_file(logos, results_content, csv_content, version):
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f'mm_solana_grid_data_v{version}_{current_time}.zip'
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for filepath, content in logos.items():
            zip_file.writestr(filepath, content)

        zip_file.writestr(f'solana_results_v{version}_{current_time}.txt', results_content)
        zip_file.writestr(f'solana_folder_contents_v{version}_{current_time}.csv', csv_content)

    os.makedirs(f'../Outputs/v{version}', exist_ok=True)
    zip_path = os.path.join(f'../Outputs/v{version}', zip_filename)
    with open(zip_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    print(f"ZIP file created at: {zip_path}")
    return zip_filename

def generate_results_content(tree, results, skipped, logo_count, sector_counts):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_profiles = len(results)
    skipped_count = len(skipped)

    content = f"""
GRID DATA EXPORT SUMMARY
========================
Date and Time: {current_time}
Total Profiles Processed: {total_profiles}
Total Logos: {logo_count}
Skipped Profiles: {skipped_count}

Folder Structure:
"""
    for sector, subfolders in tree.items():
        content += f"{sector}/\n"
        for subfolder, profiles in subfolders.items():
            content += f"  {subfolder}/ ({len(profiles)} profiles)\n"

    content += "\nProcessed Profiles:\n"
    content += "Name                 ID        Status       Sector                         ProductType      Logo\n"
    content += "-" * 90 + "\n"
    for result in results:
        name, id_, status_name, sector, product_type, logo_success = result
        content += f"{name:<20} {id_:<9} {status_name:<12} {sector:<30} {product_type:<15} {'✓' if logo_success else '✗'}\n"

    content += "\nSkipped Profiles:\n"
    for item in skipped:
        content += f"- ID: {item['id']}, Name: {item['name']}, Reason: {item['reason']}\n"

    return content


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
                filtered_tree, filtered_data, filtered_logos, filtered_results = filter_by_sector(tree, csv_data, logos,
                                                                                                  results,
                                                                                                  specific_sector)
                results_content = generate_results_content(filtered_tree, filtered_results, skipped_items,
                                                           len(filtered_logos),
                                                           {specific_sector: len(filtered_results)})
                csv_content = generate_csv_content(filtered_data)
                zip_filename = create_sector_based_output(filtered_logos, results_content, csv_content, filtered_tree,
                                                          version)
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


def filter_by_sector(tree, csv_data, logos, results, specific_sector):

    filtered_tree = {specific_sector: tree.get(specific_sector, {})}
    filtered_data = [row for row in csv_data if row['sector'] == specific_sector]
    filtered_logos = {path: content for path, content in logos.items() if path.startswith(specific_sector)}
    filtered_results = [result for result in results if result[3] == specific_sector]
    return filtered_tree, filtered_data, filtered_logos, filtered_results


def create_sector_based_output(logos, results_content, csv_content, tree, version):

    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f'mm_solana_sector_data_v{version}_{current_time}.zip'
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr(f'solana_results_v{version}_{current_time}.txt', results_content)
        zip_file.writestr(f'solana_folder_contents_v{version}_{current_time}.csv', csv_content)

        for filepath, content in logos.items():
            sector_folder, filename = os.path.split(filepath)
            zip_file.writestr(os.path.join(sector_folder, filename), content)

    os.makedirs(f'../Outputs/v{version}', exist_ok=True)
    zip_path = os.path.join(f'../Outputs/v{version}', zip_filename)
    with open(zip_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    print(f"Sector-based ZIP file created at: {zip_path}")
    return zip_filename


if __name__ == "__main__":
    main()
