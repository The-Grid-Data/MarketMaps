import os
from collections import defaultdict
from urllib.parse import urlparse

import requests

def process_data(data):

    profiles = data['data']['profileInfos']
    tree = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  # sector -> product_type -> profiles
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
                logos[f"{sector}/{product_type}/{new_filename}"] = logo_content  # Updated path
            else:
                new_filename = None

            # Add to tree structure
            tree[sector][product_type]['profiles'].append({
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