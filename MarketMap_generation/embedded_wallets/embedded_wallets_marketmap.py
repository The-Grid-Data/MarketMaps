import requests
import csv
import os
import zipfile
import io
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urlparse

GRAPHQL_URL = "https://thegriddev.node.thegrid.id/graphql"
JWT_TOKEN = os.getenv("jwt_dev")

# GraphQL query to get company profile information including logos
COMPANY_QUERY = """
query GetCompanyProfiles($profileIds: [String1!]) {
  profileInfos(where: { id: { _in: $profileIds } }) {
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
      id
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

def execute_graphql_query(
    query: str, variables: dict = None, url: str = None
) -> Optional[Dict[str, Any]]:
    """
    Sends a GraphQL query to the specified URL (or default GRAPHQL_URL).
    Returns the parsed 'data' object (or None on error).
    """
    target_url = url or GRAPHQL_URL
    payload = {"query": query, "variables": variables}
    
    if not JWT_TOKEN:
        print("❌ JWT token not found. Please set the 'jwt_dev' environment variable.")
        return None
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {JWT_TOKEN}",
    }

    try:
        print(f"🔐 Making GraphQL request to: {target_url}")
        print(f"📤 Variables: {variables}")
        
        response = requests.post(target_url, json=payload, headers=headers, timeout=30)
        print(f"📡 Response status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ HTTP Error: {response.status_code}")
            print(f"📄 Response content: {response.text[:500]}")
            return None

        try:
            data = response.json()
            print(f"📊 Response data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            if "errors" in data:
                error_msg = f"GraphQL errors: {data['errors']}"
                print(f"❌ {error_msg}")
                return None

            if "data" not in data:
                print(f"❌ No 'data' field in response: {data}")
                return None
                
            if "profileInfos" not in data["data"]:
                print(f"❌ No 'profileInfos' in data: {data['data'].keys()}")
                return None
                
            profile_count = len(data["data"]["profileInfos"])
            print(f"✅ Found {profile_count} profiles")
            return data.get("data")

        except ValueError as e:
            error_msg = f"Failed to parse JSON response: {e}"
            print(f"❌ {error_msg}")
            print(f"📄 Raw response: {response.text[:500]}")
            return None

    except requests.RequestException as e:
        error_msg = f"GraphQL request failed: {e}"
        print(f"❌ {error_msg}")
        return None

def download_logo(logo_url: str) -> Optional[bytes]:
    """Download logo from URL and return the content as bytes."""
    if not logo_url:
        return None
    try:
        response = requests.get(logo_url, timeout=30)
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to download logo from {logo_url}: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"Error downloading logo from {logo_url}: {str(e)}")
        return None

def process_csv_data(csv_file_path: str) -> Dict[str, list]:
    """Process CSV data and organize companies by segment."""
    companies_by_segment = {}
    total_rows = 0
    skipped_rows = 0
    valid_rows = 0
    
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            total_rows += 1
            company_name = row.get('Match V2', '').strip()
            segment = row.get('Company  Segment', '').strip()
            profile_id = row.get('RootID', '').strip()  # CSV column is "RootID" but contains profile IDs
            
            # Only skip rows that are completely missing profile IDs (these are the only ones we can't query)
            if not profile_id:
                skipped_rows += 1
                print(f"⚠️ Skipping row {total_rows}: Missing profile ID")
                continue
                
            valid_rows += 1
            
            # Use segment if available, otherwise default to "Unknown"
            if not segment:
                segment = "Unknown"
            
            # Use company name if available, otherwise will get it from the profile data
            if not company_name or company_name == '#N/A':
                company_name = None  # Will be filled in from profile data
                
            if segment not in companies_by_segment:
                companies_by_segment[segment] = []
                
            companies_by_segment[segment].append({
                'name': company_name,
                'profile_id': profile_id,
                'segment': segment
            })
    
    print(f"📊 CSV Processing Summary:")
    print(f"   Total rows: {total_rows}")
    print(f"   Valid rows: {valid_rows}")
    print(f"   Skipped rows: {skipped_rows}")
    
    return companies_by_segment

def fetch_company_profiles(companies_by_segment: Dict[str, list]) -> Dict[str, Any]:
    """Fetch company profile information from the GraphQL endpoint."""
    all_profile_ids = []
    for companies in companies_by_segment.values():
        for company in companies:
            all_profile_ids.append(company['profile_id'])
    
    if not all_profile_ids:
        print("❌ No profile IDs found in companies data")
        return {}
    
    print(f"🔍 Found {len(all_profile_ids)} profile IDs to query")
    print(f"📋 Sample profile IDs: {all_profile_ids[:5]}")
    
    # Ensure profile IDs are properly formatted as strings
    original_count = len(all_profile_ids)
    all_profile_ids = [str(pid).strip() for pid in all_profile_ids if pid and str(pid).strip()]
    filtered_count = len(all_profile_ids)
    
    if original_count != filtered_count:
        print(f"⚠️ Filtered out {original_count - filtered_count} invalid profile IDs")
    
    if not all_profile_ids:
        print("❌ No valid profile IDs after filtering")
        return {}
    
    variables = {"profileIds": all_profile_ids}
    print(f"📤 Sending variables: {variables}")
    print(f"📊 Variables type: {type(variables)}")
    print(f"📋 Profile IDs type: {type(variables['profileIds'])}")
    print(f"📊 Sending {len(all_profile_ids)} profile IDs to GraphQL")
    
    result = execute_graphql_query(COMPANY_QUERY, variables)
    
    if not result:
        print("❌ No result returned from GraphQL query")
        return {}
        
    if 'profileInfos' not in result:
        print(f"❌ No 'profileInfos' in result. Available keys: {list(result.keys())}")
        return {}
    
    profile_count = len(result['profileInfos'])
    print(f"✅ Successfully fetched {profile_count} profiles from GraphQL")
    print(f"📊 GraphQL returned {profile_count} profiles out of {len(all_profile_ids)} requested")
    
    if profile_count < len(all_profile_ids):
        print(f"⚠️ {len(all_profile_ids) - profile_count} profile IDs did not return results")
    
    return result

def process_profiles_and_download_logos(profile_data: Dict[str, Any], companies_by_segment: Dict[str, list]) -> Dict[str, Any]:
    """Process profile data and download logos, organizing by segment."""
    logos_by_segment = {}
    company_info_by_segment = {}
    
    # Create a mapping from profile ID to profile data
    profile_id_to_profile = {}
    for profile in profile_data.get('profileInfos', []):
        profile_id = profile.get('id')
        if profile_id:
            profile_id_to_profile[profile_id] = profile
    
    for segment, companies in companies_by_segment.items():
        logos_by_segment[segment] = {}
        company_info_by_segment[segment] = []
        
        for company in companies:
            profile_id = company['profile_id']
            profile = profile_id_to_profile.get(profile_id)
            
            if not profile:
                print(f"No profile found for company with profile ID {profile_id}")
                # Still add to company info even if no profile found
                company_name = company['name'] or f"Unknown_{profile_id}"
                company_info_by_segment[segment].append({
                    'name': company_name,
                    'profile_id': profile_id,
                    'logo_filename': None,
                    'logo_url': None,
                    'tag_line': '',
                    'description': '',
                    'status': 'Profile not found'
                })
                continue
            
            # Get company name from profile data, fallback to CSV name if profile doesn't have it
            company_name = profile.get('name') or company['name'] or f"Unknown_{profile_id}"
            logo_url = profile.get('logo')
            
            # Download logo
            logo_content = None
            if logo_url:
                logo_content = download_logo(logo_url)
            
            # Always add company to company_info_by_segment, regardless of logo status
            if logo_content:
                # Determine file extension from URL
                parsed_url = urlparse(logo_url)
                file_ext = os.path.splitext(parsed_url.path)[1]
                if not file_ext:
                    file_ext = '.png'  # Default extension
                
                # Create safe filename
                safe_filename = "".join([c for c in company_name if c.isalnum() or c in ' -_']).rstrip()
                filename = f"{safe_filename}{file_ext}"
                
                logos_by_segment[segment][filename] = logo_content
                
                company_info_by_segment[segment].append({
                    'name': company_name,
                    'profile_id': profile_id,
                    'logo_filename': filename,
                    'logo_url': logo_url,
                    'tag_line': profile.get('tagLine', ''),
                    'description': profile.get('descriptionShort', ''),
                    'status': profile.get('profileStatus', {}).get('name', 'Unknown') if profile.get('profileStatus') else 'Unknown'
                })
                
                print(f"✓ Downloaded logo for {company_name} ({segment})")
            else:
                company_info_by_segment[segment].append({
                    'name': company_name,
                    'profile_id': profile_id,
                    'logo_filename': None,
                    'logo_url': logo_url,
                    'tag_line': profile.get('tagLine', ''),
                    'description': profile.get('descriptionShort', ''),
                    'status': profile.get('profileStatus', {}).get('name', 'Unknown') if profile.get('profileStatus') else 'Unknown'
                })
                
                print(f"✗ Failed to download logo for {company_name} ({segment})")
    
    return {
        'logos': logos_by_segment,
        'company_info': company_info_by_segment
    }

def sanitize_folder_name(segment: str) -> str:
    """Sanitize segment name for use as folder name by replacing problematic characters."""
    # Replace forward slashes with underscores to avoid nested folder creation
    return segment.replace('/', '_').replace('\\', '_')

def create_zip_file(logos_by_segment: Dict[str, Any], company_info_by_segment: Dict[str, Any], csv_file_path: str) -> str:
    """Create a zip file with logos organized by segment."""
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f'embedded_wallets_marketmap_{current_time}.zip'
    
    print(f"📁 Creating zip with {len(company_info_by_segment)} segments:")
    for segment in company_info_by_segment.keys():
        sanitized = sanitize_folder_name(segment)
        company_count = len(company_info_by_segment[segment])
        logo_count = len(logos_by_segment.get(segment, {}))
        print(f"   {segment} → {sanitized}/ ({company_count} companies, {logo_count} logos)")
    
    # Create zip buffer
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Add logos organized by segment
        for segment, logos in logos_by_segment.items():
            if logos:
                sanitized_segment = sanitize_folder_name(segment)
                for filename, logo_content in logos.items():
                    zip_path = f"{sanitized_segment}/{filename}"
                    zip_file.writestr(zip_path, logo_content)
        
        # Add company information CSV for each segment
        for segment, companies in company_info_by_segment.items():
            if companies:
                sanitized_segment = sanitize_folder_name(segment)
                csv_content = create_segment_csv(companies, segment)
                zip_file.writestr(f"{sanitized_segment}/company_info.csv", csv_content)
        
        # Add summary CSV
        summary_csv = create_summary_csv(company_info_by_segment)
        zip_file.writestr("summary.csv", summary_csv)
        
        # Add original CSV file
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            original_csv_content = f.read()
        zip_file.writestr("original_data.csv", original_csv_content)
    
    # Save zip file
    output_dir = "embedded_wallets/outputs"
    os.makedirs(output_dir, exist_ok=True)
    
    zip_path = os.path.join(output_dir, zip_filename)
    with open(zip_path, 'wb') as f:
        f.write(zip_buffer.getvalue())
    
    print(f"✓ ZIP file created: {zip_path}")
    return zip_path

def create_segment_csv(companies: list, segment: str) -> str:
    """Create CSV content for a specific segment."""
    output = io.StringIO()
    fieldnames = ['name', 'profile_id', 'logo_filename', 'logo_url', 'tag_line', 'description', 'status']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for company in companies:
        writer.writerow(company)
    
    return output.getvalue()

def create_summary_csv(company_info_by_segment: Dict[str, Any]) -> str:
    """Create a summary CSV with all companies across segments."""
    output = io.StringIO()
    fieldnames = ['segment', 'name', 'profile_id', 'logo_filename', 'logo_url', 'tag_line', 'description', 'status']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for segment, companies in company_info_by_segment.items():
        for company in companies:
            row = {'segment': segment, **company}
            writer.writerow(row)
    
    return output.getvalue()

def main():
    """Main function to process CSV and create marketmap zip file."""
    csv_file_path = "Files/embedded_wallets_marketmap.csv"
    
    try:
        print("🚀 Starting Embedded Wallets Marketmap Generation...")
        
        # Step 1: Process CSV data
        print("📊 Processing CSV data...")
        companies_by_segment = process_csv_data(csv_file_path)
        
        if not companies_by_segment:
            print("❌ No valid companies found in CSV")
            return
        
        print(f"📋 Found companies in segments: {list(companies_by_segment.keys())}")
        for segment, companies in companies_by_segment.items():
            print(f"   {segment}: {len(companies)} companies")
        
        # Step 2: Fetch company profiles from GraphQL
        print("\n🔍 Fetching company profiles from GraphQL...")
        profile_data = fetch_company_profiles(companies_by_segment)
        
        if not profile_data:
            print("❌ Failed to fetch company profiles")
            return
        
        # Step 3: Process profiles and download logos
        print("\n🖼️  Processing profiles and downloading logos...")
        processed_data = process_profiles_and_download_logos(profile_data, companies_by_segment)
        
        # Step 4: Create zip file
        print("\n📦 Creating zip file...")
        zip_path = create_zip_file(
            processed_data['logos'], 
            processed_data['company_info'], 
            csv_file_path
        )
        
        # Step 5: Print summary
        print("\n✅ Marketmap generation completed successfully!")
        print(f"📁 Output file: {zip_path}")
        
        total_logos = sum(len(logos) for logos in processed_data['logos'].values())
        print(f"🖼️  Total logos downloaded: {total_logos}")
        
        for segment, companies in processed_data['company_info'].items():
            logos_count = len([c for c in companies if c['logo_filename']])
            print(f"   {segment}: {len(companies)} companies, {logos_count} logos")
        
    except Exception as e:
        error_msg = f"Error in marketmap generation: {str(e)}"
        print(f"❌ {error_msg}")
        raise

if __name__ == "__main__":
    main()
