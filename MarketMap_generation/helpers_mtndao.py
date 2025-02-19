import csv
import os
import zipfile
from datetime import datetime
import io

def generate_csv_content(csv_data):
    output = io.StringIO()
    fieldnames = ['name', 'gridid', 'tagLine', 'descriptionShort', 'sector', 'status_name', 'logo_url', 'Twitter handle', 'Twitter URL']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in csv_data:
        writer.writerow(row)
    return output.getvalue()

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
    for sector, profiles in tree.items():
        content += f"{sector}/ ({len(profiles)} profiles)\n"

    content += "\nProcessed Profiles:\n"
    content += "Name                 ID        Status       Sector                         Logo\n"
    content += "-" * 75 + "\n"
    for result in results:
        name, id_, status_name, sector, logo_success = result
        content += f"{name:<20} {id_:<9} {status_name:<12} {sector:<30} {'✓' if logo_success else '✗'}\n"

    content += "\nSkipped Profiles:\n"
    for item in skipped:
        content += f"- ID: {item['id']}, Name: {item['name']}, Reason: {item['reason']}\n"

    return content

def filter_by_sector(tree, csv_data, logos, results, specific_sector):
    filtered_tree = {specific_sector: tree.get(specific_sector, [])}
    filtered_data = [row for row in csv_data if row['sector'] == specific_sector]
    filtered_logos = {path: content for path, content in logos.items() if path.startswith(f"{specific_sector}/")}
    filtered_results = [result for result in results if result[3] == specific_sector]
    return filtered_tree, filtered_data, filtered_logos, filtered_results

def create_sector_based_output(logos, results_content, csv_content, tree, version, specific_sector):
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f'mm_solana_sector_{specific_sector}_data_v{version}_{current_time}.zip'
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        zip_file.writestr(f'solana_results_v{version}_{current_time}.txt', results_content)
        zip_file.writestr(f'solana_folder_contents_v{version}_{current_time}.csv', csv_content)

        for filepath, content in logos.items():
            if filepath.startswith(f"{specific_sector}/"):
                zip_file.writestr(filepath, content)

    os.makedirs(f'../Outputs/v{version}', exist_ok=True)
    zip_path = os.path.join(f'../Outputs/v{version}', zip_filename)
    with open(zip_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    print(f"Sector-based ZIP file created at: {zip_path}")
    return zip_filename

def create_zip_file(logos, results_content, csv_content, version):
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f'mm_solana_grid_data_v{version}_{current_time}.zip'
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        # Add logos maintaining only sector folder structure
        for filepath, content in logos.items():
            # filepath is already in correct format (sector/filename)
            zip_file.writestr(filepath, content)

        zip_file.writestr(f'solana_results_v{version}_{current_time}.txt', results_content)
        zip_file.writestr(f'solana_folder_contents_v{version}_{current_time}.csv', csv_content)

    os.makedirs(f'../Outputs/v{version}', exist_ok=True)
    zip_path = os.path.join(f'../Outputs/v{version}', zip_filename)
    with open(zip_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    print(f"ZIP file created at: {zip_path}")
    return zip_filename