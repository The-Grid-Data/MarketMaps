import pandas as pd

def compare_csvs(file1_path, file2_path, output_path):
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)

    compare_columns = ['name', 'sector', 'product_type']

    df1 = df1[compare_columns].fillna('missing')
    df2 = df2[compare_columns].fillna('missing')

    df1['source'] = 'old'
    df2['source'] = 'new'

    merged = pd.merge(
        df1, df2,
        on=['name'],
        how='outer',
        suffixes=('_old', '_new'),
        indicator=True
    )

    results = []

    for _, row in merged.iterrows():
        old_name = row['name'] if row['_merge'] != 'right_only' else ''
        new_name = row['name'] if row['_merge'] != 'left_only' else ''
        old_sector = row.get('sector_old', '') if row['_merge'] != 'right_only' else ''
        new_sector = row.get('sector_new', '') if row['_merge'] != 'left_only' else ''
        old_product_type = row.get('product_type_old', '') if row['_merge'] != 'right_only' else ''
        new_product_type = row.get('product_type_new', '') if row['_merge'] != 'left_only' else ''

        if row['_merge'] == 'left_only':
            status = 'Removed'
        elif row['_merge'] == 'right_only':
            status = 'Added'
        else:
            if old_sector == new_sector and old_product_type == new_product_type:
                status = 'Same'
            else:
                status = 'Changed'

        results.append({
            'Old Name': old_name,
            'New Name': new_name,
            'Old Sector': old_sector,
            'New Sector': new_sector,
            'Old Product Type': old_product_type,
            'New Product Type': new_product_type,
            'Status': status
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)

file1_path = 'Files/mm_tgs5_DA.csv'
file2_path = 'Files/mm_tgs7_DA.csv'
output_path = 'Results/compared_tgs5_w_tgs7_DA.csv'

compare_csvs(file1_path, file2_path, output_path)
print(f"Comparison completed. Results saved to {output_path}")
