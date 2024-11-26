import pandas as pd

csv1_file = 'Files/mm_tgs5_solana.csv'
csv2_file = 'Files/mm_tgs7_solana.csv'

csv1 = pd.read_csv(csv1_file)
csv2 = pd.read_csv(csv2_file)

names_in_csv1 = set(csv1['name'])
names_in_csv2 = set(csv2['name'])

names_only_in_csv1 = names_in_csv1 - names_in_csv2

output_file = 'Results/results.csv'
pd.DataFrame({'name': list(names_only_in_csv1)}).to_csv(output_file, index=False)

print(f"Names only in the first CSV have been saved to '{output_file}'.")
print(f"Number of names only in the first CSV: {len(names_only_in_csv1)}")
