import pandas as pd

# Read the first CSV file
csv1_path = 'EVM_ACCESS.csv'  # Path to the first CSV file
csv1 = pd.read_csv(csv1_path)

# Preprocessing: Remove trailing ~ from the InvokeAddress, ReadStateSlot, and WriteStateSlot columns
for column in ['InvokeAddress', 'ReadStateSlot', 'WriteStateSlot']:
    csv1[column] = csv1[column].str.rstrip('~')

# Read the second CSV file
csv2_path = 'EVM_ACCESSExecTime.csv'  # Path to the second CSV file
csv2 = pd.read_csv(csv2_path)

# Merge the two CSV files based on TxHash, keeping all rows from csv1 and adding ExecTime(ns)
merged_csv = pd.merge(csv1, csv2, on='TxHash', how='left')

# Filter data for block numbers between 15000000 and 15000100
filtered_csv = merged_csv[(merged_csv['BlockNumber'] >= 15000000) & (merged_csv['BlockNumber'] <= 15010000)]

# Output the merged data to a new CSV file
output_path = 'transactions.csv'  # Path to save the merged CSV file
filtered_csv.to_csv(output_path, index=False)

print("CSV file merging completed. Output path:", output_path)
