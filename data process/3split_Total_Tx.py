import csv
import sys


max_int_size = 2**31 - 1
csv.field_size_limit(max_int_size)

def read_transaction_hashes(csv_file_paths):
    transaction_hashes = set()
    for csv_file_path in csv_file_paths:
        with open(csv_file_path, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                transaction_hashes.add(row['transactionHash'])
    return transaction_hashes

def split_and_save_csv(input_csv_path, output_csv_path_part1, output_csv_path_part2, transaction_hashes):
    with open(input_csv_path, mode='r', newline='') as input_csvfile:
        reader = csv.DictReader(input_csvfile)
        fieldnames = reader.fieldnames

        part1_data = []
        part2_data = []

        for row in reader:
            if row['TxHash'] in transaction_hashes:
                part1_data.append(row)
            else:
                part2_data.append(row)

        # Save the first part of the data
        with open(output_csv_path_part1, mode='w', newline='') as output_csvfile_part1:
            writer = csv.DictWriter(output_csvfile_part1, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(part1_data)

        # Save the second part of the data
        with open(output_csv_path_part2, mode='w', newline='') as output_csvfile_part2:
            writer = csv.DictWriter(output_csvfile_part2, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(part2_data)

# Path configuration
csv_file_paths_2_and_3 = [
    '15000001to15010000_ERC20Transaction.csv',
    '15000001to15010000_ERC721Transaction.csv'
]
input_csv_path_1 = 'transactions.csv'
output_csv_path_part1 = 'transactions_token.csv'
output_csv_path_part2 = 'transactions_without_token.csv'

# Merge the transactionHash sets from the second and third CSV files
transaction_hashes = read_transaction_hashes(csv_file_paths_2_and_3)

# Split the first CSV file and save the results
split_and_save_csv(input_csv_path_1, output_csv_path_part1, output_csv_path_part2, transaction_hashes)
