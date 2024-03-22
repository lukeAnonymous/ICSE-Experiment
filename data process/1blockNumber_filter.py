import csv

def filter_csv(input_file_path, output_file_path):
    # Specify the blockNumber range to filter
    block_number_min = 15000001
    block_number_max = 15010000

    filtered_data = []

    with open(input_file_path, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            block_number = int(row['blockNumber'])
            if block_number_min <= block_number <= block_number_max:
                filtered_data.append(row)

    # Write the filtered data to a new CSV file
    with open(output_file_path, mode='w', newline='') as csvfile:
        if filtered_data:
            fieldnames = filtered_data[0].keys()  # Get field names
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in filtered_data:
                writer.writerow(row)

# Call the function with input and output file paths
filter_csv('15000000to15249999_ERC721Transaction.csv', '15000001to15010000_ERC721Transaction.csv')
