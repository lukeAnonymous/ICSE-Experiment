import pandas as pd

# Load two CSV files
df1 = pd.read_csv('15000001to15010000_ERC20Transaction.csv')
df2 = pd.read_csv('15000001to15010000_ERC721Transaction.csv')

# Merge two DataFrames
df = pd.concat([df1, df2])

# Filter data for block numbers between 15000000 and 15001500
df_filtered = df[(df['blockNumber'] >= 15000000) & (df['blockNumber'] <= 15010000)]

# Initialize an empty list to store processed data
processed_data = []

# Group the data by block number and transaction hash for processing
for (blockNumber, transactionHash), group in df_filtered.groupby(['blockNumber', 'transactionHash']):
    # Initialize index starting from 1 for each unique transaction
    index = 0
    # Iterate over each row in the group
    for _, row in group.iterrows():
        # Get 'from' and 'to' and append transaction hash and index to them
        from_extended = f"{transactionHash}_{row['from']}_{row['tokenAddress']}"
        to_extended = f"{transactionHash}_{row['to']}_{row['tokenAddress']}"
        # Append processed data to the list
        processed_data.append([blockNumber, transactionHash, from_extended, to_extended])
        # Increment index for each record of the same transaction
        index += 1

# Convert processed data to a DataFrame
processed_df = pd.DataFrame(processed_data, columns=['Block Number', 'Transaction Hash', 'From', 'To'])

# Save the processed DataFrame to a new CSV file
processed_df.to_csv('vessel.csv', index=False)
