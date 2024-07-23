import os
from datetime import datetime

# Define the target folder
target_folder = 'C://Documents/'

# Ensure the target folder exists


# Define the number of dummy files to create
num_files = 100

# Get the current date
current_date = datetime.now().strftime('%Y%m%d')
print(current_date)
# Create the dummy files
for i in range(1, num_files + 1):
    filename = f'test_{current_date}_{i}.txt'
    filepath = target_folder + filename
    with open(filepath, 'w') as file:
        file.write(f'This is dummy file number {i} created on {current_date}.\n')

print(f'{num_files} dummy files created in {target_folder}')
