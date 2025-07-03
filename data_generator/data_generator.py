# (Content of data_generator.py as provided previously)
import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

def generate_mock_data(num_files=5, max_rows_per_file=100000, output_dir="input_data"):
    """
    Generates mock user activity CSV files.

    Args:
        num_files (int): Number of CSV files to generate.
        max_rows_per_file (int): Maximum number of rows per file.
        output_dir (str): Directory to save the generated CSV files.
    """
    os.makedirs(output_dir, exist_ok=True)
    print(f"Generating {num_files} mock CSV files in '{output_dir}'...")

    # Pre-generate a pool of common IPs and user IDs for some repetition
    ip_pool = [f"192.168.1.{i}" for i in range(255)] + \
              [f"10.0.0.{i}" for i in range(255)] + \
              [f"172.16.0.{i}" for i in range(255)]
    user_id_pool = [f"user_{i:05d}" for i in range(5000)] # 5000 unique users

    for i in range(num_files):
        num_rows = random.randint(max_rows_per_file // 2, max_rows_per_file)
        file_path = os.path.join(output_dir, f"user_activity_{i+1:03d}.csv")

        # Generate data
        log_ids = [f"log_{random.randint(100000000, 999999999)}" for _ in range(num_rows)]
        user_ids = random.choices(user_id_pool, k=num_rows)
        timestamps = [(datetime.now() - timedelta(minutes=random.randint(1, 100000))).isoformat() for _ in range(num_rows)]
        ip_addresses = random.choices(ip_pool, k=num_rows)
        watch_times = np.random.randint(1, 120, num_rows) # Watch time in minutes

        # Introduce some duplicates for log_id within files
        if num_rows > 100:
            duplicate_indices = random.sample(range(num_rows), min(num_rows // 10, 500))
            for idx in duplicate_indices:
                log_ids[idx] = log_ids[random.randint(0, num_rows - 1)]

        data = pd.DataFrame({
            'log_id': log_ids,
            'user_id': user_ids,
            'timestamp': timestamps,
            'ip_address': ip_addresses,
            'watch_time(min)': watch_times
        })

        # Save to CSV
        data.to_csv(file_path, index=False)
        print(f"Generated {file_path} with {num_rows} rows.")

    print("Mock data generation complete.")

if __name__ == "__main__":
    generate_mock_data(num_files=10, max_rows_per_file=50000)
