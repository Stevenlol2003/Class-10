import os
import json
import pandas as pd
import matplotlib.pyplot as plt

def read_partition_data(partition_number, month):
    file_path = f'/files/partition-{partition_number}.json'
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            return data.get(month, {}).items()
    except FileNotFoundError:
        return []

def get_most_recent_year_data(partition_number, month):
    data = read_partition_data(partition_number, month)
    most_recent_year = max(data, key=lambda x: int(x[0]))[0] if data else None
    return most_recent_year, data[most_recent_year] if most_recent_year else None

def plot_month_averages():
    month_series = pd.Series()

    partition_numbers = range(4)  # Assuming partition numbers are 0-3

    for partition_number in partition_numbers:
        for month in ['January', 'February', 'March']:
            most_recent_year, month_data = get_most_recent_year_data(partition_number, month)

            if most_recent_year and month_data:
                avg_max_temperature = month_data['avg']
                month_key = f'{month}-{most_recent_year}'
                month_series[month_key] = avg_max_temperature

    if not month_series.empty:
        fig, ax = plt.subplots()
        month_series.plot.bar(ax=ax)
        ax.set_ylabel('Avg. Max Temperature')
        plt.tight_layout()
        plt.savefig("/files/month.svg")
    else:
        print("No data available for plotting.")

if __name__ == "__main__":
    plot_month_averages()

