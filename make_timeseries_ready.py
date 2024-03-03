import os
import pandas as pd

output_directory = 'export'
ls_ts_csvfiles = os.listdir(output_directory)

print(f'{len(ls_ts_csvfiles)} files found !')
# List to store individual dataframes from CSV files
dfs = []

# Loop through each CSV file
for ts_csvfile in ls_ts_csvfiles:
    filepath = os.path.join(output_directory, ts_csvfile)
    df = pd.read_csv(filepath)
    if df.shape[0] !=1440:
        print(f"{ticker} less than 1440 min !")
    dfs.append(df)

# Combine the dataframes into a single dataframe
combined_df = pd.concat(dfs)

# Reset the index of the combined dataframe
combined_df.reset_index(drop=True, inplace=True)

combined_df.to_csv('combined_df2023.csv')


##############################
combined_df = pd.read_csv('combined_df2023.csv')

combined_df =combined_df[['c']]

from tslearn.preprocessing import TimeSeriesScalerMeanVariance

# Extract the 'Close' column as our time series data
time_series_data = combined_df['c'].to_numpy()

# Reshape the data to have the shape (n_samples, n_timesteps, n_features)
time_series_data = time_series_data.reshape(n_ts, n_timesteps, 1)

# Normalize the data
scaler = TimeSeriesScalerMeanVariance(mu=0., std=1.)
time_series_data = scaler.fit_transform(time_series_data)

# kShape clustering
from tslearn.clustering import KShape
import matplotlib.pyplot as plt
import numpy

seed = 0
numpy.random.seed(seed)
n_clusters=3
ks = KShape(n_clusters=n_clusters, verbose=True, random_state=seed , max_iter=100,n_init=1)
y_pred = ks.fit_predict(time_series_data)

plt.figure()
for yi in range(n_clusters):
    plt.subplot(n_clusters, 1, 1 + yi)
    for xx in time_series_data[y_pred == yi]:
        plt.plot(xx.ravel(), "k-", alpha=.2)
    plt.plot(ks.cluster_centers_[yi].ravel(), "r-")
    plt.xlim(0, n_timesteps)
    plt.ylim(-4, 4)
    plt.title("Cluster %d" % (yi + 1))

plt.tight_layout()
plt.show()