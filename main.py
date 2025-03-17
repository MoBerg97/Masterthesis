import h5py
import os
import pandas as pd
import matplotlib
matplotlib.use('TkAgg',force=True)
print("Switched to:",matplotlib.get_backend())

# Functions
def read_hdf5_file(file_path):
    # Open the HDF5 file
    with h5py.File(file_path, 'r') as hdf:
        def process_group(name, obj):
            # print(f"Group: {name}")
            if isinstance(obj, h5py.Group):
                for subgroup_name, subgroup in obj.items():
                    process_group(f"{name}/{subgroup_name}", subgroup)  # Recursive call
            elif isinstance(obj, h5py.Dataset):
                # print(f"    Dataset: {name}, Shape: {obj.shape}")

                # Convert to DataFrame if it's a 2D dataset
                if obj.ndim == 2:
                    df = pd.DataFrame(obj[:])
                    # print(f"    DataFrame:\n{df.head()}")

        hdf.visititems(process_group)

# def save_hdf5_file(file_path, selected_datasets):
#     # Open the HDF5 file
#     dataframes = {}
#
#     with h5py.File(file_path, 'r') as hdf:
#         def process_group(name, obj):
#             print(f"Group: {name}")
#             if isinstance(obj, h5py.Group):
#                 for subgroup_name, subgroup in obj.items():
#                     process_group(f"{name}/{subgroup_name}", subgroup)  # Recursive call
#             elif isinstance(obj, h5py.Dataset):
#                 print(f"    Dataset: {name}, Shape: {obj.shape}")
#
#                 # Convert to DataFrame if it's a 2D dataset and if it's in the selected datasets
#                 if obj.ndim == 2 and name in selected_datasets:
#                     df = pd.DataFrame(obj[:])
#                     dataframes[name] = df  # Store DataFrame in the dictionary
#                     print(f"    Stored DataFrame: {name}")
#
#         hdf.visititems(process_group)
#
#     return dataframes

def save_hdf5_file(file_path, selected_datasets):
    # Open the HDF5 file
    dataframes = {}

    with h5py.File(file_path, 'r') as hdf:
        def process_group(name, obj):
            # print(f"Group: {name}")
            if isinstance(obj, h5py.Group):
                for subgroup_name, subgroup in obj.items():
                    process_group(f"{name}/{subgroup_name}", subgroup)  # Recursive call
            elif isinstance(obj, h5py.Dataset):
                # print(f"    Dataset: {name}, Shape: {obj.shape}")

                # Convert to DataFrame if it's a 2D or 1D dataset and if it's in the selected datasets
                if name in selected_datasets:
                    if obj.ndim == 2:
                        df = pd.DataFrame(obj[:])
                        dataframes[name] = df  # Store 2D DataFrame
                        # print(f"    Stored 2D DataFrame: {name}")
                    elif obj.ndim == 1:
                        df = pd.DataFrame(obj[:], columns=[name])  # Store 1D DataFrame
                        dataframes[name] = df
                        # print(f"    Stored 1D DataFrame: {name}")

        hdf.visititems(process_group)

    return dataframes

def set_first_row_as_column_names(dataframes):
    for name, df in dataframes.items():
        if df.shape[0] > 0:  # Ensure there is at least one row
            new_column_names = df.iloc[0]  # Get the first row
            df.columns = new_column_names  # Set the first row as column names
            df = df[1:]  # Remove the first row from the DataFrame
            dataframes[name] = df  # Update the DataFrame in the dictionary
            print(f"    Updated DataFrame: {name} with new column names from the first row.")
    return dataframes

def replicate_rows(df):
    #TODO: replicate each value before the next, not the whole set
    """Replicate each row in the DataFrame 24 times."""
    # Use `pd.concat` to repeat each row 24 times
    replicated_df = pd.concat([df] * 24, ignore_index=True)
    return replicated_df

def replicate_selected_dfs(dataframes, row_count_threshold):
    """Replicate values in DataFrames that have a certain number of rows."""
    replicated_dfs = {}

    for name, df in dataframes.items():
        if len(df) == row_count_threshold:
            print(f"Replicating DataFrame: {name} with {row_count_threshold} rows.")
            replicated_dfs[name] = replicate_rows(df)
        else:
            print(f"Skipping DataFrame: {name} with {len(df)} rows.")

    return replicated_dfs

def get_folder_file_paths(folder_path):
    folder_paths = []
    for root, dirs, files in os.walk(folder_path):
        folder_paths.append(root)
    return folder_paths

def get_folders_with_dat_not_csv(folder_path):
    folders_with_dat_not_csv = []
    for root, dirs, files in os.walk(folder_path):
        has_dat = False
        has_csv = False
        for file in files:
            if file.endswith(".dat"):
                has_dat = True
            elif file.endswith(".csv"):
                has_csv = True
        if has_dat and not has_csv:
            folders_with_dat_not_csv.append(root)
    return folders_with_dat_not_csv
# region script

<<<<<<< HEAD
# run over all folders
file_paths = get_folder_file_paths("C:\\Users\\user3\\Documents\\xAquaticRisk-Analysis\\data")
ready_file_paths = get_folders_with_dat_not_csv("C:\\Users\\morit\\Desktop\\MA\\xAquaticRisk-Analysis\\data\\Gw150Dr120")

for file_path in ready_file_paths:
    print(f"Processing folder: {file_path}")
    # Specify the arr.dat
    # file_path = r"C:\Users\user3\Documents\xAquaticRisk-Analysis\data\Ter-SD-PEARL-CASCADE-2010-2011" + "/"
    # file_path.replace("\\","/")
    file_name = f'{file_path}/arr.dat'

    # Convert the wanted datasets to pandas dataframes
    selected_datasets = ['SprayDrift/Exposure',
                         'SprayDepositionToReach/Deposition',
                         'DrainageToReach/LineicMassLoadingDrainage',
                         'Drainage/MasDraWatLay',
                         'CascadeToxswa/ConLiqWatTgtAvgHrAvg',
                         'CascadeToxswa/ConLiqWatTgtAvg'
                         'CascadeToxswa/CntSedTgt1',
                         'Weather/PRECIPITATION',
                         'Hydrology/Volume',
                         'PPPSelection/SelectedPPP',
                         'PPPSelection/ApplicationDates',
                         'PPPSelection/ApplicationRates',
                         'PPPSelection/AppliedFields'           # Todo: Load more data if needed
                          ]
    dfs = save_hdf5_file(file_name, selected_datasets)
=======
# Specify the arr.dat
file_path = r"C:\Users\user3\Documents\xAquaticRisk-Analysis\data\Ter-SD-PEARL-CASCADE-2010-2011" + "/"
file_path.replace("\\","/")
file_name = f'{file_path}arr.dat'

# Convert the wanted datasets to pandas dataframes
selected_datasets = ['SprayDepositionToReach/Deposition',
                     'DrainageToReach/LineicMassLoadingDrainage',
                     'Drainage/MasDraWatLay',
                     'CascadeToxswa/ConLiqWatTgtAvgHrAvg',
                     'CascadeToxswa/ConLiqWatTgtAvg'
                     'CascadeToxswa/CntSedTgt1',
                     'Weather/PRECIPITATION',
                     'Hydrology/Volume',
                     'PPPSelection/SelectedPPP',
                     'PPPSelection/ApplicationDates',
                     'PPPSelection/ApplicationRates',
                     'PPPSelection/AppliedFields'           # Todo: Load more data if needed
                      ]
dfs = save_hdf5_file(file_name, selected_datasets)
>>>>>>> master


    # Adjusting the per-day datasets to match the per-hour datasets
    # count_of_days = 730
    # adjusted_dfs = replicate_selected_dfs(dfs, count_of_days)
    #
    # for key, df in adjusted_dfs.items():
    #     dfs[key] = df

    # Create a date+hour range for plotting and extract substance name
    with h5py.File(file_name, 'r') as hdf:
        SimStart = hdf['xCropProtection/SimulationStart'][()].decode('utf-8')
        SimEnd = hdf['xCropProtection/SimulationEnd'][()].decode('utf-8')
        PPP = hdf['PPPSelection/SelectedPPP'][()].decode('utf-8')

    SimStart = SimStart + " 00:00:00"
    SimEnd = SimEnd + " 23:00:00"
    date_range = pd.date_range(start=SimStart, end=SimEnd, freq='h')

    # Todo: retrieve scales from h5file and automatically add a date_range accordingly
    for key in dfs:
        if len(dfs[key]) == len(date_range):
            dfs[key].index = date_range
        else:
            pass


    for name, df in dfs.items():
        name_nodir = name.replace('/', '_')
        df.to_csv(f"{file_path}/{PPP}_{name_nodir}.csv", index=True)

###
### Optional: Plotting
###

### Add date as index to dataframes
## TODO How to add the date index or do manually in plotting function?
## dfs_dated = pd.DataFrame(dfs, index=date_range)
# all x = hour y = reach dataframes matching
# hour_reach_shape = (17520,168)
# matching_dfs = {name: df for name, df in dfs.items() if df.shape == hour_reach_shape}
# for name, df in matching_dfs.items():
#     plt.plot(df.index, df.iloc[:,1], marker='o', label=name)
#
# del matching_dfs['SprayDepositionToReach/Deposition']
#
# selected_reach = 65
# def plot_reach(matching_dfs = matching_dfs, selected_reach=0):
#     colors = ['b', 'r', 'g', 'c', 'm', 'y', 'k']  # Add more colors if needed
#     # Create a figure and the first axis
#     fig, ax1 = plt.subplots()
#     # List to hold the axes for each DataFrame
#     axes = [ax1]
#     # Plot the first DataFrame
#     first_name, first_df = next(iter(matching_dfs.items()))
#     line1, = ax1.plot(first_df.index, first_df.iloc[:, selected_reach], marker='o', color=colors[0], markersize=0.2, label=first_name)
#     ax1.set_ylabel(f'{first_name}')
#     ax1.tick_params(axis='y')
#     # Create a list to hold the legend handles and labels
#     handles = [line1]
#     labels = [first_name]
#     # Loop through the remaining DataFrames
#     for i, (name, df) in enumerate(list(matching_dfs.items())[1:], start=1):
#         ax_new = ax1.twinx()  # Create a new twin axis
#         ax_new.spines['right'].set_position(('outward', 60 * i))  # Move the y-axis outward
#         line_new, = ax_new.plot(df.index, df.iloc[:, selected_reach], marker='o', markersize=0.2, color=colors[i], label=name)
#         ax_new.set_ylabel(f'{name}')
#         ax_new.tick_params(axis='y')
#         # Add the new line to the handles and labels
#         handles.append(line_new)
#         labels.append(name)
#         axes.append(ax_new)  # Store the new axis
#     # Add a title and show the plot
#     plt.title(f'Output for reach {selected_reach} and substance {PPP}')
#     # Create a legend
#     plt.xlabel('Hour')
#     plt.legend(handles, labels, loc='upper left')
#     fig.tight_layout()  # Adjust layout to prevent overlap
#     plt.show()
#
# plot_reach(selected_reach=5)

###
###
###

