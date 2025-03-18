from kaggle.api.kaggle_api_extended import KaggleApi as ka
import pandas as pd
from pathlib import Path
import os

def kaggle_connect():
    try:
        # Define base path for dataset downloads
        base_folder = Path("./data")

        # Initialize the API and authenticate
        api = ka()
        api.authenticate()

        # Prompt for the search term
        search_term = input("Search for data (press Enter to skip): ").strip()
        if not search_term:
            print("No search term entered. Exiting...")
            return None

        # Fetch datasets related to the search term
        datasets = api.dataset_list(search=search_term)
        datasets = list(datasets)
        if not datasets:
            print("No datasets found for the search term.")
            return None

        # Display datasets and prompt for user selection
        print("\nDatasets found:")
        for i, dataset in enumerate(datasets):
            print(f"{i + 1}) {dataset.ref} - {dataset.title}")

        try:
            # Get user selection
            option = int(input("\nEnter the number of the dataset to download: "))
            if option < 1 or option > len(datasets):
                print("Invalid selection. Exiting.")
                return None
            data_ref = datasets[option - 1].ref
        except ValueError:
            print("Invalid input. Please enter a valid number.")
            return None

        # Define destination folder for dataset
        new_folder = input("\nEnter the name of the new folder to store the dataset: ").strip()
        if not new_folder:
            print("Folder name cannot be empty. Exiting.")
            return None

        download_path = base_folder / new_folder
        download_path.mkdir(parents=True, exist_ok=True)

        # Download the dataset and unzip it
        print("\nDownloading dataset...")
        api.dataset_download_files(data_ref, path=str(download_path), unzip=True)
        print("Download completed successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def col_name():
    folder_path = "./data"

    # List all files in the folder
    files_and_dirs = os.listdir(folder_path)
    files = [f for f in files_and_dirs if os.path.isfile(os.path.join(folder_path, f))]
    print("Available files:", files)

    # Ask the user to select a CSV file to modify
    file = input("Enter the name of the CSV file to modify (include .csv extension): ").strip()
    if not file in files:
        print("File not found. Exiting.")
        return None

    # Load the CSV file into a DataFrame
    df = pd.read_csv(os.path.join(folder_path, file))

    # Rename columns interactively
    new_columns = []
    print("\nRename columns:")
    for col in df.columns:
        new_name = input(f"Column '{col}' new name (without spaces): ").strip()
        if new_name:
            new_columns.append(new_name)
        else:
            new_columns.append(col)  # Keep the original name if left blank

    df.columns = new_columns

    # Save the modified DataFrame to a new file
    modified_file_path = os.path.join(folder_path, "modified_data.csv")
    df.to_csv(modified_file_path, index=False)
    print(f"\nModified dataset saved as: {modified_file_path}")

if __name__ == "__main__":
    kaggle_connect()
    col_name()