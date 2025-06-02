from dateutil import parser
import tabula
import pandas as pd
import os

sample_time_date_str_length = len('2025 Mar 05 20:27:59 05 Mar 2025')
sample_date_str_length = len('25 Jan 2025')
column_names = ["date_time", "date", "description", "debit_credit", "balance", "channel", "reference", "extras"]


# Function to check if a string contains a valid date (YYYY-MM-DD format)
def includes_valid_date(value):
    value = str(value)
    index = sample_time_date_str_length - sample_date_str_length
    possible_date_time = is_valid_date(value[0:index].strip())
    possible_date = is_valid_date(value[0:sample_date_str_length].strip())
    return possible_date or possible_date_time


def get_value_or_empty_string(value):
    if pd.isna(value):
        return ''
    return ''


def is_valid_date(date_string):
    try:
        parser.parse(date_string)
        return True  # Valid date
    except ValueError:
        return False  # Invalid date


def extract_wallet_balance_data(pdf_path, password=None):

    """
    Extract wallet balance data from a PDF statement using Tabula-py
    with a single header applied across all pages

    Args:
        pdf_path (str): Path to the PDF file
        password (str, optional): Password for the PDF

    Returns:
        pandas.DataFrame: DataFrame containing transaction details
    """
    try:
        # Read first page to get the header
        first_page_df = tabula.read_pdf(
            pdf_path,
            pages=1,  # Only first page
            multiple_tables=False,
            password=password,
            guess=True,
            stream=True,
        )

        # Extract headers from the first page
        # expected_columns = first_page_df[0].columns.tolist()

        # Read all pages with the extracted header
        dfs = tabula.read_pdf(
            pdf_path,
            pages='all',  # Read all pages
            multiple_tables=True,
            password=password,
            guess=True,
            stream=True,
            encoding='utf-8'
        )

        # Define the max number of required columns
        num_cols = 8
        column_names = [f'Col_{i + 1}' for i in range(num_cols)]

        normalized_dfs = []
        for df in dfs:
            # Rename existing columns with generic names
            df = df.rename(columns={old: new for old, new in zip(df.columns, column_names)})

            # Ensure exactly 7 columns: add extra columns if needed
            for i in range(num_cols - df.shape[1]):
                df[f'Col_{df.shape[1] + 1}'] = None  # Fill missing columns with NaN

            # Trim excess columns if any
            df = df.iloc[:, :num_cols]

            # Fix rows where Col_1 does not start with a valid date
            for index, row in df.iterrows():
                # If Col_1 doesn't start with YYYY-MM-DD
                if not is_valid_date(str(row['Col_1'])) and len(str(row['Col_1'])) >= sample_time_date_str_length:
                    # print('Col_3', row['Col_3'], type(row['Col_3']))
                    original_value = str(row['Col_1'])
                    if len(original_value) > sample_date_str_length:  # Ensure there's enough to extract
                        last_11_chars = original_value[-sample_date_str_length:]  # Extract last 11 characters
                        df.at[index, 'Col_1'] = original_value[:-sample_date_str_length]  # Remove last 11 chars
                        df.at[index, 'Col_2'] = last_11_chars  # Move extracted part to Col_2

                        # Shift Col_2 to Col_5 one step right (Col_3 to Col_6)
                        for col in range(2, 6):  # Move Col_2 → Col_3, Col_3 → Col_4, etc.
                            df.at[index, f'Col_{col + 1}'] = row[f'Col_{col}']

                if not includes_valid_date(row['Col_1']) and (not pd.isna(row['Col_1'])) and row['Col_1'] != '00':
                    # row['Col_1']
                    df.loc[index] = None # Set entire row to None

            for index, row in df.iterrows():
                if not is_valid_date(str(row['Col_2'])) and index > 0 and pd.isna(df.at[index - 1, 'Col_2']) and not pd.isna(row['Col_2']):
                    print('Col_2', row['Col_2'])

                    # Shift Col_2 to Col_3 one step right (Col_3 to Col_4)
                    for col in range(1, 3):
                        df.at[index, f'Col_{col + 1}'] = row[f'Col_{col}']

                    df.at[index, 'Col_1'] = None

            normalized_dfs.append(df)

        combined_df = pd.concat(normalized_dfs, axis=0, ignore_index=True)

        # Identify rows where only Col_1 has a value
        only_col1_mask = combined_df.iloc[:, 1:].isna().all(axis=1)

        # Merge rows where this pattern occurs two rows later
        indexes_to_remove = set()
        for i in range(len(combined_df) - 2):
            if only_col1_mask[i] and only_col1_mask[i + 2]:  # If current and 2nd next row have only Col_1 filled
                combined_df.at[
                    i + 1, 'Col_1'] = f"{get_value_or_empty_string(combined_df.at[i, 'Col_1'])}{get_value_or_empty_string(combined_df.at[i + 2, 'Col_1'])} {get_value_or_empty_string(combined_df.at[i + 1, 'Col_1'])}"
                indexes_to_remove.add(i)  # Remove the intermediate row
                indexes_to_remove.add(i + 2)  # Remove the last row

        # Drop merged rows
        combined_df.drop(index=list(indexes_to_remove), inplace=True)

        # Reset index after dropping rows
        # combined_df.reset_index(drop=True, inplace=True)

        combined_df.rename(columns={old: new for old, new in zip(combined_df.columns, column_names)})
        combined_df.columns = column_names

        # print(combined_df)

        return combined_df

    except Exception as e:
        print(f"Error processing PDF: {e}")
        return None


def save_to_csv(dataframe, csv_path):
    """
    Save DataFrame to a CSV file

    Args:
        dataframe (pandas.DataFrame): DataFrame to save
        csv_path (str): Path to save the CSV file
    """
    # Ensure we have data to write
    if dataframe is None or dataframe.empty:
        print("No data found to write to CSV.")
        return

    # Save to CSV
    dataframe.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"CSV file saved successfully at {csv_path}")


def main():
    # PDF file path
    pdf_path = 'test-assets/wallet_balance_statement.pdf'

    # PDF password (set to None if no password)
    pdf_password = None

    # Path for output CSV
    csv_path = os.path.splitext(pdf_path)[0] + '_transactions_new.csv'

    # Extract transactions
    transactions_df = extract_wallet_balance_data(pdf_path, pdf_password)

    # Save to CSV if extraction was successful
    if transactions_df is not None:
        save_to_csv(transactions_df, csv_path)
    else:
        print("Failed to extract transactions from the PDF.")


if __name__ == "__main__":
    main()