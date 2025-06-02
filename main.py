import tabula
import pandas as pd
import os


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

    # Set password
    password = None

    try:
        # Read first page to get the header
        first_page_df = tabula.read_pdf(
            pdf_path,
            pages=1,  # Only first page
            multiple_tables=False,
            password=password,
            guess=False,  # Disable automatic guessing
            stream=True,
        )

        # Extract headers from the first page
        expected_columns = first_page_df[0].columns.tolist()

        # Read all pages
        dfs = tabula.read_pdf(
            pdf_path,
            pages='all',  # Read all pages
            multiple_tables=True,
            password=password,
            guess=False,  # Disable automatic guessing
            stream=True,
            encoding='utf-8'
        )

        # Combine all DataFrames without filtering
        if not dfs:
            print("No tables found in the PDF.")
            return None

        # Combine all DataFrames without filtering for column count
        combined_df = pd.concat(dfs, ignore_index=True)
        # combined_df = pd

        # Optional: Clean column names (remove any special characters or spaces)
        combined_df.columns = [str(col).strip().replace(' ', '_') for col in combined_df.columns]

        # Remove any completely empty rows
        combined_df = combined_df.dropna(how='all')

        # Just print info about the DataFrame for debugging
        print(f"Combined DataFrame shape: {combined_df.shape}")
        print(f"Columns in combined DataFrame: {combined_df.columns.tolist()}")

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
    pdf_password = 'your_pdf_password'

    # Path for output CSV
    csv_path = os.path.splitext(pdf_path)[0] + '_transactions.csv'

    # Extract transactions
    transactions_df = extract_wallet_balance_data(pdf_path, pdf_password)

    # Save to CSV if extraction was successful
    if transactions_df is not None:
        save_to_csv(transactions_df, csv_path)
    else:
        print("Failed to extract transactions from the PDF.")


if __name__ == "__main__":
    main()