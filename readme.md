# Bank Statements PDF to CSV Converter

Currently works for OPay and Stanbic IBTC bank statements.

Does not always retrieve all rows depending on how PDF is rendered, but retrieves over 95% of rows.

## How to use

To use the pdf to csv converter, you need to create and store your OPay or Stanbic IBTC bank statement in the root directory as `test-assets/wallet_balance_statement.pdf`.

## Underlying Technology

This project uses [Tabula](https://tabula.technology/) to convert the PDF that can be easily parsed.
