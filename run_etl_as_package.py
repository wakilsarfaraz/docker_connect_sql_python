# run_etl.py
import logging
import getpass
from etl_pipeline import clear_folder, manage_tables, write_dataframe_to_db
from etl_pipeline import calculate_payments, calculate_duration, calculate_profitable_actors
from etl_pipeline import write_local_txt_output

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler("etl_pipeline.log"),
                            logging.StreamHandler()
                        ])

    server = input("Enter SQL Server address (Hint! starts with tcp and ends with .net): ").strip()
    username = input("Enter Your Username: ").strip()
    password = getpass.getpass("Enter Your Password: ").strip()
    connection_string = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server},1433;"
        f"Database=sakila;"
        f"Uid={username};Pwd={password};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

    clear_folder("reports")
    manage_tables(connection_string)

    payments_df = calculate_payments("sqlFiles/queries/payments.sql", connection_string)
    duration_df = calculate_duration("sqlFiles/queries/filmduration.sql", connection_string)
    profitable_actors_df = calculate_profitable_actors("sqlFiles/queries/profitable_actors.sql", connection_string)

    write_dataframe_to_db(payments_df, "payment_summary_table", connection_string)
    write_dataframe_to_db(duration_df, "duration_summary_table", connection_string)
    write_dataframe_to_db(profitable_actors_df, "profitable_actors_table", connection_string)

    write_local_txt_output(payments_df, "reports", "payment_summary.txt")
    write_local_txt_output(duration_df, "reports", "duration_summary.txt")
    write_local_txt_output(profitable_actors_df, "reports", "profitable_actors.txt")

