import base64
from datetime import datetime, timedelta
import pandas as pd
import requests
import triumpy as tp
import os
from dotenv import load_dotenv

class AccrualsProcessor:
    load_dotenv()

    BASE_PATH = os.getenv("BASE_PATH")
    API_KEY = os.getenv("API_KEY")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    AUTH_URL = os.getenv("AUTH_URL")
    DATA_URL = os.getenv("DATA_URL")
    OUTPUT_ERROR_FILE = os.getenv("OUTPUT_ERROR_FILE")


    def __init__(self):
        self.current_date = datetime.today() - timedelta(days=1)
        self.errors = []
        self.encoded_credentials = self.get_encoded_credentials()
        self.jwt = self.get_auth_token()

    def get_encoded_credentials(self) -> str:
        credentials = f"{self.API_KEY}:{self.CLIENT_SECRET}"
        return base64.b64encode(credentials.encode()).decode()

    def get_auth_token(self) -> str:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + self.encoded_credentials
        }
        data = {"grant_type": "client_credentials"}

        print("Requesting authentication token...")
        response = requests.post(self.AUTH_URL, headers=headers, data=data)

        if response.status_code == 200:
            print("Authentication successful.")
            return response.json().get('access_token')
        else:
            print(f"Auth request failed with status code {response.status_code}")
            print("Response:", response.text)
            raise Exception("Failed to retrieve authentication token")

    def fetch_data(self, date: str, account: str) -> pd.DataFrame:
        params = {
            'fromDate': date,
            'summaryDetail': 'S',
            'dateType': 'P',
            'account': account
        }
        headers = {
            'Authorization': f'Bearer {self.jwt}',
            'X-NT-API-Key': self.API_KEY,
            'Accept': 'application/json'
        }

        print(f"Requesting data for {date} and account {account} from API...")
        response = requests.post(self.DATA_URL, json=params, headers=headers, timeout=90)

        if response.ok:
            print(f"Data retrieval successful for {date}, account {account}.")
            raw_data = response.json()
            df = pd.json_normalize(raw_data)
            df["Upload_Date"] = datetime.today().strftime('%Y-%m-%d')
            df["Account"] = account  
            return df
        else:
            print(f"Data request failed for {date}, account {account} with status code {response.status_code}")
            print("Response:", response.text)
            raise Exception(f"Failed to retrieve data for {date}, account {account} (Status code: {response.status_code})")

    def save_data_to_excel(self, df: pd.DataFrame, date: str, account: str):
        output_path = f"C:\\Users\\stephan.ledesma\\Scripts\\Acrruals\\output\\NT_ACCRUALS_{account}_{date}.csv"
        df.to_csv(output_path, index=False)
        print(f"Raw data for {date}, account {account} saved to {output_path}")

    def insert_data_into_snowflake(self, df: pd.DataFrame, date: str, account: str):
        try:
            tp.snow.insert(df, 'TR_TEST', 'NT', 'ACCRUALS', username='SLEDESMA', update_schema=True, warehouse='COMPUTE_WH')
            print(f"Successfully inserted data for {date}, account {account} into Snowflake.")
        except Exception as e:
            print(f"Error inserting data into Snowflake for {date}, account {account}: {e}")

    def log_error_to_excel(self):
        if self.errors:
            error_df = pd.DataFrame(self.errors, columns=["Date", "Account", "Error"])
            error_df.to_csv(self.OUTPUT_ERROR_FILE, index=False)
            print(f"Errors logged to {self.OUTPUT_ERROR_FILE}")

    def process_dates(self, accounts: list):
        one_year_ago = self.current_date - timedelta(days=1)
        date = self.current_date

        for account in accounts:
            print(f"Processing data for account: {account}")
            date = self.current_date  

            while date >= one_year_ago:
                date_str = date.strftime('%Y-%m-%d')
                try:
                    df = self.fetch_data(date_str, account)
                    self.save_data_to_excel(df, date_str, account)
                    self.insert_data_into_snowflake(df, date_str, account)
                except Exception as e:
                    print(f"Error processing date {date_str}, account {account}: {e}")
                    self.errors.append({"Date": date_str, "Account": account, "Error": str(e)})

                date -= timedelta(days=1)

        self.log_error_to_excel()

if __name__ == "__main__":
    ACCOUNTS_LIST = ["TIR34", "TIR32", "TIR29", "TIR20", "TIR16", "TIR28"]  
    processor = AccrualsProcessor()
    processor.process_dates(ACCOUNTS_LIST)
