import base64
from datetime import datetime, timedelta
import pandas as pd
import requests
import triumpy as tp

class AccrualsProcessor:
    BASE_PATH = r"C:\Users\stephan.ledesma\Scripts\Acrruals\Logs"
    API_KEY = "l7b32a64bb76ff4f2882f9398ff1c11cf8"
    CLIENT_SECRET = "24bd2f3fb6bc4653906700e8a53fc33f"
    AUTH_URL = "https://apiservices.ntrs.com/auth/oauth/v2/token"
    DATA_URL = "https://apiservices.ntrs.com/ent/fundaccounting/v1/transactions"
    OUTPUT_ERROR_FILE = r"C:\Users\stephan.ledesma\Scripts\Acrruals\Error_Log.csv"

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

    def fetch_data(self, date: str) -> pd.DataFrame:
        params = {
            'fromDate': date,
            'summaryDetail': 'S',
            'dateType': 'P'
        }
        headers = {
            'Authorization': f'Bearer {self.jwt}',
            'X-NT-API-Key': self.API_KEY,
            'Accept': 'application/json'
        }
        
        print(f"Requesting data for {date} from API...")
        response = requests.post(self.DATA_URL, json=params, headers=headers, timeout=90)
        
        if response.ok:
            print(f"Data retrieval successful for {date}.")
            raw_data = response.json()
            df = pd.json_normalize(raw_data)
            df["Upload_Date"] = datetime.today().strftime('%Y-%m-%d')
            return df
        else:
            print(f"Data request failed for {date} with status code {response.status_code}")
            print("Response:", response.text)
            raise Exception(f"Failed to retrieve data for {date} (Status code: {response.status_code})")

    def save_data_to_excel(self, df: pd.DataFrame, date: str):
        output_path = f"C:\\Users\\stephan.ledesma\\Scripts\\Acrruals\\output\\NT_ACCRUALS_{date}.csv"
        df.to_csv(output_path, index=False)
        print(f"Raw data for {date} saved to {output_path}")

    def insert_data_into_snowflake(self, df: pd.DataFrame, formatted_date: str):
        try:
            tp.snow.insert(df, 'TR_TEST', 'NT', 'ACCRUELS', username='SLEDESMA', warehouse='COMPUTE_WH')
            print(f"Successfully inserted data for {formatted_date} into Snowflake.")
        except Exception as e:
            print(f"Error inserting data into Snowflake for {formatted_date}: {e}")

    def log_error_to_excel(self):
        if self.errors:
            error_df = pd.DataFrame(self.errors, columns=["Date", "Error"])
            error_df.to_csv(self.OUTPUT_ERROR_FILE, index=False)
            print(f"Errors logged to {self.OUTPUT_ERROR_FILE}")

    def process_dates(self):
        one_year_ago = self.current_date - timedelta(days=0)
        date = self.current_date
        
        while date >= one_year_ago:
            date_str = date.strftime('%Y-%m-%d')
            try:
                df = self.fetch_data(date_str)
                self.save_data_to_excel(df, date_str)
                self.insert_data_into_snowflake(df, date_str)
            except Exception as e:
                print(f"Error processing date {date_str}: {e}")
                self.errors.append({"Date": date_str, "Error": str(e)})
            
            date -= timedelta(days=1)
        
        self.log_error_to_excel()

if __name__ == "__main__":
    processor = AccrualsProcessor()
    processor.process_dates()