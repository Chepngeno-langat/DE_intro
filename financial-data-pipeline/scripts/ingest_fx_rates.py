import requests
import psycopg2
import os
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

# Config
API_KEY = os.getenv("OER_API_KEY")
API_URL = f"https://openexchangerates.org/api/latest.json?app_id={API_KEY}"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

TARGET_CURRENCIES = ["EUR", "GBP", "KES", "NGN", "ZAR", "JPY", "INR"]

def fetch_fx_rates():
	"""Fetch latest fx rates"""
	
	response = requests.get(API_URL)
	response.raise_for_status()
	data = response.json()
	return data["base"], data["rates"]

def load_to_postgres(base_currency, rates):
	"""Insert raw fx rates into postgreSQL"""
	conn = psycopg2.connect(**DB_CONFIG)
	cursor = conn.cursor()
	today = date.today()
	rows_inserted = 0

	for currency, rate in rates.items():
		if currency in TARGET_CURRENCIES:
			cursor.execute("""
				INSERT INTO raw_fx_rates (base_currency, target_currency, rate, rate_date)
				VALUES (%s, %s, %s, %s)
				ON CONFLICT DO NOTHING
			""", (base_currency, currency, rate, today))
			rows_inserted += 1

	conn.commit()
	cursor.close()
	conn.close()
	print(f"Inserted {rows_inserted} rows for {today}")

def run():
	print("Starting FX rate ingestion")
	base, rates = fetch_fx_rates()
	load_to_postgres(base, rates)
	print("Ingestion Complete")

if __name__ == "__main__":
	run()



































