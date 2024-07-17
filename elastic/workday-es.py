import requests
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import time
import signal
import sys

with open('config.json') as config_file:
    config = json.load(config_file)

# Extract and validate configuration details
workday_config = config.get('workday')
log_file_path = config.get('log_file_path')

if not all([workday_config, log_file_path]):
    print("Configuration error: 'workday' and 'log_file_path' must be provided in config.json")
    sys.exit(1)

required_workday_keys = ['rest_api_endpoint', 'token_endpoint', 'client_id', 'client_secret', 'refresh_token']
if not all(key in workday_config for key in required_workday_keys):
    print(f"Configuration error: All of {required_workday_keys} must be provided in 'workday' section of config.json")
    sys.exit(1)

log_handler = TimedRotatingFileHandler(log_file_path, when="midnight", interval=1, backupCount=7)
log_handler.suffix = "%Y-%m-%d"
log_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
logger = logging.getLogger()
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)

# Workday API configuration
rest_api_endpoint = workday_config['rest_api_endpoint']
token_endpoint = workday_config['token_endpoint']
client_id = workday_config['client_id']
client_secret = workday_config['client_secret']
refresh_token = workday_config['refresh_token']

# Signal handling for graceful shutdown
def signal_handler(sig, frame):
    logger.info('Received signal to terminate. Exiting gracefully...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_access_token():
    for attempt in range(3):  # Retry up to 3 times
        try:
            response = requests.post(
                token_endpoint,
                data={
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token,
                    'client_id': client_id,
                    'client_secret': client_secret
                }
            )
            response.raise_for_status()
            logger.info('Successfully obtained access token')
            return response.json()['access_token']
        except requests.exceptions.RequestException as e:
            logger.error(f'Error obtaining access token: {e}')
            if e.response:
                logger.error(f'Response content: {e.response.content}')
            if attempt < 2:
                logger.info('Retrying to obtain access token...')
                time.sleep(10)  # Wait 10 seconds before retrying
    return None

def fetch_workday_data(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    for attempt in range(3):  # Retry up to 3 times
        try:
            response = requests.get(rest_api_endpoint, headers=headers)
            response.raise_for_status()
            logger.info('Successfully fetched data from Workday')
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f'Error fetching data from Workday: {e}')
            if e.response:
                logger.error(f'Response content: {e.response.content}')
            if attempt < 2:
                logger.info('Retrying to fetch data from Workday...')
                time.sleep(10)  # Wait 10 seconds before retrying
    return None

def log_data(data):
    logger.info(json.dumps(data))

def main():
    access_token = get_access_token()
    if access_token:
        data = fetch_workday_data(access_token)
        if data:
            log_data(data)
        else:
            logger.error('No data fetched from Workday')
    else:
        logger.error('Failed to obtain access token')

if __name__ == '__main__':
    while True:
        try:
            main()
        except Exception as e:
            logger.error(f'Unexpected error: {e}')
        time.sleep(3600)  # Wait for 60 minutes before running again
