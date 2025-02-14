import os

from dotenv import load_dotenv


load_dotenv(override=True)


TIME_SLEEP_SECS = float(os.environ.get('TIME_SLEEP_SECS') or '0.5')