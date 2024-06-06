import os
from dotenv import load_dotenv

def load_env_variables():
    """
    Load environment variables from a .env file.
    """
    directory = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(directory, '../.env')
    load_dotenv(dotenv_path)
