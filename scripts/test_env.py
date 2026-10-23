
import os
import sys

with open('test_env.log', 'w') as f:
    f.write("Starting test_env...\n")
    try:
        from dotenv import load_dotenv
        f.write("dotenv imported\n")
        load_dotenv()
        f.write("dotenv loaded\n")
        f.write(f"DATABASE_URL: {os.environ.get('DATABASE_URL')}\n")
    except Exception as e:
        f.write(f"Error: {e}\n")
