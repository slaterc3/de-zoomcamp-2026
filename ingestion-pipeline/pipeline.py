import sys
import pandas as pd
from sqlalchemy import create_engine

print(f"Job started. Arguments received: {sys.argv}")

# We will pass connection details via args later, 
# for now, let's just prove we have the libraries installed.
try:
    print("Pandas version:", pd.__version__)
    import psycopg2
    print("Psycopg2 is installed and ready.")
    print("SQLAlchemy is ready.")
    print("SUCCESS: Environment is set up correctly.")
except ImportError as e:
    print(f"FAILURE: Missing dependency - {e}")