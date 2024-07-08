from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# Load environment variables from .env file
# This assumes your .env file is in the same directory as your Python script
from dotenv import load_dotenv
load_dotenv()

# Get database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    raise ValueError("No DATABASE_URL set in environment variables")

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a sessionmaker to create database sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
