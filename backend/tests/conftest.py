"""
conftest.py — Add the backend directory to sys.path so pytest can import
              utils, parser, scraper, and main without installing them as a package.
"""
import sys
import os

# Insert the backend directory so imports like `from utils import ...` work
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
