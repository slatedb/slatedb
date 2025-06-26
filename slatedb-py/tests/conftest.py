import pytest
import tempfile
import shutil
import os
from slatedb import SlateDB

@pytest.fixture
def temp_dir():
    """Create a temporary directory for database files."""
    dir_path = tempfile.mkdtemp()
    yield dir_path
    shutil.rmtree(dir_path)

@pytest.fixture
def db_path(temp_dir):
    """Create a path for the database."""
    return os.path.join(temp_dir, "test_db")

@pytest.fixture
def env_file(temp_dir):
    env_file = os.path.join(temp_dir, ".env")
    with open(env_file, "w") as f: 
        f.write("CLOUD_PROVIDER=local\n")
        f.write("LOCAL_PATH=/\n")
    yield env_file

@pytest.fixture
def db(db_path):
    """Create a SlateDB instance."""
    db = SlateDB(db_path)
    yield db
    try:
        db.close()
    except Exception:
        pass  # Ignore errors during cleanup 