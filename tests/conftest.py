import os
import sys
import pytest

curr_file_path = globals().get("__file__", sys.argv[0])
curr_dir = os.path.dirname(os.path.abspath(curr_file_path))
proj_root = os.path.abspath(os.path.join(curr_dir, ".."))

sys.path.append(proj_root)

@pytest.fixture(scope = "session")
def spark():
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    
    except ImportError:

        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()

        except ImportError:

            raise ImportError("Couldn't initialize neither of the sessions")
        
    return spark