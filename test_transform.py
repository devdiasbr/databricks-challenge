
import sys
import os
from pyspark.sql import SparkSession

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.append(src_dir)

from utils.transformations import BaseTransform

def test_chain():
    spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()
    data = [("  abc  ", "DEF", 1)]
    df = spark.createDataFrame(data, ["col1", "col2", "col3"])
    
    transformer = BaseTransform(df)
    
    try:
        df_clean = (transformer
                    .trim_strings()
                    .upper_strings()
                    .drop_duplicates()
                    .treat_nulls()
                    .get_dataframe())
        
        print("Chain successful!")
        df_clean.show()
        
        if df_clean is None:
            print("df_clean is None!")
        else:
            print("df_clean is valid DataFrame")
            
    except Exception as e:
        print(f"Chain failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_chain()
