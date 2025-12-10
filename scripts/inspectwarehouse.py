import pandas as pd
import os
from pathlib import Path

def inspect_warehouse_csvs():
    """
    Inspect all CSV files in the warehouse directory and display 
    column names and data types in an easy-to-copy format.
    """
    
    # Get the warehouse directory path
    script_dir = Path(__file__).parent
    warehouse_dir = script_dir.parent / 'warehouse'
    
    # Find all CSV files in warehouse directory
    csv_files = list(warehouse_dir.glob('*.csv'))
    
    if not csv_files:
        print("No CSV files found in warehouse directory")
        return
    
    print("="*80)
    print("WAREHOUSE CSV INSPECTION REPORT")
    print("="*80)
    print()
    
    for csv_file in sorted(csv_files):
        print(f"📁 FILE: {csv_file.name}")
        print("-" * 60)
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
            print()
            print("COLUMN NAME".ljust(30) + "DATA TYPE")
            print("-" * 50)
            
            # Display column names and data types
            for col in df.columns:
                dtype = str(df[col].dtype)
                print(f"{col:<30} {dtype}")
            
            print()
            
        except Exception as e:
            print(f"❌ Error reading {csv_file.name}: {str(e)}")
            print()
    
    print("="*80)
    print("COPY-PASTE SUMMARY")
    print("="*80)
    
    # Create a copy-paste friendly summary
    for csv_file in sorted(csv_files):
        try:
            df = pd.read_csv(csv_file)
            print(f"\n{csv_file.name}:")
            for col in df.columns:
                dtype = str(df[col].dtype)
                print(f"  {col} ({dtype})")
        except Exception as e:
            print(f"{csv_file.name}: Error - {str(e)}")

if __name__ == "__main__":
    inspect_warehouse_csvs()
