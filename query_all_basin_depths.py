#!/usr/bin/env python3
"""
Query all basin depths from CyberShake dataset using the Data Access Tool.
This script retrieves Z1.0 and Z2.5 values for ALL sites.
"""

import os
import sys
import subprocess
import pandas as pd
import pymysql
from datetime import datetime

def query_all_sites_direct():
    """
    Direct database query to get all sites with basin depths.
    """
    print("Connecting to CyberShake database...")
    
    try:
        # Connect to the database
        connection = pymysql.connect(
            host='moment.usc.edu',
            user='cybershk_ro',
            password='CyberShake2007',
            database='CyberShake',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Successfully connected to CyberShake database")
        
        # Query for all sites with basin depths from the most recent study
        query = """
        SELECT DISTINCT
            cs.CS_Short_Name as Site_ID,
            cs.CS_Site_Name as Site_Long_Name,
            cs.CS_Site_Lat as Latitude,
            cs.CS_Site_Lon as Longitude,
            cr.Target_Vs30 as Thompson_Vs30,
            cr.Model_Vs30 as Model_Vs30,
            cr.Z1_0 as Z1_0,
            cr.Z2_5 as Z2_5,
            s.Study_Name as Study
        FROM 
            CyberShake_Sites cs
            INNER JOIN CyberShake_Runs cr ON cs.CS_Site_ID = cr.Site_ID
            INNER JOIN Studies s ON cr.Study_ID = s.Study_ID
        WHERE 
            s.Study_Name IN ('Study 22.12 LF', 'Study 22.12 BB', 'Study 22.12')
            AND cr.Z1_0 IS NOT NULL 
            AND cr.Z2_5 IS NOT NULL
        ORDER BY 
            cs.CS_Short_Name
        """
        
        print("Executing query for all sites...")
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"Retrieved {len(results)} sites with basin depth data")
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Close connection
        cursor.close()
        connection.close()
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def use_data_access_tool():
    """
    Alternative method using the CyberShake Data Access Tool interactively.
    """
    print("\nAttempting to use CyberShake Data Access Tool...")
    
    # Navigate to cs-data-tools directory
    cs_tools_dir = os.path.join(os.path.dirname(__file__), 'cs-data-tools', 'src')
    retrieve_script = os.path.join(cs_tools_dir, 'retrieve_cs_data.py')
    
    if os.path.exists(retrieve_script):
        print(f"Found retrieve_cs_data.py at: {retrieve_script}")
        
        # Create a simple input script for the interactive tool
        input_commands = """1
Study 22.12 LF
2
0
n
"""
        
        # Write input to temporary file
        with open('input_commands.txt', 'w') as f:
            f.write(input_commands)
        
        # Run the tool with input redirection
        cmd = f"python3 {retrieve_script} < input_commands.txt"
        
        print("Running Data Access Tool...")
        result = os.system(cmd)
        
        if result == 0:
            print("Data Access Tool completed successfully")
            # Look for the output file
            import glob
            csv_files = glob.glob('csdata.*.data.csv')
            if csv_files:
                latest_file = max(csv_files, key=os.path.getctime)
                print(f"Found output file: {latest_file}")
                return pd.read_csv(latest_file)
        else:
            print("Data Access Tool failed")
    
    return None

def compile_basin_depth_data():
    """
    Main function to compile all basin depth data.
    """
    print("="*60)
    print("CyberShake Basin Depth Data Compilation")
    print("="*60)
    
    # Try direct database query first
    df = query_all_sites_direct()
    
    if df is None or df.empty:
        print("\nDirect query failed, trying Data Access Tool...")
        df = use_data_access_tool()
    
    if df is not None and not df.empty:
        # Clean up column names
        df.columns = df.columns.str.replace('Z1_0', 'Z1.0')
        df.columns = df.columns.str.replace('Z2_5', 'Z2.5')
        
        # Sort by Site_ID
        df = df.sort_values('Site_ID')
        
        # Display summary statistics
        print("\n" + "="*60)
        print("BASIN DEPTH SUMMARY STATISTICS")
        print("="*60)
        print(f"Total number of sites: {len(df)}")
        
        print("\nZ1.0 Statistics (depth to Vs=1.0 km/s):")
        print(f"  Minimum: {df['Z1.0'].min():.1f} m")
        print(f"  Maximum: {df['Z1.0'].max():.1f} m")
        print(f"  Mean: {df['Z1.0'].mean():.1f} m")
        print(f"  Median: {df['Z1.0'].median():.1f} m")
        print(f"  Std Dev: {df['Z1.0'].std():.1f} m")
        
        print("\nZ2.5 Statistics (depth to Vs=2.5 km/s):")
        print(f"  Minimum: {df['Z2.5'].min():.1f} m")
        print(f"  Maximum: {df['Z2.5'].max():.1f} m")
        print(f"  Mean: {df['Z2.5'].mean():.1f} m")
        print(f"  Median: {df['Z2.5'].median():.1f} m")
        print(f"  Std Dev: {df['Z2.5'].std():.1f} m")
        
        # Save to CSV
        output_file = 'cybershake_all_sites_basin_depths.csv'
        df.to_csv(output_file, index=False, float_format='%.1f')
        print(f"\n✓ Basin depth data saved to: {output_file}")
        
        # Display first 10 sites as preview
        print("\n" + "="*60)
        print("PREVIEW: First 10 sites")
        print("="*60)
        preview_cols = ['Site_ID', 'Latitude', 'Longitude', 'Z1.0', 'Z2.5']
        if all(col in df.columns for col in preview_cols):
            print(df[preview_cols].head(10).to_string(index=False))
        
        return output_file
    else:
        print("\nFailed to retrieve basin depth data")
        return None

if __name__ == "__main__":
    output_file = compile_basin_depth_data()
    
    if output_file:
        print(f"\n✓ Successfully compiled basin depth data")
        print(f"✓ Output file: {output_file}")
        
        # Attempt to open the file
        if sys.platform == "darwin":  # macOS
            os.system(f"open {output_file}")
        elif sys.platform == "win32":  # Windows
            os.system(f"start {output_file}")
        else:  # Linux
            os.system(f"xdg-open {output_file}")