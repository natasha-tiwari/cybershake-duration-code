#!/usr/bin/env python3
"""
Query comprehensive site information from CyberShake dataset.
Retrieves all available site data including location, seismic parameters,
and site characteristics (without distance calculations).
"""

import os
import sys
import pandas as pd
import pymysql
from datetime import datetime

def query_all_site_information():
    """
    Query all available site information from CyberShake database.
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
        
        # Query for all site information
        # Note: CyberShake doesn't store explicit topography/geology fields,
        # but Vs30 and basin depths serve as site characterization parameters
        query = """
        SELECT DISTINCT
            cs.CS_Short_Name as Site_ID,
            cs.CS_Site_Name as Site_Long_Name,
            cs.CS_Site_Lat as Latitude,
            cs.CS_Site_Lon as Longitude,
            cr.Target_Vs30 as Thompson_Vs30,
            cr.Model_Vs30 as Model_Vs30,
            cr.Z1_0 as Z1_0_depth_m,
            cr.Z2_5 as Z2_5_depth_m,
            CASE 
                WHEN cr.Target_Vs30 >= 760 THEN 'A/B (Rock)'
                WHEN cr.Target_Vs30 >= 360 THEN 'C (Very Dense Soil/Soft Rock)'
                WHEN cr.Target_Vs30 >= 180 THEN 'D (Stiff Soil)'
                WHEN cr.Target_Vs30 < 180 THEN 'E (Soft Soil)'
                ELSE 'Unknown'
            END as NEHRP_Site_Class,
            CASE
                WHEN cr.Z1_0 < 100 THEN 'Shallow Basin'
                WHEN cr.Z1_0 >= 100 AND cr.Z1_0 < 500 THEN 'Moderate Basin'
                WHEN cr.Z1_0 >= 500 THEN 'Deep Basin'
                ELSE 'Unknown'
            END as Basin_Category,
            s.Study_Name as Study
        FROM 
            CyberShake_Sites cs
            INNER JOIN CyberShake_Runs cr ON cs.CS_Site_ID = cr.Site_ID
            INNER JOIN Studies s ON cr.Study_ID = s.Study_ID
        WHERE 
            s.Study_Name IN ('Study 22.12 LF', 'Study 22.12 BB')
            AND cr.Z1_0 IS NOT NULL 
            AND cr.Z2_5 IS NOT NULL
        ORDER BY 
            cs.CS_Short_Name, s.Study_Name
        """
        
        print("Executing query for all site information...")
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"Retrieved {len(results)} site records")
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Close connection
        cursor.close()
        connection.close()
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def add_site_characteristics(df):
    """
    Add interpreted site characteristics based on available parameters.
    """
    if df is None or df.empty:
        return df
    
    # Add geological interpretation based on Vs30
    def interpret_geology(vs30):
        if vs30 >= 760:
            return "Hard Rock/Bedrock"
        elif vs30 >= 560:
            return "Rock/Soft Rock"
        elif vs30 >= 360:
            return "Very Dense Soil/Soft Rock"
        elif vs30 >= 270:
            return "Dense Soil"
        elif vs30 >= 180:
            return "Stiff Soil"
        else:
            return "Soft Soil"
    
    # Add topographical interpretation based on location and basin depth
    def interpret_topography(z1_0, z2_5):
        if z1_0 < 50:
            return "Likely Mountainous/Elevated"
        elif z1_0 < 200:
            return "Likely Hillside/Moderate Relief"
        elif z1_0 < 500:
            return "Likely Valley/Low Relief"
        else:
            return "Deep Basin/Flat"
    
    df['Interpreted_Geology'] = df['Thompson_Vs30'].apply(interpret_geology)
    df['Interpreted_Topography'] = df.apply(lambda row: interpret_topography(row['Z1_0_depth_m'], row['Z2_5_depth_m']), axis=1)
    
    return df

def compile_site_information():
    """
    Main function to compile all site information.
    """
    print("="*60)
    print("CyberShake Site Information Compilation")
    print("="*60)
    
    # Query all site information
    df = query_all_site_information()
    
    if df is not None and not df.empty:
        # Add interpreted characteristics
        df = add_site_characteristics(df)
        
        # Get unique sites (remove duplicates from different studies)
        unique_sites = df.drop_duplicates(subset=['Site_ID'], keep='first')
        
        # Display summary
        print("\n" + "="*60)
        print("SITE INFORMATION SUMMARY")
        print("="*60)
        print(f"Total unique sites: {len(unique_sites)}")
        print(f"Total records (including different studies): {len(df)}")
        
        # Site class distribution
        print("\nNEHRP Site Class Distribution:")
        class_counts = unique_sites['NEHRP_Site_Class'].value_counts()
        for site_class, count in class_counts.items():
            print(f"  {site_class}: {count} sites ({count/len(unique_sites)*100:.1f}%)")
        
        # Basin category distribution
        print("\nBasin Category Distribution:")
        basin_counts = unique_sites['Basin_Category'].value_counts()
        for category, count in basin_counts.items():
            print(f"  {category}: {count} sites ({count/len(unique_sites)*100:.1f}%)")
        
        # Vs30 statistics
        print("\nVs30 Statistics:")
        print(f"  Minimum: {unique_sites['Thompson_Vs30'].min():.1f} m/s")
        print(f"  Maximum: {unique_sites['Thompson_Vs30'].max():.1f} m/s")
        print(f"  Mean: {unique_sites['Thompson_Vs30'].mean():.1f} m/s")
        print(f"  Median: {unique_sites['Thompson_Vs30'].median():.1f} m/s")
        
        # Geographic extent
        print("\nGeographic Extent:")
        print(f"  Latitude range: {unique_sites['Latitude'].min():.4f} to {unique_sites['Latitude'].max():.4f}")
        print(f"  Longitude range: {unique_sites['Longitude'].min():.4f} to {unique_sites['Longitude'].max():.4f}")
        
        # Save to CSV - unique sites only
        output_file = 'cybershake_all_sites_information.csv'
        
        # Select and order columns for output
        output_columns = [
            'Site_ID', 'Site_Long_Name', 'Latitude', 'Longitude',
            'Thompson_Vs30', 'Model_Vs30',
            'Z1_0_depth_m', 'Z2_5_depth_m',
            'NEHRP_Site_Class', 'Basin_Category',
            'Interpreted_Geology', 'Interpreted_Topography'
        ]
        
        unique_sites[output_columns].to_csv(output_file, index=False, float_format='%.2f')
        print(f"\n✓ Site information saved to: {output_file}")
        
        # Display preview
        print("\n" + "="*60)
        print("PREVIEW: First 10 sites")
        print("="*60)
        preview_cols = ['Site_ID', 'Latitude', 'Longitude', 'Thompson_Vs30', 
                       'NEHRP_Site_Class', 'Basin_Category', 'Interpreted_Geology']
        print(unique_sites[preview_cols].head(10).to_string(index=False))
        
        return output_file
    else:
        print("\nFailed to retrieve site information")
        return None

if __name__ == "__main__":
    output_file = compile_site_information()
    
    if output_file:
        print(f"\n✓ Successfully compiled site information")
        print(f"✓ Output file: {output_file}")
        
        # Note about data interpretation
        print("\n" + "="*60)
        print("IMPORTANT NOTE:")
        print("="*60)
        print("CyberShake database does not contain explicit topography or")
        print("geology fields. The provided interpretations are based on:")
        print("- NEHRP Site Class: Derived from Vs30 values")
        print("- Geology: Interpreted from seismic velocities")
        print("- Topography: Inferred from basin depths")
        print("- These are approximations based on seismic parameters")
        
        # Attempt to open the file
        if sys.platform == "darwin":  # macOS
            os.system(f"open {output_file}")
        elif sys.platform == "win32":  # Windows
            os.system(f"start {output_file}")
        else:  # Linux
            os.system(f"xdg-open {output_file}")