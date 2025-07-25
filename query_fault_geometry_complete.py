#!/usr/bin/env python3

"""
Query CyberShake database for all available fault geometry information
This script retrieves fault trace coordinates and calculates basic geometry
"""

import pymysql
import pandas as pd
import numpy as np
import os
import sys

def connect_to_database():
    """Establish connection to CyberShake database"""
    try:
        connection = pymysql.connect(
            host='moment.usc.edu',
            user='cybershk_ro',
            password='CyberShake2007',
            database='CyberShake',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def query_fault_geometry(connection):
    """Query all available fault geometry from Ruptures table"""
    
    cursor = connection.cursor()
    
    # Get all unique faults with their trace coordinates
    query = """
    SELECT DISTINCT
        Source_ID as Fault_ID,
        Source_Name as Fault_Name,
        Start_Lat,
        Start_Lon,
        End_Lat,
        End_Lon,
        COUNT(DISTINCT Rupture_ID) as Num_Ruptures,
        MIN(Rupture_ID) as Min_Rupture_ID,
        MAX(Rupture_ID) as Max_Rupture_ID
    FROM Ruptures
    WHERE ERF_ID = 35
    GROUP BY Source_ID, Source_Name, Start_Lat, Start_Lon, End_Lat, End_Lon
    ORDER BY Source_ID
    """
    
    print("Querying fault geometry from CyberShake database...")
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    
    return results

def calculate_fault_parameters(df):
    """Calculate approximate fault length and azimuth from trace coordinates"""
    
    # Calculate fault length using Haversine formula
    R = 6371.0  # Earth's radius in km
    
    lat1 = np.radians(df['Start_Lat'])
    lat2 = np.radians(df['End_Lat'])
    lon1 = np.radians(df['Start_Lon'])
    lon2 = np.radians(df['End_Lon'])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    df['Fault_Length_km'] = R * c
    
    # Calculate azimuth (bearing) from start to end
    y = np.sin(dlon) * np.cos(lat2)
    x = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    
    azimuth_rad = np.arctan2(y, x)
    df['Fault_Azimuth_deg'] = (np.degrees(azimuth_rad) + 360) % 360
    
    return df

def main():
    # Connect to database
    connection = connect_to_database()
    
    # Query fault geometry
    results = query_fault_geometry(connection)
    
    # Convert to DataFrame
    df = pd.DataFrame(results)
    
    # Calculate additional parameters
    df = calculate_fault_parameters(df)
    
    # Add metadata columns
    df['ERF_ID'] = 35
    df['ERF_Name'] = 'UCERF2'
    df['Geometry_Type'] = 'Surface Trace'
    df['Data_Source'] = 'CyberShake Ruptures Table'
    
    # Add notes about missing parameters
    df['Fault_Width_km'] = 'NOT AVAILABLE - Requires UCERF2 source files'
    df['Fault_Dip_deg'] = 'NOT AVAILABLE - Requires UCERF2 source files'
    df['Fault_Rake_deg'] = 'NOT AVAILABLE - Requires UCERF2 source files'
    df['Fault_Area_km2'] = 'NOT AVAILABLE - Requires UCERF2 source files'
    df['Top_Depth_km'] = 'NOT AVAILABLE - Requires UCERF2 source files'
    df['Bottom_Depth_km'] = 'NOT AVAILABLE - Requires UCERF2 source files'
    
    # Save to CSV
    output_file = 'cybershake_fault_geometry_available.csv'
    df.to_csv(output_file, index=False)
    
    print(f"\nFault geometry data saved to: {output_file}")
    print(f"Total faults retrieved: {len(df)}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"  Fault length range: {df['Fault_Length_km'].min():.1f} - {df['Fault_Length_km'].max():.1f} km")
    print(f"  Average fault length: {df['Fault_Length_km'].mean():.1f} km")
    print(f"  Total unique faults: {df['Fault_ID'].nunique()}")
    
    # Create documentation file
    doc_content = """CYBERSHAKE FAULT GEOMETRY DATA DOCUMENTATION
===========================================

AVAILABLE PARAMETERS:
--------------------
1. Fault_ID: Unique fault identifier (Source_ID from database)
2. Fault_Name: Fault name including segment information
3. Start_Lat/Lon: Starting coordinates of fault trace
4. End_Lat/Lon: Ending coordinates of fault trace
5. Fault_Length_km: Calculated from trace endpoints using Haversine formula
6. Fault_Azimuth_deg: Bearing from start to end point (0-360 degrees)
7. Num_Ruptures: Number of rupture scenarios for this fault

PARAMETERS NOT AVAILABLE IN CYBERSHAKE:
--------------------------------------
The following parameters are part of the complete UCERF2 fault model but are NOT 
stored in the CyberShake database:

1. Fault_Width_km: Down-dip width of the fault
2. Fault_Dip_deg: Dip angle of the fault plane
3. Fault_Rake_deg: Rake angle (slip direction)
4. Fault_Area_km2: Total fault area
5. Top_Depth_km: Depth to top of fault
6. Bottom_Depth_km: Depth to bottom of fault
7. Slip_Rate_mm/yr: Long-term slip rate
8. Recurrence_Interval: Average time between events

EXPLANATION OF FAULT_ID CONNECTION:
----------------------------------
- Fault_ID in this dataset corresponds to Source_ID in the CyberShake database
- Each Fault_ID represents a unique fault or fault segment combination
- Fault names follow the pattern: "FaultSystem;Segments"
  Example: "San Andreas;CO+CC+BB+NB+SS+BG+CH" 
  Where CO, CC, etc. are segment abbreviations

NOTES:
------
- CyberShake stores only the surface trace (map view) of faults
- The 3D geometry is used in rupture generation but not exposed via the database
- Fault_Length_km is approximate, calculated from trace endpoints
- Actual fault length may differ due to fault curvature not captured by endpoints
"""
    
    with open('cybershake_fault_geometry_README.txt', 'w') as f:
        f.write(doc_content)
    
    print("\nDocumentation saved to: cybershake_fault_geometry_README.txt")
    
    # Close connection
    connection.close()

if __name__ == "__main__":
    main()
