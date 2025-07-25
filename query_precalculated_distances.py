#!/usr/bin/env python3
"""
Query pre-calculated site-to-fault distances from CyberShake.
This retrieves existing distance data - no calculations are performed.
"""

import pymysql
import pandas as pd
import sys
import os

def query_site_distances():
    """
    Query pre-calculated site-to-fault distances from CyberShake.
    """
    print("Retrieving pre-calculated site-to-fault distances...")
    
    try:
        connection = pymysql.connect(
            host='moment.usc.edu',
            user='cybershk_ro',
            password='CyberShake2007',
            database='CyberShake',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        # First, get the minimum distance to any fault for each site
        query = """
        SELECT 
            cs.CS_Short_Name as Site_ID,
            cs.CS_Site_Name as Site_Name,
            cs.CS_Site_Lat as Latitude,
            cs.CS_Site_Lon as Longitude,
            MIN(csr.Site_Rupture_Dist) as Min_Distance_to_Fault_km,
            AVG(csr.Site_Rupture_Dist) as Avg_Distance_to_Faults_km,
            COUNT(DISTINCT csr.Source_ID) as Num_Faults_Within_200km
        FROM 
            CyberShake_Sites cs
            JOIN CyberShake_Site_Ruptures csr ON cs.CS_Site_ID = csr.CS_Site_ID
        WHERE 
            csr.Site_Rupture_Dist <= 200.0
        GROUP BY 
            cs.CS_Site_ID, cs.CS_Short_Name, cs.CS_Site_Name, cs.CS_Site_Lat, cs.CS_Site_Lon
        ORDER BY 
            cs.CS_Short_Name
        """
        
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        df_summary = pd.DataFrame(results)
        print(f"Retrieved distance data for {len(df_summary)} sites")
        
        # Now get some detailed distance examples
        query2 = """
        SELECT 
            cs.CS_Short_Name as Site_ID,
            r.Source_Name as Fault_Name,
            csr.Site_Rupture_Dist as Distance_km,
            r.Start_Lat as Fault_Start_Lat,
            r.Start_Lon as Fault_Start_Lon,
            r.End_Lat as Fault_End_Lat,
            r.End_Lon as Fault_End_Lon
        FROM 
            CyberShake_Site_Ruptures csr
            JOIN CyberShake_Sites cs ON csr.CS_Site_ID = cs.CS_Site_ID
            JOIN Ruptures r ON csr.Source_ID = r.Source_ID 
                AND csr.Rupture_ID = r.Rupture_ID 
                AND csr.ERF_ID = r.ERF_ID
        WHERE 
            cs.CS_Short_Name IN ('USC', 'PAS', 'WNGC', 'STNI', 'SBSM')
            AND csr.Site_Rupture_Dist <= 50.0
        ORDER BY 
            cs.CS_Short_Name, csr.Site_Rupture_Dist
        LIMIT 100
        """
        
        cursor.execute(query2)
        results2 = cursor.fetchall()
        df_detail = pd.DataFrame(results2)
        
        cursor.close()
        connection.close()
        
        return df_summary, df_detail
        
    except Exception as e:
        print(f"Error: {e}")
        return None, None

def query_fault_list():
    """
    Query list of faults with their geometry.
    """
    print("\nRetrieving fault geometry data...")
    
    try:
        connection = pymysql.connect(
            host='moment.usc.edu',
            user='cybershk_ro',
            password='CyberShake2007',
            database='CyberShake',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        # Get unique faults
        query = """
        SELECT DISTINCT
            Source_ID,
            Source_Name as Fault_Name,
            MIN(Start_Lat) as Min_Start_Lat,
            MAX(Start_Lat) as Max_Start_Lat,
            MIN(Start_Lon) as Min_Start_Lon,
            MAX(Start_Lon) as Max_Start_Lon,
            MIN(End_Lat) as Min_End_Lat,
            MAX(End_Lat) as Max_End_Lat,
            MIN(End_Lon) as Min_End_Lon,
            MAX(End_Lon) as Max_End_Lon,
            COUNT(DISTINCT Rupture_ID) as Num_Ruptures
        FROM 
            Ruptures
        WHERE 
            ERF_ID = 35
        GROUP BY 
            Source_ID, Source_Name
        ORDER BY 
            Source_Name
        LIMIT 50
        """
        
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        df = pd.DataFrame(results)
        cursor.close()
        connection.close()
        
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def main():
    print("="*60)
    print("CyberShake Pre-calculated Distance Data")
    print("="*60)
    print("All distances are pre-calculated and stored in the database")
    print("No calculations or simulations are being performed\n")
    
    # Get site distance summary
    df_summary, df_detail = query_site_distances()
    
    if df_summary is not None and not df_summary.empty:
        # Save summary
        output_file = 'cybershake_site_fault_distances.csv'
        df_summary.to_csv(output_file, index=False, float_format='%.2f')
        print(f"✓ Site distance summary saved to: {output_file}")
        
        # Display statistics
        print("\nDistance Statistics (all pre-calculated):")
        print(f"  Sites analyzed: {len(df_summary)}")
        print(f"  Minimum distance to any fault: {df_summary['Min_Distance_to_Fault_km'].min():.2f} km")
        print(f"  Maximum closest fault distance: {df_summary['Min_Distance_to_Fault_km'].max():.2f} km")
        print(f"  Average closest fault distance: {df_summary['Min_Distance_to_Fault_km'].mean():.2f} km")
        
        print("\nSites closest to faults:")
        closest = df_summary.nsmallest(10, 'Min_Distance_to_Fault_km')[['Site_ID', 'Site_Name', 'Min_Distance_to_Fault_km']]
        print(closest.to_string(index=False))
        
        print("\nSites farthest from faults:")
        farthest = df_summary.nlargest(5, 'Min_Distance_to_Fault_km')[['Site_ID', 'Site_Name', 'Min_Distance_to_Fault_km']]
        print(farthest.to_string(index=False))
    
    if df_detail is not None and not df_detail.empty:
        # Save detailed examples
        output_file2 = 'cybershake_distance_examples.csv'
        df_detail.to_csv(output_file2, index=False, float_format='%.2f')
        print(f"\n✓ Distance examples saved to: {output_file2}")
    
    # Get fault list
    df_faults = query_fault_list()
    if df_faults is not None and not df_faults.empty:
        output_file3 = 'cybershake_fault_geometry.csv'
        df_faults.to_csv(output_file3, index=False, float_format='%.4f')
        print(f"✓ Fault geometry saved to: {output_file3}")
        print(f"\nTotal faults retrieved: {len(df_faults)}")
    
    print("\n" + "="*60)
    print("Summary:")
    print("- Site-to-fault distances: PRE-CALCULATED in CyberShake_Site_Ruptures table")
    print("- Fault geometry: STORED in Ruptures table (start/end coordinates)")
    print("- Basin structure: STORED as Z1.0 and Z2.5 depths in CyberShake_Runs table")
    print("- All data retrieved directly from database, no calculations performed")
    print("="*60)
    
    # Open the main output file
    if sys.platform == "darwin" and os.path.exists(output_file):
        os.system(f"open {output_file}")

if __name__ == "__main__":
    main()