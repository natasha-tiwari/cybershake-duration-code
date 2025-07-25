#!/usr/bin/env python3
"""
Create CSV files for ALL faults in CyberShake with their pre-calculated distances.
"""

import pymysql
import pandas as pd
import os
import re
import sys

def sanitize_filename(name):
    """Clean fault name for use as filename."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', '_', name)
    name = name.strip('_')
    return name[:100]

def get_all_faults():
    """Get ALL faults from CyberShake that have distance data."""
    print("Retrieving complete fault list from CyberShake...")
    
    try:
        connection = pymysql.connect(
            host='moment.usc.edu',
            user='cybershk_ro',
            password='CyberShake2007',
            database='CyberShake',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        # Get ALL unique faults that have distance data
        query = """
        SELECT DISTINCT
            r.Source_ID,
            r.Source_Name,
            MIN(r.Start_Lat) as Fault_Start_Lat,
            MIN(r.Start_Lon) as Fault_Start_Lon,
            MAX(r.End_Lat) as Fault_End_Lat,
            MAX(r.End_Lon) as Fault_End_Lon,
            COUNT(DISTINCT r.Rupture_ID) as Num_Ruptures
        FROM 
            Ruptures r
        WHERE 
            r.ERF_ID = 35
            AND EXISTS (
                SELECT 1 
                FROM CyberShake_Site_Ruptures csr 
                WHERE csr.Source_ID = r.Source_ID 
                AND csr.ERF_ID = r.ERF_ID
            )
        GROUP BY 
            r.Source_ID, r.Source_Name
        ORDER BY 
            r.Source_Name
        """
        
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return results
        
    except Exception as e:
        print(f"Error getting fault list: {e}")
        return None

def get_fault_site_distances(source_id):
    """Get ALL pre-calculated distances from a specific fault to all sites."""
    
    try:
        connection = pymysql.connect(
            host='moment.usc.edu',
            user='cybershk_ro',
            password='CyberShake2007',
            database='CyberShake',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        # Get all site distances for this fault (no distance limit)
        query = """
        SELECT DISTINCT
            cs.CS_Short_Name as Site_ID,
            cs.CS_Site_Name as Site_Name,
            cs.CS_Site_Lat as Site_Latitude,
            cs.CS_Site_Lon as Site_Longitude,
            MIN(csr.Site_Rupture_Dist) as Distance_km
        FROM 
            CyberShake_Site_Ruptures csr
            JOIN CyberShake_Sites cs ON csr.CS_Site_ID = cs.CS_Site_ID
        WHERE 
            csr.Source_ID = %s
            AND csr.ERF_ID = 35
        GROUP BY 
            cs.CS_Site_ID, cs.CS_Short_Name, cs.CS_Site_Name, 
            cs.CS_Site_Lat, cs.CS_Site_Lon
        ORDER BY 
            csr.Site_Rupture_Dist, cs.CS_Short_Name
        """
        
        cursor = connection.cursor()
        cursor.execute(query, (source_id,))
        results = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return results
        
    except Exception as e:
        print(f"Error getting distances for source {source_id}: {e}")
        return None

def create_all_fault_files():
    """Create CSV files for ALL faults."""
    print("="*60)
    print("Creating ALL Fault Distance Files from CyberShake")
    print("="*60)
    
    # Create output directory
    output_dir = "all_fault_distance_files"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    # Get ALL faults
    faults = get_all_faults()
    if not faults:
        print("No faults retrieved")
        return
    
    print(f"\nFound {len(faults)} faults with distance data")
    print("Processing all faults...\n")
    
    # Process each fault
    successful_files = []
    failed_faults = []
    
    for i, fault in enumerate(faults):
        source_id = fault['Source_ID']
        source_name = fault['Source_Name']
        
        if (i+1) % 10 == 0:
            print(f"Progress: {i+1}/{len(faults)} faults processed...")
        
        # Get distances for this fault
        distances = get_fault_site_distances(source_id)
        
        if distances:
            # Create DataFrame
            df = pd.DataFrame(distances)
            
            # Add fault geometry info
            df['Fault_ID'] = source_id
            df['Fault_Name'] = source_name
            df['Fault_Start_Lat'] = fault['Fault_Start_Lat']
            df['Fault_Start_Lon'] = fault['Fault_Start_Lon']
            df['Fault_End_Lat'] = fault['Fault_End_Lat']
            df['Fault_End_Lon'] = fault['Fault_End_Lon']
            df['Fault_Num_Ruptures'] = fault['Num_Ruptures']
            
            # Reorder columns
            column_order = [
                'Fault_ID', 'Fault_Name', 'Fault_Start_Lat', 'Fault_Start_Lon',
                'Fault_End_Lat', 'Fault_End_Lon', 'Fault_Num_Ruptures',
                'Site_ID', 'Site_Name', 'Site_Latitude', 'Site_Longitude', 'Distance_km'
            ]
            df = df[column_order]
            
            # Create filename
            filename = f"fault_{source_id:04d}_{sanitize_filename(source_name)}.csv"
            filepath = os.path.join(output_dir, filename)
            
            # Save to CSV
            df.to_csv(filepath, index=False, float_format='%.4f')
            successful_files.append((source_id, source_name, filename, len(df)))
        else:
            failed_faults.append((source_id, source_name))
    
    print(f"\nProcessing complete!")
    
    # Create detailed summary file
    if successful_files:
        summary_file = os.path.join(output_dir, "00_ALL_FAULTS_SUMMARY.csv")
        summary_df = pd.DataFrame(successful_files, 
                                columns=['Fault_ID', 'Fault_Name', 'Filename', 'Num_Sites'])
        summary_df = summary_df.sort_values('Fault_ID')
        summary_df.to_csv(summary_file, index=False)
        
        # Create text summary
        text_summary = os.path.join(output_dir, "00_README.txt")
        with open(text_summary, 'w') as f:
            f.write("CyberShake Complete Fault Distance Files\n")
            f.write("="*50 + "\n\n")
            f.write(f"Total faults processed: {len(faults)}\n")
            f.write(f"Successful files created: {len(successful_files)}\n")
            f.write(f"Failed: {len(failed_faults)}\n\n")
            f.write("Data source: CyberShake pre-calculated distances\n")
            f.write("Tables used:\n")
            f.write("  - CyberShake_Site_Ruptures (distances)\n")
            f.write("  - Ruptures (fault geometry)\n")
            f.write("  - CyberShake_Sites (site information)\n\n")
            f.write("IMPORTANT: All distances are pre-calculated\n")
            f.write("No calculations or data creation was performed\n\n")
            
            if failed_faults:
                f.write("Faults with no distance data:\n")
                for fid, fname in failed_faults:
                    f.write(f"  - {fid}: {fname}\n")
        
        print(f"\n✓ Created {len(successful_files)} fault distance files")
        print(f"✓ Files saved in: {output_dir}/")
        print(f"✓ Summary files created:")
        print(f"   - 00_ALL_FAULTS_SUMMARY.csv (fault list)")
        print(f"   - 00_README.txt (documentation)")
        
        if failed_faults:
            print(f"\n⚠ {len(failed_faults)} faults had no distance data")
    
    print("\n" + "="*60)
    print("All data retrieved from CyberShake database")
    print("No calculations performed - only pre-existing data used")
    print("="*60)
    
    # Open the directory
    if sys.platform == "darwin":
        os.system(f"open {output_dir}")
    elif sys.platform == "win32":
        os.system(f"explorer {output_dir}")
    else:
        os.system(f"xdg-open {output_dir}")

if __name__ == "__main__":
    create_all_fault_files()