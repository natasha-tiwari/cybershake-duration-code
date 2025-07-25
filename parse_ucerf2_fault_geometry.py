#!/usr/bin/env python3

"""
Parse UCERF2 fault geometry XML file and extract parameters to CSV
"""

import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2

def calculate_fault_length(trace_locations):
    """Calculate fault length from trace coordinates using Haversine formula"""
    if len(trace_locations) < 2:
        return 0.0
    
    total_length = 0.0
    R = 6371.0  # Earth's radius in km
    
    for i in range(len(trace_locations) - 1):
        lat1, lon1 = radians(trace_locations[i][0]), radians(trace_locations[i][1])
        lat2, lon2 = radians(trace_locations[i+1][0]), radians(trace_locations[i+1][1])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        total_length += R * c
    
    return total_length

def calculate_fault_width(dip_deg, upper_depth, lower_depth):
    """Calculate down-dip width from dip angle and depth range"""
    depth_range = lower_depth - upper_depth
    dip_rad = radians(dip_deg)
    
    # For vertical faults (dip = 90), width equals depth range
    if abs(dip_deg - 90.0) < 0.01:
        return depth_range
    
    # Calculate down-dip width
    width = depth_range / sin(dip_rad)
    return width

def parse_ucerf2_faults(xml_file):
    """Parse UCERF2 fault section data from XML file"""
    
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    faults = []
    
    for fault_section in root.findall('FaultSectionPrefData'):
        # Extract attributes
        fault_data = {
            'fault_id': int(fault_section.get('sectionId')),
            'fault_name': fault_section.get('sectionName'),
            'short_name': fault_section.get('shortName', ''),
            'slip_rate_mm_yr': fault_section.get('aveLongTermSlipRate'),
            'slip_rate_std_dev': fault_section.get('slipRateStdDev'),
            'dip_deg': float(fault_section.get('aveDip')),
            'rake_deg': fault_section.get('aveRake'),
            'upper_depth_km': float(fault_section.get('aveUpperDepth')),
            'lower_depth_km': float(fault_section.get('aveLowerDepth')),
            'aseismic_slip_factor': float(fault_section.get('aseismicSlipFactor')),
            'dip_direction_deg': float(fault_section.get('dipDirection'))
        }
        
        # Handle NaN values
        try:
            fault_data['slip_rate_mm_yr'] = float(fault_data['slip_rate_mm_yr'])
        except (ValueError, TypeError):
            fault_data['slip_rate_mm_yr'] = np.nan
            
        try:
            fault_data['slip_rate_std_dev'] = float(fault_data['slip_rate_std_dev'])
        except (ValueError, TypeError):
            fault_data['slip_rate_std_dev'] = np.nan
            
        try:
            fault_data['rake_deg'] = float(fault_data['rake_deg'])
        except (ValueError, TypeError):
            fault_data['rake_deg'] = np.nan
        
        # Extract fault trace coordinates
        trace_locations = []
        fault_trace = fault_section.find('FaultTrace')
        if fault_trace is not None:
            for location in fault_trace.findall('Location'):
                lat = float(location.get('Latitude'))
                lon = float(location.get('Longitude'))
                depth = float(location.get('Depth'))
                trace_locations.append((lat, lon, depth))
        
        # Calculate fault length from trace
        fault_data['fault_length_km'] = calculate_fault_length(trace_locations)
        
        # Calculate fault width from dip and depth range
        fault_data['fault_width_km'] = calculate_fault_width(
            fault_data['dip_deg'],
            fault_data['upper_depth_km'],
            fault_data['lower_depth_km']
        )
        
        # Store trace information
        fault_data['num_trace_points'] = len(trace_locations)
        if trace_locations:
            fault_data['start_lat'] = trace_locations[0][0]
            fault_data['start_lon'] = trace_locations[0][1]
            fault_data['end_lat'] = trace_locations[-1][0]
            fault_data['end_lon'] = trace_locations[-1][1]
        else:
            fault_data['start_lat'] = np.nan
            fault_data['start_lon'] = np.nan
            fault_data['end_lat'] = np.nan
            fault_data['end_lon'] = np.nan
        
        faults.append(fault_data)
    
    return faults

def main():
    # Parse XML file
    xml_file = 'ucerf2_data/PrefFaultSectionData.xml'
    print(f"Parsing UCERF2 fault geometry from: {xml_file}")
    
    faults = parse_ucerf2_faults(xml_file)
    
    # Convert to DataFrame
    df = pd.DataFrame(faults)
    
    # Reorder columns for clarity
    column_order = [
        'fault_id', 'fault_name',
        'fault_length_km', 'fault_width_km',
        'dip_deg', 'rake_deg',
        'upper_depth_km', 'lower_depth_km',
        'slip_rate_mm_yr', 'slip_rate_std_dev',
        'dip_direction_deg', 'aseismic_slip_factor',
        'start_lat', 'start_lon', 'end_lat', 'end_lon',
        'num_trace_points'
    ]
    
    df = df[column_order]
    
    # Save to CSV
    output_file = 'ucerf2_fault_geometry_complete.csv'
    df.to_csv(output_file, index=False)
    
    print(f"\nUCERF2 fault geometry saved to: {output_file}")
    print(f"Total fault sections: {len(df)}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"  Fault length range: {df['fault_length_km'].min():.1f} - {df['fault_length_km'].max():.1f} km")
    print(f"  Fault width range: {df['fault_width_km'].min():.1f} - {df['fault_width_km'].max():.1f} km")
    print(f"  Dip angle range: {df['dip_deg'].min():.0f} - {df['dip_deg'].max():.0f} degrees")
    print(f"  Faults with valid rake: {df['rake_deg'].notna().sum()}")
    print(f"  Faults with slip rate: {df['slip_rate_mm_yr'].notna().sum()}")
    
    # Print sample data
    print("\nSample fault data (first 5 faults):")
    print(df[['fault_id', 'fault_name', 'fault_length_km', 'fault_width_km', 'dip_deg', 'rake_deg']].head())

if __name__ == "__main__":
    main()