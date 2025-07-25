#!/usr/bin/env python3
"""
Calculate period-independent and period-dependent durations from CyberShake binary .grm files.
Modified version that can read binary GRM format.
"""

import numpy as np
import pandas as pd
from scipy import signal
from scipy.integrate import cumulative_trapezoid
import os
import glob
import argparse
import struct


def load_binary_grm_file(filename):
    """
    Load acceleration time series from binary .grm file.
    
    GRM files have a 56-byte header followed by binary float data:
    - Header contains metadata including nt (number of timesteps) at bytes 40-44
    - Data consists of X and Y components stored as 4-byte floats
    """
    try:
        with open(filename, 'rb') as fp:
            # Read 56-byte header
            header = fp.read(56)
            if len(header) < 56:
                raise ValueError("Invalid header size")
            
            # Extract number of timesteps from header
            nt = struct.unpack('i', header[40:44])[0]
            
            # Read acceleration data (2 components * 4 bytes per float * nt timesteps)
            num_components = 2
            sizeof_float = 4
            data_size = num_components * sizeof_float * nt
            data_bytes = fp.read(data_size)
            
            if len(data_bytes) < data_size:
                raise ValueError(f"Insufficient data: expected {data_size} bytes, got {len(data_bytes)}")
            
            # Convert binary data to numpy array
            data = np.frombuffer(data_bytes, dtype=np.float32)
            
            # Reshape to separate X and Y components
            data = data.reshape(num_components, nt)
            
            # Extract X component (first row)
            accel_cms2 = data[0, :]
            
            # Create time array assuming 100 Hz sampling rate (0.01s timestep)
            dt = 0.01
            time = np.arange(nt) * dt
            
            # Convert from cm/s^2 to g (1g = 981 cm/s^2)
            accel_g = accel_cms2 / 981.0
            
            return time, accel_g
            
    except Exception as e:
        print(f"Error loading binary .grm file {filename}: {e}")
        return None, None


def calculate_arias_intensity_detailed(time, accel_g):
    """
    Calculate Arias Intensity with detailed explanation.
    
    Arias Intensity is a measure of the total energy content of an earthquake
    ground motion. It's calculated as:
    
    IA(t) = (π/2g) * ∫[a(t)]^2 dt
    
    Where:
    - a(t) is the acceleration time history in m/s^2
    - g is gravitational acceleration (9.81 m/s^2)
    - The integral is from 0 to time t
    
    The factor π/2g comes from the relationship between the input energy
    to a linear oscillator and the integral of squared acceleration.
    
    Returns:
    - arias_intensity: Cumulative Arias Intensity array in m/s
    - calculation_details: Dictionary with intermediate values
    """
    g = 9.81  # gravitational acceleration in m/s^2
    
    # Step 1: Convert acceleration from g to m/s^2
    accel_ms2 = accel_g * g
    
    # Step 2: Square the acceleration
    accel_squared = accel_ms2**2
    
    # Step 3: Perform cumulative integration using trapezoidal rule
    # The trapezoidal rule approximates the integral as the sum of trapezoids
    # For each interval [ti, ti+1], area = 0.5 * (yi + yi+1) * (ti+1 - ti)
    cumulative_integral = cumulative_trapezoid(accel_squared, time, initial=0)
    
    # Step 4: Apply the Arias Intensity scaling factor
    arias_intensity = (np.pi / (2 * g)) * cumulative_integral
    
    # Calculate some useful metrics
    details = {
        'max_acceleration_g': np.max(np.abs(accel_g)),
        'max_acceleration_ms2': np.max(np.abs(accel_ms2)),
        'total_arias_intensity': arias_intensity[-1],
        'scaling_factor': np.pi / (2 * g),
        'duration': time[-1] - time[0],
        'num_points': len(time),
        'sampling_rate': 1.0 / np.mean(np.diff(time))
    }
    
    return arias_intensity, details


def find_5_95_duration_detailed(time, arias_intensity):
    """
    Calculate 5%-95% duration with detailed explanation.
    
    The 5%-95% duration represents the time interval during which
    the central 90% of the earthquake energy is released.
    
    Process:
    1. Normalize the Arias Intensity to range [0, 1]
    2. Find the time when 5% of total energy is reached (t5)
    3. Find the time when 95% of total energy is reached (t95)
    4. Duration = t95 - t5
    
    This metric is useful because:
    - It's less sensitive to noise at the beginning and end of records
    - It captures the main energy release phase of the earthquake
    - It correlates with damage potential
    """
    total_arias = arias_intensity[-1]
    
    if total_arias == 0:
        return 0.0, 0.0, 0.0, {}
    
    # Normalize to [0, 1]
    normalized_arias = arias_intensity / total_arias
    
    # Find indices where 5% and 95% are first exceeded
    idx_5 = np.where(normalized_arias >= 0.05)[0]
    idx_95 = np.where(normalized_arias >= 0.95)[0]
    
    if len(idx_5) == 0 or len(idx_95) == 0:
        return 0.0, 0.0, 0.0, {}
    
    # Use linear interpolation for more accurate time values
    # This finds the exact time when the threshold is crossed
    t5 = np.interp(0.05, normalized_arias, time)
    t95 = np.interp(0.95, normalized_arias, time)
    
    duration_5_95 = t95 - t5
    
    details = {
        'energy_at_t5': 0.05 * total_arias,
        'energy_at_t95': 0.95 * total_arias,
        'percent_before_t5': 5.0,
        'percent_after_t95': 5.0,
        'percent_in_duration': 90.0
    }
    
    return duration_5_95, t5, t95, details


def design_bandpass_filter_detailed(f_center, fs, bandwidth_factor=0.2):
    """
    Design a Butterworth bandpass filter with detailed explanation.
    
    For period-dependent duration calculation, we filter the acceleration
    to isolate motion at specific frequencies corresponding to structural
    periods of interest.
    
    Parameters:
    - f_center: Center frequency (Hz) = 1/Period
    - fs: Sampling frequency (Hz)
    - bandwidth_factor: Bandwidth as fraction of center frequency
    
    Example: For T=1.0s:
    - f_center = 1.0 Hz
    - With bandwidth_factor = 0.2, filter passes 0.8-1.2 Hz
    - This isolates motion that would affect 1-second period structures
    """
    # Calculate filter bounds
    f_low = f_center * (1 - bandwidth_factor)
    f_high = f_center * (1 + bandwidth_factor)
    
    # Ensure we're below Nyquist frequency
    nyquist = fs / 2
    f_high = min(f_high, nyquist * 0.95)
    f_low = max(f_low, 0.01)  # Avoid zero frequency
    
    # Design 4th order Butterworth filter
    # Butterworth filters have maximally flat frequency response in passband
    # 4th order provides good rolloff without excessive ringing
    order = 4
    sos = signal.butter(order, [f_low, f_high], btype='band', fs=fs, output='sos')
    
    filter_info = {
        'type': 'Butterworth Bandpass',
        'order': order,
        'center_freq_hz': f_center,
        'low_freq_hz': f_low,
        'high_freq_hz': f_high,
        'bandwidth_hz': f_high - f_low,
        'relative_bandwidth': bandwidth_factor * 2
    }
    
    return sos, filter_info


def calculate_period_dependent_duration(time, accel_g, period):
    """
    Calculate duration for a specific oscillator period.
    
    This represents how long the shaking would affect a structure
    with the specified natural period.
    """
    # Calculate sampling frequency
    dt = np.mean(np.diff(time))
    fs = 1.0 / dt
    
    # Center frequency corresponding to the period
    f_center = 1.0 / period
    
    # Design bandpass filter
    sos, filter_info = design_bandpass_filter_detailed(f_center, fs)
    
    # Apply zero-phase filtering (forward and backward pass)
    # This preserves the timing of peaks in the signal
    filtered_accel = signal.sosfiltfilt(sos, accel_g)
    
    # Calculate Arias Intensity for filtered signal
    filtered_arias, _ = calculate_arias_intensity_detailed(time, filtered_accel)
    
    # Calculate 5-95% duration for filtered signal
    duration_5_95, t5, t95, _ = find_5_95_duration_detailed(time, filtered_arias)
    
    return duration_5_95, t5, t95, filtered_arias[-1], filter_info


def process_grm_file(filename):
    """
    Process a single .grm file and calculate all durations.
    """
    print(f"\nProcessing: {os.path.basename(filename)}")
    
    # Load data
    time, accel_g = load_binary_grm_file(filename)
    if time is None:
        return None
    
    print(f"  Duration: {time[-1]:.2f}s, {len(time)} points")
    print(f"  Peak acceleration: {np.max(np.abs(accel_g)):.3f}g")
    
    # Calculate period-independent duration
    arias_intensity, arias_details = calculate_arias_intensity_detailed(time, accel_g)
    duration_5_95, t5, t95, duration_details = find_5_95_duration_detailed(time, arias_intensity)
    
    print(f"\nPeriod-Independent Results:")
    print(f"  Total Arias Intensity: {arias_details['total_arias_intensity']:.3e} m/s")
    print(f"  5% time (t5): {t5:.3f}s")
    print(f"  95% time (t95): {t95:.3f}s")
    print(f"  5-95% Duration: {duration_5_95:.3f}s")
    print(f"  Energy in duration window: {duration_details['percent_in_duration']:.0f}%")
    
    # Initialize results
    results = {
        'filename': os.path.basename(filename),
        'peak_accel_g': arias_details['max_acceleration_g'],
        'total_duration_s': arias_details['duration'],
        'arias_total_ms': arias_details['total_arias_intensity'],
        'duration_5_95_unfiltered_s': duration_5_95,
        't5_unfiltered_s': t5,
        't95_unfiltered_s': t95
    }
    
    # Calculate period-dependent durations
    print(f"\nPeriod-Dependent Results:")
    print(f"{'Period(s)':<10} {'Freq(Hz)':<10} {'Filter(Hz)':<15} {'Duration(s)':<12}")
    print("-" * 50)
    
    periods = [0.1, 0.2, 0.5, 1.0, 2.0, 3.0]
    
    for T in periods:
        dur, t5_f, t95_f, arias_f, filter_info = calculate_period_dependent_duration(time, accel_g, T)
        
        filter_range = f"{filter_info['low_freq_hz']:.2f}-{filter_info['high_freq_hz']:.2f}"
        print(f"{T:<10.1f} {filter_info['center_freq_hz']:<10.1f} {filter_range:<15} {dur:<12.3f}")
        
        # Add to results
        results[f'duration_5_95_T{T:.1f}s'] = dur
        results[f't5_T{T:.1f}s'] = t5_f
        results[f't95_T{T:.1f}s'] = t95_f
        results[f'arias_filtered_T{T:.1f}'] = arias_f
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Calculate seismic durations from binary .grm acceleration files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python calculate_grm_durations_binary.py "USC_Study 15.12/4384"
  python calculate_grm_durations_binary.py single_file.grm
        """
    )
    parser.add_argument('input_path', help='Input file or directory path')
    parser.add_argument('-o', '--output', default='grm_duration_results.csv',
                        help='Output CSV file (default: grm_duration_results.csv)')
    parser.add_argument('-n', '--num_files', type=int, default=None,
                        help='Maximum number of files to process')
    
    args = parser.parse_args()
    
    # Find .grm files
    if os.path.isfile(args.input_path):
        grm_files = [args.input_path]
    elif os.path.isdir(args.input_path):
        grm_files = glob.glob(os.path.join(args.input_path, '**/*_bb.grm'), recursive=True)
        if not grm_files:
            grm_files = glob.glob(os.path.join(args.input_path, '**/*.grm'), recursive=True)
    else:
        print(f"Error: Path not found: {args.input_path}")
        return
    
    if not grm_files:
        print(f"No .grm files found in {args.input_path}")
        return
    
    # Limit number of files if specified
    if args.num_files:
        grm_files = grm_files[:args.num_files]
    
    print(f"Found {len(grm_files)} .grm file(s) to process")
    
    # Process files
    all_results = []
    for i, grm_file in enumerate(grm_files):
        print(f"\n{'='*60}")
        print(f"File {i+1}/{len(grm_files)}")
        
        try:
            results = process_grm_file(grm_file)
            if results:
                all_results.append(results)
        except Exception as e:
            print(f"Error processing {grm_file}: {e}")
            continue
    
    # Save results
    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(args.output, index=False)
        
        print(f"\n{'='*60}")
        print(f"Results saved to: {args.output}")
        print(f"\nSummary Statistics:")
        print(f"Files processed: {len(df)}")
        print(f"\nMean values:")
        print(f"  Peak acceleration: {df['peak_accel_g'].mean():.3f}g")
        print(f"  Unfiltered 5-95% duration: {df['duration_5_95_unfiltered_s'].mean():.2f}s")
        
        for T in [0.1, 0.2, 0.5, 1.0, 2.0, 3.0]:
            col = f'duration_5_95_T{T:.1f}s'
            if col in df.columns:
                print(f"  T={T:.1f}s duration: {df[col].mean():.2f}s")


if __name__ == "__main__":
    main()