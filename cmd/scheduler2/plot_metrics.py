#!/usr/bin/env python3
"""
Plot scheduler telemetry metrics from CSV log files.
Generates 13 publication-quality plots.
Usage: python3 plot_metrics.py scheduler_metrics.csv
"""

import sys
import os

try:
    import pandas as pd
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from collections import Counter
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip3 install pandas matplotlib")
    sys.exit(1)

def plot_metrics(csv_file, output_dir='./plots'):
    """Generate all 13 plots from CSV metrics file."""
    
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found!")
        print(f"Current directory: {os.getcwd()}")
        sys.exit(1)
    
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)
    
    if df.empty or len(df) == 0:
        print("No data found in CSV!")
        sys.exit(1)
    
    print(f"Parsed {len(df)} data points")
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert timestamp to relative seconds from start
    start_time = df['timestamp'].iloc[0]
    df['rel_time'] = df['timestamp'] - start_time
    
    # Style settings
    plt.rcParams['font.size'] = 12
    plt.rcParams['axes.labelsize'] = 14
    plt.rcParams['axes.titlesize'] = 16
    plt.rcParams['axes.titleweight'] = 'bold'
    plt.rcParams['figure.dpi'] = 150
    
    # Color palette matching your uploaded plots
    colors = {
        'throughput': '#B5B52A',      # Yellow-green
        'latency': '#2CA02C',          # Green
        'queue': '#9467BD',            # Purple
        'temperature': '#FF7F0E',      # Orange
        'memory': '#1F77B4',           # Blue
        'cpu': '#D62728',              # Red
        'power': '#8C564B',            # Brown
        'dqn': '#E377C2',              # Pink
        'scheduler': '#9400D3',        # Dark violet
        'qos': '#17BECF',              # Cyan
        'least_impedance': '#1F77B4',  # Blue
        'closest_node': '#2CA02C',     # Green
    }
    
    # 1. Throughput vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['throughput'], color=colors['throughput'], linewidth=2)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Throughput (fps)', fontsize=14)
    ax.set_title('Throughput vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/throughput_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [1/13] throughput_vs_time.png")
    
    # 2. Queue Length vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['queue'], color=colors['queue'], linewidth=2)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Queue Length (frames)', fontsize=14)
    ax.set_title('Queue Length vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/queue_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [2/13] queue_vs_time.png")
    
    # 3. Node Temperature vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['temperature'], color=colors['temperature'], linewidth=2)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Temperature (\u00b0C)', fontsize=14)
    ax.set_title('Node Temperature vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/temperature_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [3/13] temperature_vs_time.png")
    
    # 4. Total Frames Processed vs Time (cumulative)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['total_frames'], color=colors['memory'], linewidth=3)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Total Frames', fontsize=14)
    ax.set_title('Total Frames Processed vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/frames_processed_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [4/13] frames_processed_vs_time.png")
    
    # 5. DQN Inference Time vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['dqn_inference_ms'], color=colors['dqn'], linewidth=2)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Inference Time (ms)', fontsize=14)
    ax.set_title('DQN Inference Latency vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/dqn_inference_time_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [5/13] dqn_inference_time_vs_time.png")
    
    # 6. Processing Latency vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['latency'], color=colors['latency'], linewidth=2.5)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Latency (ms)', fontsize=14)
    ax.set_title('Processing Latency vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/latency_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [6/13] latency_vs_time.png")
    
    # 7. Memory Utilization vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['memory'], color=colors['memory'], linewidth=2.5)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Memory (%)', fontsize=14)
    ax.set_title('Memory Utilization vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/memory_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [7/13] memory_vs_time.png")
    
    # 8. CPU Utilization vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['cpu'], color=colors['cpu'], linewidth=2.5)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('CPU (%)', fontsize=14)
    ax.set_title('CPU Utilization vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/cpu_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [8/13] cpu_vs_time.png")
    
    # 9. Worker Node Selection Counts (bar chart)
    node_counts = Counter(df['selected_node'])
    fig, ax = plt.subplots(figsize=(12, 6))
    nodes = list(node_counts.keys())
    counts = list(node_counts.values())
    bar_colors = ['#7F7F7F', '#BCBD22', '#17BECF'][:len(nodes)]
    bars = ax.bar(nodes, counts, color=bar_colors, width=0.6)
    ax.set_ylabel('Selection Count', fontsize=14)
    ax.set_title('Worker Node Selection Counts', fontsize=18, fontweight='bold')
    ax.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/node_selection.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [9/13] node_selection.png")
    
    # 10. Selected Policy Distribution (bar chart)
    policy_counts = Counter(df['selected_policy'])
    policy_names = {0: 'Closest Node', 1: 'Round Robin', 2: 'Least Impedance', 3: 'Fallback'}
    fig, ax = plt.subplots(figsize=(10, 6))
    policies = [policy_names.get(p, f'Policy {p}') for p in policy_counts.keys()]
    counts = list(policy_counts.values())
    bar_colors = [colors['closest_node'] if 'Closest' in p else colors['least_impedance'] 
                  for p in policies]
    bars = ax.bar(policies, counts, color=bar_colors, width=0.6)
    ax.set_ylabel('Scheduling Count', fontsize=14)
    ax.set_title('Selected Policy Distribution', fontsize=18, fontweight='bold')
    ax.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/policy_usage.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [10/13] policy_usage.png")
    
    # 11. Scheduler Decision Time vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['scheduler_decision_ms'], color=colors['scheduler'], linewidth=2.5)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Decision Time (ms)', fontsize=14)
    ax.set_title('Scheduler Decision Latency vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/scheduler_decision_time_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [11/13] scheduler_decision_time_vs_time.png")
    
    # 12. QoS Score vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['qos'], color=colors['qos'], linewidth=2.5)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('QoS Score (0.0 - 1.0)', fontsize=14)
    ax.set_title('QoS Score vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/qos_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [12/13] qos_vs_time.png")
    
    # 13. Power Consumption vs Time
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['rel_time'], df['power'], color=colors['power'], linewidth=2)
    ax.set_xlabel('Time (seconds)', fontsize=14)
    ax.set_ylabel('Power (W)', fontsize=14)
    ax.set_title('Power Consumption vs Time', fontsize=18, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/power_vs_time.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [13/13] power_vs_time.png")
    
    print(f"\nAll plots saved to {output_dir}/")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 plot_metrics.py <csv_file>")
        print("Example: python3 plot_metrics.py scheduler_metrics.csv")
        sys.exit(1)
    
    plot_metrics(sys.argv[1])
