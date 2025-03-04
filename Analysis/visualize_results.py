import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os

def load_data():
    """Load all analysis results"""
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    print(f"Loading data from {results_dir}...")
    
    # Load CSV files
    insider_clusters = pd.read_csv(os.path.join(results_dir, 'insider_clusters.csv'))
    company_patterns = pd.read_csv(os.path.join(results_dir, 'company_patterns.csv'))
    enriched_data = pd.read_csv(os.path.join(results_dir, 'enriched_data.csv'))
    
    # Fix company patterns column names and data types
    company_patterns.columns = ['Company Name', 'Value_count', 'Value_sum', 'Value_mean', 
                              'Price_Impact_mean', 'Price_Impact_std', 'Transaction_Price_mean', 
                              'Price_Bought_mean', 'Success_Rate']
    
    # Convert numeric columns
    numeric_cols = ['Value_count', 'Value_sum', 'Value_mean', 'Price_Impact_mean', 
                   'Price_Impact_std', 'Transaction_Price_mean', 'Price_Bought_mean', 'Success_Rate']
    for col in numeric_cols:
        company_patterns[col] = pd.to_numeric(company_patterns[col], errors='coerce')
    
    # Load JSON timing analysis
    with open(os.path.join(results_dir, 'timing_analysis.json'), 'r') as f:
        timing_analysis = json.load(f)
    
    print("Data loaded successfully!")
    return insider_clusters, company_patterns, enriched_data, timing_analysis

def plot_timing_patterns(timing_analysis):
    """Plot timing-related patterns"""
    print("\n=== Plotting Timing Patterns ===")
    
    # Convert to pandas Series
    hourly_value = pd.Series(timing_analysis['hourly_value'])
    daily_impact = pd.Series(timing_analysis['daily_impact'])
    monthly_pattern = pd.Series(timing_analysis['monthly_pattern'])
    
    # Create subplots
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 6))
    
    # Plot 1: Hourly trading value
    sns.barplot(x=hourly_value.index, y=hourly_value.values, ax=ax1)
    ax1.set_title('Average Trade Value by Hour')
    ax1.set_xlabel('Hour')
    ax1.set_ylabel('Average Value ($)')
    
    # Plot 2: Daily impact
    sns.barplot(x=daily_impact.index, y=daily_impact.values, ax=ax2)
    ax2.set_title('Average Price Impact by Day')
    ax2.set_xlabel('Day')
    ax2.set_ylabel('Price Impact (%)')
    
    # Plot 3: Monthly pattern
    sns.barplot(x=monthly_pattern.index, y=monthly_pattern.values, ax=ax3)
    ax3.set_title('Average Trade Value by Month')
    ax3.set_xlabel('Month')
    ax3.set_ylabel('Average Value ($)')
    
    plt.tight_layout()
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    plt.savefig(os.path.join(results_dir, 'timing_patterns.png'))
    plt.close()
    print("Timing patterns plot saved as 'timing_patterns.png'")

def plot_insider_clusters(insider_clusters):
    """Plot insider clustering analysis"""
    print("\n=== Plotting Insider Clusters ===")
    
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(insider_clusters['Value_mean'], 
                         insider_clusters['Price_Impact_mean'],
                         c=insider_clusters['Cluster'],
                         cmap='viridis',
                         alpha=0.6,
                         s=100)
    
    plt.title('Insider Trading Clusters')
    plt.xlabel('Average Trade Value ($)')
    plt.ylabel('Average Price Impact (%)')
    plt.colorbar(scatter, label='Cluster')
    
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    plt.savefig(os.path.join(results_dir, 'insider_clusters.png'))
    plt.close()
    print("Insider clusters plot saved as 'insider_clusters.png'")
    
    # Print cluster statistics
    cluster_stats = insider_clusters.groupby('Cluster').agg({
        'Value_mean': ['mean', 'count'],
        'Price_Impact_mean': ['mean', 'std']
    }).round(3)
    
    print("\nCluster Statistics:")
    print(cluster_stats)

def plot_company_analysis(company_patterns):
    """Plot company-specific patterns"""
    print("\n=== Plotting Company Analysis ===")
    
    # Drop rows with missing values
    company_patterns = company_patterns.dropna(subset=['Success_Rate', 'Value_mean', 'Price_Impact_mean'])
    
    # Top companies plot
    plt.figure(figsize=(12, 6))
    top_companies = company_patterns.nlargest(10, 'Success_Rate')
    sns.barplot(data=top_companies, x='Success_Rate', y='Company Name')
    plt.title('Top 10 Companies by Trading Success Rate')
    plt.xlabel('Success Rate')
    plt.tight_layout()
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    plt.savefig(os.path.join(results_dir, 'top_companies.png'))
    plt.close()
    print("Top companies plot saved as 'top_companies.png'")
    
    # Value vs Impact plot
    plt.figure(figsize=(12, 8))
    plt.scatter(company_patterns['Value_mean'], 
               company_patterns['Price_Impact_mean'],
               alpha=0.5)
    plt.title('Trade Value vs Price Impact by Company')
    plt.xlabel('Average Trade Value ($)')
    plt.ylabel('Average Price Impact (%)')
    plt.savefig(os.path.join(results_dir, 'value_vs_impact.png'))
    plt.close()
    print("Value vs Impact plot saved as 'value_vs_impact.png'")

def plot_price_impact(enriched_data):
    """Plot price impact analysis"""
    print("\n=== Plotting Price Impact Analysis ===")
    
    # Convert to datetime
    enriched_data['Filing Date'] = pd.to_datetime(enriched_data['Filing Date'])
    
    # Rolling average plot
    plt.figure(figsize=(15, 6))
    enriched_data.set_index('Filing Date')['Price_Impact'].rolling(window=30).mean().plot()
    plt.title('30-Day Rolling Average of Price Impact')
    plt.xlabel('Date')
    plt.ylabel('Price Impact (%)')
    plt.grid(True)
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    plt.savefig(os.path.join(results_dir, 'rolling_impact.png'))
    plt.close()
    print("Rolling impact plot saved as 'rolling_impact.png'")
    
    # Distribution plot
    plt.figure(figsize=(12, 6))
    sns.histplot(data=enriched_data, x='Price_Impact', bins=50)
    plt.title('Distribution of Price Impacts')
    plt.xlabel('Price Impact (%)')
    plt.ylabel('Count')
    plt.savefig(os.path.join(results_dir, 'impact_distribution.png'))
    plt.close()
    print("Impact distribution plot saved as 'impact_distribution.png'")
    
    # Print statistics
    impact_stats = enriched_data['Price_Impact'].describe().round(3)
    print("\nPrice Impact Statistics:")
    print(impact_stats)

def main():
    """Main function to run all visualizations"""
    print("Starting visualization process...")
    
    # Set style using seaborn
    sns.set_theme(style="whitegrid")
    sns.set_palette("husl")
    
    # Create results directory if it doesn't exist
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    os.makedirs(results_dir, exist_ok=True)
    
    # Load data
    insider_clusters, company_patterns, enriched_data, timing_analysis = load_data()
    
    # Generate all plots
    plot_timing_patterns(timing_analysis)
    plot_insider_clusters(insider_clusters)
    plot_company_analysis(company_patterns)
    plot_price_impact(enriched_data)
    
    print("\nVisualization complete! All plots saved in the 'results' directory.")

if __name__ == "__main__":
    main() 