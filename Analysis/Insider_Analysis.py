import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import yfinance as yf
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os
import json

class InsiderAnalyzer:
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_file = os.path.join(self.script_dir, 'Scraping', 'insider_buys.csv')
        self.df = self.load_data()
        
    def load_data(self):
        """Load and prepare the insider trading data"""
        df = pd.read_csv(self.data_file)
        df['Filing Date'] = pd.to_datetime(df['Filing Date'])
        return df
    
    def analyze_timing_patterns(self):
        """Analyze time-based patterns in trades"""
        df = self.df.copy()
        df['Hour'] = df['Filing Date'].dt.hour
        df['Day'] = df['Filing Date'].dt.day_name()
        df['Month'] = df['Filing Date'].dt.month
        
        # Calculate price impact
        df['Price_Impact'] = (df['Price Bought'] - df['Transaction Price']) / df['Transaction Price']
        
        timing_analysis = {
            'hourly_value': df.groupby('Hour')['Value'].mean(),
            'daily_impact': df.groupby('Day')['Price_Impact'].mean(),
            'monthly_pattern': df.groupby('Month')['Value'].mean(),
            'best_filing_hour': int(df.groupby('Hour')['Price_Impact'].mean().idxmax()),
            'best_filing_day': df.groupby('Day')['Price_Impact'].mean().idxmax()
        }
        
        return timing_analysis, df
    
    def analyze_insider_clusters(self, df):
        """Analyze insider trading clusters and patterns"""
        insider_stats = df.groupby('Insider Name').agg({
            'Value': ['count', 'mean', 'sum'],
            'Price_Impact': ['mean', 'std']
        })
        
        # Fix column names after aggregation
        insider_stats.columns = ['Value_count', 'Value_mean', 'Value_sum', 
                               'Price_Impact_mean', 'Price_Impact_std']
        insider_stats = insider_stats.reset_index()
        
        # Handle NaN values by dropping rows with missing data
        insider_stats = insider_stats.dropna(subset=['Value_mean', 'Price_Impact_mean'])
        
        # Only proceed with clustering if we have enough data
        if len(insider_stats) >= 3:  # Need at least 3 points for 3 clusters
            # Prepare for clustering
            cluster_features = ['Value_mean', 'Price_Impact_mean']
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(insider_stats[cluster_features])
            
            # Perform clustering
            kmeans = KMeans(n_clusters=min(3, len(insider_stats)), random_state=42)
            insider_stats['Cluster'] = kmeans.fit_predict(scaled_features)
        else:
            insider_stats['Cluster'] = 0  # Single cluster if not enough data
        
        return insider_stats
    
    def analyze_company_patterns(self, df):
        """Analyze company-specific trading patterns"""
        company_stats = df.groupby('Company Name').agg({
            'Value': ['count', 'sum', 'mean'],
            'Price_Impact': ['mean', 'std'],
            'Transaction Price': 'mean',
            'Price Bought': 'mean'
        }).reset_index()
        
        company_stats['Success_Rate'] = df.groupby('Company Name')['Price_Impact'].apply(
            lambda x: (x > 0).mean()
        ).values
        
        return company_stats
    
    def generate_predictive_features(self, df):
        """Generate features for predictive analysis"""
        df = df.copy()
        
        # Time-based features
        df['Is_Morning'] = df['Hour'].between(9, 12)
        df['Is_Afternoon'] = df['Hour'].between(13, 16)
        
        # Value-based features
        df['Large_Trade'] = df['Value'] > df['Value'].median()
        
        # Rolling metrics
        df = df.sort_values('Filing Date')
        df['Rolling_Avg_Impact'] = df.groupby('Company Name')['Price_Impact'].transform(
            lambda x: x.rolling(3, min_periods=1).mean()
        )
        
        return df

class PatternRecognizer:
    def __init__(self, df):
        self.df = df
        self.patterns = {}
        
    def identify_cluster_trades(self):
        """Identify clusters of trades"""
        self.df['Trade_Week'] = self.df['Filing Date'].dt.isocalendar().week
        clusters = self.df.groupby(['Company Name', 'Trade_Week']).size()
        return clusters[clusters > 1]
    
    def identify_size_anomalies(self):
        """Identify unusually large trades"""
        mean_value = self.df['Value'].mean()
        std_value = self.df['Value'].std()
        return self.df[self.df['Value'] > (mean_value + 2*std_value)]
    
    def analyze_all_patterns(self):
        """Run all pattern analysis"""
        self.patterns['clusters'] = self.identify_cluster_trades()
        self.patterns['anomalies'] = self.identify_size_anomalies()
        return self.patterns

class SignalGenerator:
    def __init__(self, historical_df):
        self.historical_df = historical_df
        self.thresholds = self.calculate_thresholds()
    
    def calculate_thresholds(self):
        """Calculate signal thresholds"""
        return {
            'value_threshold': float(self.historical_df['Value'].quantile(0.75)),
            'impact_threshold': float(self.historical_df['Price_Impact'].mean()),
            'success_threshold': 0.6
        }
    
    def generate_signal(self, new_trade):
        """Generate trading signals"""
        signals = []
        
        # Value signal
        if new_trade['Value'] > self.thresholds['value_threshold']:
            signals.append('Large Value Trade')
        
        # Cluster signal
        recent_company_trades = self.historical_df[
            (self.historical_df['Company Name'] == new_trade['Company Name']) &
            (self.historical_df['Filing Date'] > new_trade['Filing Date'] - timedelta(days=5))
        ]
        if len(recent_company_trades) > 0:
            signals.append('Cluster Trade')
        
        return signals

def run_analysis():
    """Main function to run all analyses"""
    analyzer = InsiderAnalyzer()
    
    # Create results directory
    results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
    print(f"Creating directory: {results_dir}")
    
    try:
        os.makedirs(results_dir, exist_ok=True)
        
        # Run analyses
        timing_analysis, df_with_timing = analyzer.analyze_timing_patterns()
        insider_clusters = analyzer.analyze_insider_clusters(df_with_timing)
        company_patterns = analyzer.analyze_company_patterns(df_with_timing)
        df_with_features = analyzer.generate_predictive_features(df_with_timing)
        
        # Save files with full paths
        insider_clusters_path = os.path.join(results_dir, 'insider_clusters.csv')
        company_patterns_path = os.path.join(results_dir, 'company_patterns.csv')
        enriched_data_path = os.path.join(results_dir, 'enriched_data.csv')
        timing_analysis_path = os.path.join(results_dir, 'timing_analysis.json')
        
        # Save each file
        insider_clusters.to_csv(insider_clusters_path, index=False)
        print(f"Saved: {insider_clusters_path}")
        
        company_patterns.to_csv(company_patterns_path, index=False)
        print(f"Saved: {company_patterns_path}")
        
        df_with_features.to_csv(enriched_data_path, index=False)
        print(f"Saved: {enriched_data_path}")
        
        # Convert timing analysis values to serializable format
        timing_analysis_serializable = {
            k: v.to_dict() if isinstance(v, pd.Series) else str(v) if isinstance(v, np.int32) else v
            for k, v in timing_analysis.items()
        }
        
        with open(timing_analysis_path, 'w') as f:
            json.dump(timing_analysis_serializable, f, indent=4)
        print(f"Saved: {timing_analysis_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    
    return {
        'timing_analysis': timing_analysis,
        'insider_clusters': insider_clusters,
        'company_patterns': company_patterns,
        'enriched_data': df_with_features
    }

if __name__ == "__main__":
    print("Starting analysis...")
    results = run_analysis()
    print("Analysis complete.") 