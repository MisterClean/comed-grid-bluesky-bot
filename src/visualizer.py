import matplotlib.pyplot as plt
from matplotlib.dates import HourLocator, DateFormatter
import seaborn as sns
import pytz
from datetime import datetime, timedelta
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()

class LoadVisualizer:
    def __init__(self):
        self.config = load_config()
        self.visualization_config = self.config['visualization']
        self.timezone = pytz.timezone(self.config['data_settings']['timezones']['target'])
        self.setup_style()

    def setup_style(self):
        """Set up the plotting style"""
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = [
            self.visualization_config['chart_width'],
            self.visualization_config['chart_height']
        ]

    def create_24h_chart(self, df, output_path=None, timezone=None):
        """Create a chart of the last 24 hours of load data"""
        try:
            # Use provided timezone or default to config
            tz = timezone if timezone is not None else self.timezone
            now = datetime.now(tz)
            yesterday = now - timedelta(days=1)
            
            # Convert UTC timestamps to target timezone for display
            plot_data = df.copy()
            plot_data['display_time'] = plot_data['interval_start_utc'].dt.tz_convert(tz)
            
            # Filter last 24 hours based on target timezone
            plot_data = plot_data[plot_data['display_time'] >= yesterday]
            
            plt.figure()
            plt.plot(plot_data['display_time'], 
                    plot_data['load.comed'], 
                    linewidth=2)
            
            self._format_chart(tz)
            
            # Use provided output path or default
            output_path = output_path or 'output/comed_load_24h.png'
            plt.savefig(output_path, 
                       dpi=self.visualization_config['dpi'], 
                       bbox_inches='tight')
            plt.close()
            
            logger.info(f"Chart saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating chart: {str(e)}")
            raise

    def _format_chart(self, timezone):
        """Apply chart formatting"""
        plt.title('ComEd Load - Last 24 Hours (Central Time)', 
                 fontsize=14, pad=20)
        plt.xlabel('Time (Central)', fontsize=12)
        plt.ylabel('Load (MW)', fontsize=12)
        
        plt.gca().xaxis.set_major_locator(
            HourLocator(interval=self.visualization_config['hour_interval']))
        plt.gca().xaxis.set_major_formatter(
            DateFormatter('%I %p', tz=timezone))
        
        plt.gcf().autofmt_xdate()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

class NuclearVisualizer:
    def __init__(self):
        self.config = load_config()
        self.visualization_config = self.config['visualization']
        self.timezone = pytz.timezone(self.config['data_settings']['timezones']['target'])
        self.setup_style()

    def setup_style(self):
        """Set up the plotting style"""
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = [
            self.visualization_config['chart_width'],
            self.visualization_config['chart_height']
        ]

    def create_nuclear_vs_load_chart(self, load_df, nuclear_df, output_path=None, timezone=None):
        """Create a chart comparing nuclear generation to load"""
        try:
            # Use provided timezone or default to config
            tz = timezone if timezone is not None else self.timezone
            now = datetime.now(tz)
            yesterday = now - timedelta(days=1)
            
            # Convert UTC timestamps to target timezone for display
            load_data = load_df.copy()
            load_data['display_time'] = load_data['interval_start_utc'].dt.tz_convert(tz)
            
            nuclear_data = nuclear_df.copy()
            nuclear_data['display_time'] = nuclear_data['timestamp'].dt.tz_convert(tz)
            
            # Filter last 24 hours based on target timezone
            load_data = load_data[load_data['display_time'] >= yesterday]
            nuclear_data = nuclear_data[nuclear_data['display_time'] >= yesterday]
            
            # Calculate total nuclear generation per timestamp
            nuclear_totals = nuclear_data.groupby('display_time')['estimated_mw'].sum().reset_index()
            
            plt.figure()
            
            # Plot load data
            plt.plot(load_data['display_time'], 
                    load_data['load.comed'], 
                    color='coral',
                    linewidth=2,
                    label='Load')
            
            # Plot nuclear generation
            plt.plot(nuclear_totals['display_time'],
                    nuclear_totals['estimated_mw'],
                    color='navy',
                    linewidth=2,
                    label='Nuclear Generation')
            
            self._format_chart(tz)
            
            # Add legend
            plt.legend(loc='upper right')
            
            # Use provided output path or default
            output_path = output_path or 'output/nuclear_vs_load_24h.png'
            plt.savefig(output_path, 
                       dpi=self.visualization_config['dpi'], 
                       bbox_inches='tight')
            plt.close()
            
            logger.info(f"Nuclear vs Load chart saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating nuclear vs load chart: {str(e)}")
            raise

    def _format_chart(self, timezone):
        """Apply chart formatting"""
        plt.title('Nuclear Generation vs Load - Last 24 Hours (Central Time)', 
                 fontsize=14, pad=20)
        plt.xlabel('Time (Central)', fontsize=12)
        plt.ylabel('Megawatts (MW)', fontsize=12)
        
        plt.gca().xaxis.set_major_locator(
            HourLocator(interval=self.visualization_config['hour_interval']))
        plt.gca().xaxis.set_major_formatter(
            DateFormatter('%I %p', tz=timezone))
        
        plt.gcf().autofmt_xdate()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
