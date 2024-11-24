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
            # Ensure nuclear data has timestamp column
            time_col = 'timestamp' if 'timestamp' in nuclear_data.columns else 'interval_start_utc'
            nuclear_data['display_time'] = nuclear_data[time_col].dt.tz_convert(tz)
            
            # Filter last 24 hours based on target timezone
            load_data = load_data[load_data['display_time'] >= yesterday]
            nuclear_data = nuclear_data[nuclear_data['display_time'] >= yesterday]
            
            plt.figure()
            
            # Plot load data
            plt.plot(load_data['display_time'], 
                    load_data['load.comed'], 
                    color='coral',
                    linewidth=2,
                    label='Load')
            
            # Plot nuclear generation
            plt.plot(nuclear_data['display_time'],
                    nuclear_data['estimated_mw'],
                    color='navy',
                    linewidth=2,
                    label='Nuclear Generation')
            
            self._format_chart(tz)
            
            # Add legend
            plt.legend(loc='upper right')
            
            # Set dynamic y-axis limits with 2000MW buffer
            max_load = load_data['load.comed'].max()
            max_nuclear = nuclear_data['estimated_mw'].max()
            max_value = max(max_load, max_nuclear)
            plt.ylim(0, max_value + 2000)
            
            # Log the maximum values for debugging
            logger.info(f"Maximum load: {max_load:.0f} MW")
            logger.info(f"Maximum nuclear generation: {max_nuclear:.0f} MW")
            logger.info(f"Setting y-axis limit to: {max_value + 2000:.0f} MW")
            
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
