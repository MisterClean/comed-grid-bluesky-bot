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
