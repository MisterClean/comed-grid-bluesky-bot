import matplotlib.pyplot as plt
from matplotlib.dates import HourLocator, DateFormatter
import seaborn as sns
import pytz
from datetime import datetime, timedelta
from src.utils.logger import setup_logger
from src.utils.config import load_config
import matplotlib.ticker as ticker
import matplotlib.dates as mdates

logger = setup_logger()

class NuclearVisualizer:
    def __init__(self):
        self.config = load_config()
        self.visualization_config = self.config['visualization']
        self.timezone = pytz.timezone(self.config['data_settings']['timezones']['target'])
        self.setup_style()

    def setup_style(self):
        """Set up the plotting style"""
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = [12, 8]  # 3:2 ratio

    def create_nuclear_chart(self, nuclear_df, nuclear_stats, output_path=None, timezone=None):
        """Create a chart of nuclear generation data"""
        try:
            # Use provided timezone or default to config
            tz = timezone if timezone is not None else self.timezone
            now = datetime.now(tz)
            yesterday = now - timedelta(days=1)
            
            # Convert UTC timestamps to target timezone for display
            nuclear_data = nuclear_df.copy()
            time_col = 'timestamp' if 'timestamp' in nuclear_data.columns else 'interval_start_utc'
            nuclear_data['display_time'] = nuclear_data[time_col].dt.tz_convert(tz)
            
            # Get load data from stats
            load_data = nuclear_stats['load_data'].copy()
            load_data['display_time'] = load_data['interval_start_utc'].dt.tz_convert(tz)
            
            # Filter last 24 hours based on target timezone
            nuclear_data = nuclear_data[nuclear_data['display_time'] >= yesterday]
            load_data = load_data[load_data['display_time'] >= yesterday]
            
            # Create figure with specific background color
            fig = plt.figure(facecolor='white')
            ax = plt.gca()
            ax.set_facecolor('white')
            
            # Plot load data with coral line
            plt.plot(load_data['display_time'], 
                    load_data['load.comed'], 
                    color='coral',
                    linewidth=2,
                    label='Load')
            
            # Plot nuclear generation with dark blue line
            plt.plot(nuclear_data['display_time'],
                    nuclear_data['estimated_mw'],
                    color='navy',
                    linewidth=2,
                    label='Nuclear Generation')
            
            # Add legend
            plt.legend(loc='upper right')
            
            def add_stats_box(stats):
                """Add a box containing stats"""
                bbox_props = dict(
                    boxstyle="round,pad=0.5",
                    fc="white",
                    ec="gray",
                    alpha=0.9
                )
                
                # Format the text for the box
                text = (
                    f"Last 24 hours\n"
                    f"Nuclear Coverage: {stats['nuclear_percentage']:.1f}%\n"
                    f"Hours at Full Coverage: {stats['full_coverage_hours']:.1f}%\n"
                    f"Average Generation: {stats['total_nuclear']/24000:.1f} GW"
                )
                
                # Add text box
                ax.text(0.02, 0.02, text,
                       transform=ax.transAxes,
                       bbox=bbox_props,
                       ha='left',
                       va='bottom',
                       fontsize=10)
            
            # Add stats box
            add_stats_box(nuclear_stats)
            
            # Set dynamic y-axis limits with 2000MW buffer
            max_load = load_data['load.comed'].max()
            max_nuclear = nuclear_data['estimated_mw'].max()
            max_value = max(max_load, max_nuclear)
            plt.ylim(0, max_value + 2000)
            
            # Main title with left alignment
            ax.text(0.0, 1.1, 'Nuclear Generation vs Load - Last 24 Hours (Central Time)', 
                    transform=ax.transAxes,
                    fontsize=16,
                    fontweight='bold')
            
            # Subtitle with left alignment
            ax.text(0.0, 1.05, '5 minute intervals (Megawatts). Nuclear data estimated using NRC & EIA reporting.',
                    transform=ax.transAxes,
                    fontsize=12,
                    color='gray')
            
            # Add attribution text at bottom right with reduced opacity
            ax.text(0.98, 0.02, '@comed-grid.bsky.social | Data From Grid Status, NRC, EIA',
                   transform=ax.transAxes,
                   ha='right',
                   va='bottom',
                   fontsize=8,
                   color='gray',
                   style='italic',
                   alpha=0.6)  # Reduced opacity
            
            # Remove labels since units are in subtitle
            plt.xlabel('')
            plt.ylabel('')
            
            # X-axis time formatting - show more frequent labels
            plt.gca().xaxis.set_major_locator(
                HourLocator(interval=3))  # Show every 3 hours
            plt.gca().xaxis.set_major_formatter(
                DateFormatter('%-I:%M %p', tz=tz))
            
            # Rotate x-axis labels for better readability
            plt.xticks(rotation=45, ha='right')
            
            # Y-axis formatting with comma separator
            def y_fmt(x, p):
                return f"{int(x):,}"
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(y_fmt))
            
            # Lighter grid
            plt.grid(True, alpha=0.15)
            
            # Remove top and right spines
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            
            # Add padding at the top for the titles and bottom for rotated labels
            plt.subplots_adjust(top=0.75, bottom=0.2)
            
            # Use provided output path or default
            output_path = output_path or 'output/nuclear_generation_24h.png'
            
            # Save with optimized settings for Bluesky
            plt.savefig(output_path, 
                       dpi=166,  # Optimized DPI to stay under 2000x2000 pixels
                       bbox_inches='tight',
                       facecolor='white',
                       format='png')  # Explicitly set PNG format
            plt.close()
            
            logger.info(f"Nuclear generation chart saved to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating nuclear generation chart: {str(e)}")
            plt.close()
            raise
