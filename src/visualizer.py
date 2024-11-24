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

class LoadVisualizer:
    def __init__(self):
        self.config = load_config()
        self.visualization_config = self.config['visualization']
        self.timezone = pytz.timezone(self.config['data_settings']['timezones']['target'])
        self.setup_style()

    def setup_style(self):
        """Set up the plotting style"""
        sns.set_style("whitegrid")
        # Adjust figure size to 3:2 aspect ratio while staying under 2000x2000 pixels
        plt.rcParams['figure.figsize'] = [12, 8]  # 3:2 ratio

    def create_24h_chart(self, df, output_path=None, timezone=None):
        """Create a chart of the last 48 hours of load data"""
        try:
            # Use provided timezone or default to config
            tz = timezone if timezone is not None else self.timezone
            now = datetime.now(tz)
            two_days_ago = now - timedelta(days=2)
            
            # Convert UTC timestamps to target timezone for display
            plot_data = df.copy()
            plot_data['display_time'] = plot_data['interval_start_utc'].dt.tz_convert(tz)
            
            # Filter last 48 hours based on target timezone
            plot_data = plot_data[plot_data['display_time'] >= two_days_ago]
            
            # Split data into two 24-hour periods
            one_day_ago = now - timedelta(days=1)
            last_24h = plot_data[plot_data['display_time'] >= one_day_ago]
            previous_24h = plot_data[(plot_data['display_time'] < one_day_ago) & (plot_data['display_time'] >= two_days_ago)]
            
            # Create figure with specific background color
            fig = plt.figure(facecolor='white')
            ax = plt.gca()
            ax.set_facecolor('white')
            
            # Plot the data with a specific color
            plt.plot(plot_data['display_time'], 
                    plot_data['load.comed'], 
                    color='#40E0D0',  # Turquoise color
                    linewidth=2,
                    zorder=1)  # Ensure line is behind points
            
            # Define complementary colors for max/min points
            max_color = '#FF9E80'  # Coral/peach color
            min_color = '#FFEB3B'  # Yellow
            
            def format_time(dt):
                """Format datetime to include specific time"""
                return dt.strftime('%-I:%M %p').lower()
            
            def add_stats_box(stats, position='left'):
                """Add a box containing max/min stats"""
                bbox_props = dict(
                    boxstyle="round,pad=0.5",
                    fc="white",
                    ec="gray",
                    alpha=0.9
                )
                
                # Format the text for the box
                max_time = format_time(stats['max_time'])
                min_time = format_time(stats['min_time'])
                text = f"{stats['period']}\nMax Load: {int(stats['max_val']):,} MW at {max_time}\nMin Load: {int(stats['min_val']):,} MW at {min_time}"
                
                # Position the box at the bottom
                x = 0.02 if position == 'left' else 0.52
                
                # Add text box - always left aligned now
                ax.text(x, 0.02, text,
                       transform=ax.transAxes,
                       bbox=bbox_props,
                       ha='left',
                       va='bottom',
                       fontsize=10)
            
            # Add points for max/min values
            if not last_24h.empty:
                max_last = last_24h.loc[last_24h['load.comed'].idxmax()]
                min_last = last_24h.loc[last_24h['load.comed'].idxmin()]
                
                # Plot max/min points for last 24h
                plt.plot([max_last['display_time']], [max_last['load.comed']], 'o', 
                        color=max_color, markersize=8, zorder=2)
                plt.plot([min_last['display_time']], [min_last['load.comed']], 'o',
                        color=min_color, markersize=8, zorder=2)
                
                # Add stats box for last 24h
                add_stats_box({
                    'period': 'Last 24 hours',
                    'max_val': max_last['load.comed'],
                    'min_val': min_last['load.comed'],
                    'max_time': max_last['display_time'],
                    'min_time': min_last['display_time']
                }, position='right')

            if not previous_24h.empty:
                max_prev = previous_24h.loc[previous_24h['load.comed'].idxmax()]
                min_prev = previous_24h.loc[previous_24h['load.comed'].idxmin()]
                
                # Plot max/min points for previous 24h
                plt.plot([max_prev['display_time']], [max_prev['load.comed']], 'o',
                        color=max_color, markersize=8, zorder=2)
                plt.plot([min_prev['display_time']], [min_prev['load.comed']], 'o',
                        color=min_color, markersize=8, zorder=2)
                
                # Add stats box for previous 24h
                add_stats_box({
                    'period': 'Yesterday',
                    'max_val': max_prev['load.comed'],
                    'min_val': min_prev['load.comed'],
                    'max_time': max_prev['display_time'],
                    'min_time': min_prev['display_time']
                }, position='left')
            
            # Set y-axis limits
            min_load = plot_data['load.comed'].min()
            plt.ylim(min_load - 700, None)  # Set minimum 700 lower than data minimum
            
            # Main title with left alignment
            ax.text(0.0, 1.1, 'ComEd Grid Load', 
                    transform=ax.transAxes,
                    fontsize=16,
                    fontweight='bold')
            
            # Subtitle with left alignment
            ax.text(0.0, 1.05, 'Last 48 hours, 5 minute intervals (Megawatts)',
                    transform=ax.transAxes,
                    fontsize=12,
                    color='gray')
            
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
            plt.subplots_adjust(top=0.75, bottom=0.2)  # Decreased top margin to create more space above title
            
            # Use provided output path or default
            output_path = output_path or 'output/comed_load_48h.png'
            
            # Save with optimized settings for Bluesky
            plt.savefig(output_path, 
                       dpi=166,  # Optimized DPI to stay under 2000x2000 pixels (12*166=1992)
                       bbox_inches='tight',
                       facecolor='white',
                       format='png')  # Explicitly set PNG format
            plt.close()
            
        except Exception as e:
            logger.error(f"Error creating chart: {e}")
            plt.close()
            raise

    def _format_chart(self, timezone):
        """Apply chart formatting"""
        plt.gcf().autofmt_xdate()
        plt.tight_layout()
