# ComEd Load Bot

A Python application that monitors and reports ComEd region load data from the PJM Interconnection grid. The bot posts updates to Bluesky every 4 hours with load statistics and visualizations. Data is provided by [GridStatus.io](https://www.gridstatus.io), a comprehensive API for accessing power grid data.

## Features

- Fetches ComEd zone load data from the PJM Interconnection grid via GridStatus API
- Calculates load statistics for 4-hour periods
- Generates 24-hour load visualizations with:
  - Custom styling and themes
  - Dynamic color schemes based on load levels
  - Configurable chart dimensions and DPI
  - Automatic grid line and label formatting
  - Smart timestamp handling
  - Flexible hour interval markers
- Posts updates to Bluesky with optional source link inclusion
- Smart file management (auto-cleanup of old chart files)
- Comprehensive timezone handling (UTC to Central Time)
- Configurable data fetching with initial historical load
- Automatic retry mechanism for failed posts
- Comprehensive logging and error handling
- Configuration-driven behavior
- SQLite database for efficient data storage and retrieval
- Robust data analysis with statistical computations
- Flexible text formatting for social media posts

## Prerequisites

- Python 3.9+
- GridStatus API key (create account at [gridstatus.io](https://www.gridstatus.io))
- Bluesky account credentials

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/comed-load-bot.git
cd comed-load-bot
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
# Create a .env file in the project root
GRIDSTATUS_API_KEY=your_gridstatus_api_key
BLUESKY_USERNAME=your_bluesky_username
BLUESKY_PASSWORD=your_bluesky_password
```

## Project Structure

```
.
├── __init__.py
├── config.yaml          # Application configuration
├── readme.md
├── requirements.txt
├── run.py              # Application entry point
└── src/
    ├── utils/
    │   ├── config.py   # Configuration loader
    │   ├── database.py # Database operations
    │   ├── logger.py   # Logging setup
    │   └── text_utils.py # Text formatting utilities
    ├── data_loader.py  # GridStatus API interface
    ├── visualizer.py   # Chart generation
    ├── analyzer.py     # Data analysis
    ├── interfaces.py   # Type definitions
    └── bluesky_poster.py  # Bluesky integration
```

## Configuration

The application behavior can be customized through `config.yaml`:

```yaml
data_settings:
  days_back: 1          # Regular data fetch window
  initial_days_back: 3  # Initial historical data fetch
  limit: 10000         # API request limit
  dataset: "pjm_standardized_5_min"
  columns:             # Specific columns to fetch
    - "interval_start_utc"
    - "interval_end_utc"
    - "load.comed"
  timezones:
    source: "UTC"
    target: "America/Chicago"

visualization:
  chart_width: 12
  chart_height: 6
  dpi: 300
  hour_interval: 3    # Hour markers on x-axis
  style:
    grid: true
    theme: "seaborn"
    color_scheme: "viridis"

posting:
  interval_hours: 4    # How often to post updates
  include_images: true
  retry_attempts: 3    # Number of retry attempts for failed posts
  include_source_link: true  # Include data source link in posts
```

## Usage

### Running the Bot

To run the bot once:
```bash
python run.py
```

### Scheduling Regular Updates

To run the bot every 4 hours, you can:

1. Use cron (Linux/Mac):
```bash
0 */4 * * * cd /path/to/comed-load-bot && ./venv/bin/python run.py
```

2. Use Task Scheduler (Windows):
Create a task that runs every 4 hours executing:
```bash
C:\path\to\comed-load-bot\venv\Scripts\python.exe C:\path\to\comed-load-bot\run.py
```

## Output Examples

The bot generates two types of output:

1. Statistical Summary:
```
ComEd Load Report (2:00 PM - 6:00 PM CT)

Average Load: 12,345 MW
Maximum Load: 13,567 MW
Minimum Load: 11,234 MW
```

2. Visual Chart:
- 24-hour load visualization with timestamp in filename
- Shows load trends with configurable hour intervals
- Central Time zone with proper timezone handling
- Professional formatting with grid lines and clear labels
- Customizable dimensions and DPI settings
- Dynamic color schemes based on load levels
- Automatic axis scaling and formatting

## Data Management

- SQLite database for efficient data storage
- Automatic cleanup of old chart files (keeps 5 most recent)
- Initial data load fetches 3 days of historical data
- Regular updates fetch 24-hour windows
- Proper timezone handling between UTC and Central Time
- Configurable dataset and column selection

## API Usage Notes

- The free GridStatus API plan has a limit of 1 million rows per month
- The application is designed to be efficient with API calls by:
  - Only querying ComEd zone data
  - Using appropriate row limits
  - Minimizing API calls through smart data caching
  - Configurable data fetch windows
  - Database storage to prevent redundant fetches

## Error Handling

- Comprehensive error logging with traceback
- Automatic retry mechanism for failed posts (configurable attempts)
- Graceful handling of API failures
- File system error handling for chart generation
- Timezone conversion error handling
- Database transaction management

## Logging

Logs are written to both stdout and `logs/comed_bot.log`:
- Rotates daily
- Retains 7 days of history
- Includes timestamps, log levels, and function names
- Detailed error tracking with full tracebacks
- SQL query logging for debugging

## Development

### Adding New Features

1. Create new modules in `src/` for major features
2. Update configuration in `config.yaml` if needed
3. Add any new dependencies to `requirements.txt`
4. Update interfaces.py with new type definitions
5. Add appropriate error handling and logging
6. Update database schema if needed

### Running Tests

```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details

## Acknowledgments

This project would not be possible without:

- [GridStatus.io](https://www.gridstatus.io) - The backbone of this project, providing comprehensive and reliable power grid data through their excellent API
- PJM Interconnection for the underlying ComEd zone load data
- The Python community for the excellent libraries that power this bot

## Support

For support, please:
1. Check the existing issues
2. Create a new issue with:
   - Detailed description of the problem
   - Relevant logs
   - Steps to reproduce
