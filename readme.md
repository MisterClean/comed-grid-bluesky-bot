# ComEd Load Bot

A Python application that monitors and reports ComEd region load data from the PJM Interconnection grid. The bot posts updates to Bluesky every 4 hours with load statistics and visualizations.

## Features

- Fetches ComEd zone load data from the GridStatus API
- Calculates load statistics for 4-hour periods
- Generates 24-hour load visualizations
- Posts updates to Bluesky
- Handles timezone conversions (UTC to Central Time)
- Comprehensive logging and error handling
- Configuration-driven behavior

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
├── main.py             # Application entry point
├── readme.md
├── requirements.txt
└── src/
    └── utils/
        ├── config.py   # Configuration loader
        └── logger.py   # Logging setup
    ├── data_loader.py  # GridStatus API interface
    ├── visualizer.py   # Chart generation
    ├── analyzer.py     # Data analysis
    └── bluesky_poster.py  # Bluesky integration
```

## Configuration

The application behavior can be customized through `config.yaml`:

```yaml
data_settings:
  days_back: 2          # How many days of historical data to fetch
  limit: 10000          # API request limit
  dataset: "pjm_load_5m"
  location: "COMED"

visualization:
  chart_width: 12
  chart_height: 6
  dpi: 300
  hour_interval: 3

posting:
  interval_hours: 4     # How often to post updates
```

## Usage

### Running the Bot

To run the bot once:
```bash
python main.py
```

### Scheduling Regular Updates

To run the bot every 4 hours, you can:

1. Use cron (Linux/Mac):
```bash
0 */4 * * * cd /path/to/comed-load-bot && ./venv/bin/python main.py
```

2. Use Task Scheduler (Windows):
Create a task that runs every 4 hours executing:
```bash
C:\path\to\comed-load-bot\venv\Scripts\python.exe C:\path\to\comed-load-bot\main.py
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
- 24-hour load visualization saved as `output/comed_load_24h.png`
- Shows load trends with 3-hour intervals
- Central Time zone
- Professional formatting with grid lines and clear labels

## API Usage Notes

- The free GridStatus API plan has a limit of 1 million rows per month
- The application is designed to be efficient with API calls by:
  - Only querying ComEd zone data
  - Using appropriate row limits
  - Minimizing API calls through smart data caching

## Logging

Logs are written to both stdout and `logs/comed_bot.log`:
- Rotates daily
- Retains 7 days of history
- Includes timestamps, log levels, and function names
- Detailed error tracking

## Development

### Adding New Features

1. Create new modules in `src/` for major features
2. Update configuration in `config.yaml` if needed
3. Add any new dependencies to `requirements.txt`
4. Update main.py to integrate new features

### Running Tests

```bash
# TODO: Add testing instructions once tests are implemented
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

- [GridStatus.io](https://www.gridstatus.io) for providing the API
- PJM Interconnection for the underlying data

## Support

For support, please:
1. Check the existing issues
2. Create a new issue with:
   - Detailed description of the problem
   - Relevant logs
   - Steps to reproduce