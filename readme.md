# ComEd Grid & Nuclear Bot

A Python application that monitors and reports on the ComEd region grid, including both load data from PJM Interconnection and nuclear generation data from multiple authoritative sources. The bot posts updates to Bluesky every 4 hours with comprehensive grid statistics and visualizations.

## Features

### Grid Load Monitoring
- Fetches ComEd zone load data from the PJM Interconnection grid via GridStatus API
- Calculates load statistics for 4-hour periods
- Generates 24-hour load visualizations with customizable formatting

### Nuclear Generation Tracking
- Integrates real-time nuclear reactor status data from the NRC
- Combines with EIA capacity data to estimate actual nuclear generation
- Tracks 10 nuclear reactors across 5 plants in the ComEd region:
  - Braidwood (Units 1 & 2)
  - Byron (Units 1 & 2)
  - Dresden (Units 2 & 3)
  - LaSalle (Units 1 & 2)
  - Quad Cities (Units 1 & 2)

### Data Management
- SQLite database for persistent storage of historical data
- Automatic data validation and deduplication
- Smart seasonal capacity calculations based on time of year
- Comprehensive timezone handling (UTC to Central Time)

### Reporting
- Posts updates to Bluesky with optional source link inclusion
- Customizable posting intervals for different data types
- Smart file management (auto-cleanup of old chart files)
- Automatic retry mechanism for failed posts

## Prerequisites

- Python 3.9+
- API Keys:
  - GridStatus API key (create account at [gridstatus.io](https://www.gridstatus.io))
  - EIA API key (obtain from [eia.gov](https://www.eia.gov/opendata/))
- Bluesky account credentials

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/comed-grid-bluesky-bot.git
cd comed-grid-bluesky-bot
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
EIA_API_KEY=your_eia_api_key
BLUESKY_USERNAME=your_bluesky_username
BLUESKY_PASSWORD=your_bluesky_password
```

## Configuration

The application behavior is customized through `config.yaml`:

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

posting:
  interval_hours: 4    # How often to post updates
  include_images: true
  retry_attempts: 3    # Number of retry attempts for failed posts
  include_source_link: true
  processes:
    load:
      enabled: true    # Enable/disable load data posts
    nuclear:
      enabled: true    # Enable/disable nuclear data posts

nuclear_data:
  nrc:
    url: "https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/powerreactorstatusforlast365days.txt"
    plants:           # Nuclear units to track
      - "Braidwood 1"
      - "Braidwood 2"
      - "Byron 1"
      - "Byron 2"
      - "Dresden 2"
      - "Dresden 3"
      - "LaSalle 1"
      - "LaSalle 2"
      - "Quad Cities 1"
      - "Quad Cities 2"
  eia:
    plant_ids:        # EIA plant identifiers
      - "6022"  # Braidwood
      - "6023"  # Byron
      - "869"   # Dresden
      - "6026"  # LaSalle
      - "880"   # Quad Cities
```

## Data Sources

### Grid Load Data
- Source: PJM Interconnection via GridStatus API
- Frequency: 5-minute intervals
- Metrics: Real-time load in MW

### Nuclear Generation Data
1. NRC Power Reactor Status Reports
   - Source: Nuclear Regulatory Commission
   - Updates: Daily
   - Metrics: Reactor power percentage

2. EIA Plant Capacity Data
   - Source: Energy Information Administration
   - Updates: Monthly
   - Metrics: Net Summer/Winter capacity

## Database Schema

The application maintains a SQLite database (`grid_data.db`) with the following tables:

1. `grid_load` - Stores ComEd zone load data
2. `nrc_status` - Stores daily reactor status reports
3. `eia_capacity` - Stores monthly plant capacity data

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
0 */4 * * * cd /path/to/comed-grid-bluesky-bot && ./venv/bin/python run.py
```

2. Use Task Scheduler (Windows):
Create a task that runs every 4 hours executing:
```bash
C:\path\to\comed-grid-bluesky-bot\venv\Scripts\python.exe C:\path\to\comed-grid-bluesky-bot\run.py
```

## Output Examples

The bot generates several types of output:

1. Load Statistics:
```
ComEd Load Report (2:00 PM - 6:00 PM CT)
Average Load: 12,345 MW
Maximum Load: 13,567 MW
Minimum Load: 11,234 MW
```

2. Nuclear Generation:
```
Nuclear Fleet Status
Total Generation: 11,234 MW
Units at Full Power: 8
Units in Maintenance: 2
Fleet Capacity Factor: 93.5%
```

3. Visual Charts:
- 24-hour load visualization
- Nuclear generation trends
- Professional formatting with grid lines and clear labels
- Customizable dimensions and DPI settings

## Error Handling

- Comprehensive error logging with traceback
- Automatic retry mechanism for failed posts
- Data validation and sanitization
- API failure handling for multiple data sources
- Database transaction management

## Logging

Logs are written to both stdout and `logs/comed_bot.log`:
- Daily rotation with 7-day retention
- Detailed error tracking with tracebacks
- Separate logging for data fetching and posting processes
- Performance metrics for API calls and database operations

## Development

### Adding New Features

1. Create new modules in `src/` for major features
2. Update configuration in `config.yaml` if needed
3. Add any new dependencies to `requirements.txt`
4. Update database schema if required
5. Add appropriate tests

### Running Tests

```bash
python -m pytest src/tests/
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

- [GridStatus.io](https://www.gridstatus.io) for the PJM data API
- [Nuclear Regulatory Commission](https://www.nrc.gov) for reactor status data
- [Energy Information Administration](https://www.eia.gov) for capacity data
- PJM Interconnection for the underlying grid data
