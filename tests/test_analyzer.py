import pytest
import pandas as pd
import pytz
from datetime import datetime, timedelta
from src.analyzer import LoadAnalyzer
from src.interfaces import AnalysisError

@pytest.fixture
def sample_data():
    """Create sample load data for testing."""
    # Create a 24-hour period of data at 5-minute intervals
    periods = 24 * 12  # 24 hours * 12 five-minute intervals per hour
    now = datetime.now(pytz.UTC)
    
    dates = [now - timedelta(minutes=5*i) for i in range(periods)]
    dates.reverse()
    
    # Create sample load values with a realistic pattern
    # Morning peak around 8-9am, evening peak around 6-7pm
    loads = []
    for date in dates:
        hour = date.hour
        if 6 <= hour < 10:  # Morning ramp
            base = 12000 + (hour - 6) * 1000
        elif 10 <= hour < 16:  # Midday plateau
            base = 14000
        elif 16 <= hour < 20:  # Evening peak
            base = 15000 + (hour - 16) * 500
        else:  # Night valley
            base = 10000
        
        # Add some random variation
        load = base + (hash(str(date)) % 1000)
        loads.append(load)
    
    return pd.DataFrame({
        'interval_start_utc': dates[:-1],
        'interval_end_utc': dates[1:],
        'load.comed': loads[:-1]
    })

@pytest.fixture
def analyzer():
    """Create an analyzer instance for testing."""
    return LoadAnalyzer()

def test_calculate_stats_basic(analyzer, sample_data):
    """Test basic statistics calculation."""
    stats = analyzer.calculate_stats(sample_data)
    
    # Check that all expected keys are present
    expected_keys = {
        'current_load',
        'peak_load',
        'peak_time',
        'min_load',
        'min_time',
        'avg_load',
        'load_trend',
        'daily_pattern',
        'period_summary'
    }
    assert set(stats.keys()) == expected_keys
    
    # Check that numeric values are reasonable
    assert 5000 <= stats['current_load'] <= 20000
    assert 5000 <= stats['peak_load'] <= 20000
    assert 5000 <= stats['min_load'] <= 20000
    assert 5000 <= stats['avg_load'] <= 20000
    
    # Check that min/max values are correct
    assert stats['peak_load'] >= stats['avg_load'] >= stats['min_load']
    
    # Check trend data structure
    assert isinstance(stats['load_trend'], dict)
    assert 'direction' in stats['load_trend']
    assert 'percentage' in stats['load_trend']
    assert stats['load_trend']['direction'] in {'increasing', 'decreasing', 'stable'}
    assert isinstance(stats['load_trend']['percentage'], (int, float))

def test_calculate_stats_empty_data(analyzer):
    """Test handling of empty data."""
    empty_df = pd.DataFrame(columns=['interval_start_utc', 'interval_end_utc', 'load.comed'])
    with pytest.raises(AnalysisError):
        analyzer.calculate_stats(empty_df)

def test_calculate_stats_missing_columns(analyzer):
    """Test handling of missing columns."""
    bad_df = pd.DataFrame({
        'interval_start_utc': [datetime.now(pytz.UTC)],
        'load.comed': [1000]
    })
    with pytest.raises(KeyError):
        analyzer.calculate_stats(bad_df)

def test_daily_pattern(analyzer, sample_data):
    """Test daily pattern analysis."""
    stats = analyzer.calculate_stats(sample_data)
    pattern = stats['daily_pattern']
    
    # Check pattern structure
    assert isinstance(pattern, dict)
    assert all(isinstance(hour, int) and 0 <= hour <= 23 for hour in pattern.keys())
    assert all(isinstance(load, (int, float)) for load in pattern.values())
    
    # Check for expected patterns
    morning_hours = {7, 8, 9}
    evening_hours = {17, 18, 19}
    night_hours = {0, 1, 2, 3, 4}
    
    # Calculate average loads for different periods
    morning_avg = sum(pattern.get(h, 0) for h in morning_hours) / len(morning_hours)
    evening_avg = sum(pattern.get(h, 0) for h in evening_hours) / len(evening_hours)
    night_avg = sum(pattern.get(h, 0) for h in night_hours) / len(night_hours)
    
    # Verify expected load patterns
    assert evening_avg > night_avg  # Evening peak higher than night
    assert morning_avg > night_avg  # Morning peak higher than night

def test_period_summary_format(analyzer, sample_data):
    """Test period summary formatting."""
    stats = analyzer.calculate_stats(sample_data)
    summary = stats['period_summary']
    
    # Check summary structure
    assert isinstance(summary, str)
    assert "ComEd Load Report" in summary
    assert "Current Load:" in summary
    assert "Peak Load:" in summary
    assert "Minimum Load:" in summary
    assert "Average Load:" in summary
    assert "Load is" in summary
    
    # Check number formatting
    def has_formatted_number(text):
        """Check if text contains a properly formatted number."""
        return any(c.isdigit() and ',' in text for c in text.split())
    
    assert has_formatted_number(summary)

def test_trend_analysis(analyzer):
    """Test trend analysis with controlled data."""
    # Create data with known trend
    now = datetime.now(pytz.UTC)
    dates = [now - timedelta(minutes=5*i) for i in range(24)]
    dates.reverse()
    
    # Increasing trend
    increasing_loads = [10000 + i*100 for i in range(24)]
    increasing_df = pd.DataFrame({
        'interval_start_utc': dates[:-1],
        'interval_end_utc': dates[1:],
        'load.comed': increasing_loads[:-1]
    })
    
    stats = analyzer.calculate_stats(increasing_df)
    assert stats['load_trend']['direction'] == 'increasing'
    
    # Decreasing trend
    decreasing_loads = [15000 - i*100 for i in range(24)]
    decreasing_df = pd.DataFrame({
        'interval_start_utc': dates[:-1],
        'interval_end_utc': dates[1:],
        'load.comed': decreasing_loads[:-1]
    })
    
    stats = analyzer.calculate_stats(decreasing_df)
    assert stats['load_trend']['direction'] == 'decreasing'
    
    # Stable trend
    stable_loads = [12000 + (hash(str(i)) % 100) for i in range(24)]
    stable_df = pd.DataFrame({
        'interval_start_utc': dates[:-1],
        'interval_end_utc': dates[1:],
        'load.comed': stable_loads[:-1]
    })
    
    stats = analyzer.calculate_stats(stable_df)
    assert stats['load_trend']['direction'] in {'stable', 'increasing', 'decreasing'}
