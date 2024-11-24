from src.utils.database import DatabaseManager
from src.visualizer import LoadVisualizer
from datetime import datetime, timedelta
import pytz

def test_visualization():
    # Initialize components
    db = DatabaseManager()
    visualizer = LoadVisualizer()
    
    # Get data from the last 48 hours
    end_time = datetime.now(pytz.UTC)
    start_time = end_time - timedelta(days=2)
    
    # Get existing data from database
    df = db.get_data_since(start_time.isoformat())
    
    if not df.empty:
        # Generate test visualization
        test_output = 'output/test_visualization.png'
        visualizer.create_24h_chart(df, output_path=test_output)
        print(f"Test visualization created at: {test_output}")
    else:
        print("No data found in database")

if __name__ == "__main__":
    test_visualization()
