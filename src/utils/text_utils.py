from atproto import client_utils

def create_load_post_text(stats, include_link=True):
    """Create formatted text for load post"""
    text_builder = client_utils.TextBuilder()
    
    # Build the main message
    post_text = (
        f"ComEd Load Report "
        f"({stats['start_time'].strftime('%I:%M %p')} - "
        f"{stats['end_time'].strftime('%I:%M %p')} CT)\n\n"
        f"Average Load: {stats['average']:,.0f} MW\n"
        f"Maximum Load: {stats['maximum']:,.0f} MW\n"
        f"Minimum Load: {stats['minimum']:,.0f} MW"
    )
    
    text_builder.text(post_text)
    
    if include_link:
        link = "https://www.pjm.com/markets-and-operations"
        text_builder.text("\n\nData source: ").link("PJM", link)
    
    return text_builder
