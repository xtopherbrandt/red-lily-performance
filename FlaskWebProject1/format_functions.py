from datetime import datetime, date, timedelta
import dateutil.parser

def format_datetime(value, format='date'):
    if format == 'date_time':
        format="%B %d, %Y at %I:%M %p"
    elif format == 'date':
        format="%B %d, %Y"
    return value.strftime(format)