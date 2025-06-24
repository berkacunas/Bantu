import julian
import datetime
import dateutil.tz

DEFAULT_DATETIME_FORMAT = '"%Y-%m-%d %H:%M:%S.%f"'
DEFAULT_JULIAN_FORMAT = 'jd'

def get_timezone_diff() -> int:

    localtz = dateutil.tz.tzlocal()
    localoffset = localtz.utcoffset(datetime.datetime.now(localtz))
    
    return int(localoffset.total_seconds() / 3600)


def is_valid_dt_format(datetime_str, format=DEFAULT_DATETIME_FORMAT) -> bool:
    
    valid = True
    try:
        valid = bool(datetime.datetime.strptime(datetime_str, format))
    except ValueError:
        valid = False
        
    return valid

def to_julian(dt) -> float:
    
    return julian.to_jd(dt, fmt=DEFAULT_JULIAN_FORMAT)

def from_julian(jul, format=DEFAULT_JULIAN_FORMAT) -> datetime.datetime:
    '''Format jd or mjd'''
    return julian.from_jd(jul, fmt=format)

def is_julian(jul, format=DEFAULT_JULIAN_FORMAT) -> bool:

    try:
        dt = from_julian(jul, format)
    except:
        return False
    
    return isinstance(dt, datetime.datetime)

def main():

    jul_datetime = to_julian(datetime.datetime(2025, 6, 3, 12, 8, 12))
    dt = from_julian(jul_datetime)
    
    print(f"Julian: {jul_datetime}")
    print(f"Back to datetime: {dt}")
    
    print(get_timezone_diff())        

if __name__ == '__main__':
    
    main()

    
    