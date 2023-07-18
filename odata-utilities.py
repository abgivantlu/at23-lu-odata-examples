"""
A module for useful python functions
"""

from datetime import datetime
import re
from pydash import get

# This is taken from the dbo.SyStatus table
STATUS_HIERARCHY = {
    'NEW':	    0,
    'SCHED':	0,
    'INT':	    0,
    'NP':	    0,
    'PLACED':	0,
    'NA':	    0,
    'ATT':	    1,
    'NDS-ATT':	2,
    'PROB':	    3,
    'NDS-PROB':	4,
    'LOA':	    5,
    'SPN':	    7,
    'FUT':	    9,
    'NDS-FUT':	10,
    'BP':	    11,
    'NDS-BP':	12,
    'REENTRY':	13,
    'NDS-RE':	14,
    'APPREC':	16,
    'PENDAPP':	17,
    'GRAD':	    19,
    'COMPLETE':	21,
    'NDS-COMP':	22,
    'INC':	    23,
    'NDS-INC':	24,
    'DROP':	    25,
    'NDS-DROP':	26,
    'APPREJ':	31,
    'NDS-REJ':	32,
    'NOSHOW':	33,
    'NDS-NO':	34,
    'CANCEL':	35,
    'NDS-CAN':	36,
    'TOPROG':	37,
    'TOCAMP':	37,
    'FROMPROG':	37,
    'FROMCAMP':	37,
    'ISPLACED':	38,
    'ISNP':	    39,
    'ISNA':	    40
}

def parse_anthology_date(date_string:str) -> datetime.date:
    """
    Returns a date object that corresponds to a date string from Anthology.

    Follow these formats: '%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S%', or '%Y-%m-%dT%H:%M'.

    Example of an accepted string: '2022-09-10T12:30:00-0500'.
    """
    try:
        return parse_anthology_datetime(date_string).date()
    except AttributeError:
        return None

def parse_anthology_time(date_string:str) -> datetime.time:
    """
    Returns a time object that corresponds to a date string from Anthology.

    Follow these formats: '%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S%', or '%Y-%m-%dT%H:%M'.

    Example of an accepted string: '2022-09-10T12:30:00-0500'.
    """
    try:
        return parse_anthology_datetime(date_string).time()
    except AttributeError:
        return None
                
def parse_anthology_datetime(date_string:str) -> datetime:
    """
    Returns a datetime object that corresponds to a date string from Anthology.

    Follow these formats: '%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S%', or '%Y-%m-%dT%H:%M'.

    Example of an accepted string: '2022-09-10T12:30:00-0500'.
    """
    try:
        return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        # This will try to remove the ':' in the timezone info in the incoming date string
        # in order to comply with Python 3.6 datetime standards
        try:
            match_obj = re.search(r'[-+]\d\d:\d\d', date_string)
            date_string = date_string[:match_obj.start()] + date_string[match_obj.start():match_obj.end()].replace(':', '')
            return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S%z')
        except (ValueError, AttributeError):
            try:
                # This is to account for date strings with more than 6 digits of millisecond placeholders
                match_obj_seconds = re.search(r'[.]\d*', date_string)
                if match_obj_seconds:
                    date_string = date_string[:match_obj_seconds.start()] + date_string[match_obj_seconds.start():match_obj_seconds.end()][:7] + date_string[match_obj_seconds.end():]
                    return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%f%z')
                raise ValueError
            except ValueError:            
                try:
                    return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
                except ValueError:
                    try:
                        return datetime.strptime(date_string, '%Y-%m-%dT%H:%M')
                    except ValueError:
                        print(f"ValueError: '{date_string}' format not recognized. Parameter type: {type(date_string)}. Use '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S%', or '%Y-%m-%dT%H:%M'")
                        return None   

def parse_slate_date(date_str):
    """
    Parse a date string from Slate into a Date object
    """
    try:
        return datetime.strptime(date_str, '%Y/%m/%d').date()
    except ValueError:
        print(f"ValueError: '{date_str}' format not recognized. Use '%Y/%m/%d'")
        return None

def parse_anthology_get_datetime(date_str):
    """
    Parse a date string from using the Anthology API 'GET' command.

    Example string: '2023/01/01 00:00:00'
    """
    return datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S')

def get_active_and_admitted_category_filter(path_to_status_category:str) -> str:
    """
    This method generates and returns a string that can be used as
    an OData filter for Active and Admitted students
    """
    return (
        f"({path_to_status_category} eq 'A'" # Active
        f" or {path_to_status_category} eq 'E'" # Enrollment
        f" or {path_to_status_category} eq 'X'" # NDS-Enrollment
        f" or {path_to_status_category} eq 'Y')" # NDS-Active
    )

def get_active_category_filter(path_to_status_category:str) -> str:
    """
    This method generates and returns a string that can be used as
    an OData filter for Active students
    """
    return (
        f"({path_to_status_category} eq 'A'" # Active
        f" or {path_to_status_category} eq 'Y')" # NDS-Active
    )

def get_backload_filter(path_to_status_category:str) -> str:
    """
    This method generates and returns a string that can be used as
    an OData filter for backloading data
    """
    return (
        f"({path_to_status_category} eq 'A'" # Active
        f" or {path_to_status_category} eq 'E'" # Enrollment
        f" or {path_to_status_category} eq 'X'" # NDS-Enrollment
        f" or {path_to_status_category} eq 'Y'" # NDS-Active
        f" or {path_to_status_category} eq 'P'" # Permament Out
        f" or {path_to_status_category} eq 'Z')" # NDS-Permanent Out
    )

def get_current_attending_enrollment(enrollment_periods:list) -> dict:
    """
    This method will return the most recent enrollment period that has an attending status
    from a list of sorted enrollments. 
    
    If no enrollments have the attending status, the most recent one will be returned.
    This is to handle students who are currently attending a program but have
    been admitted to another program at Lipscomb.
    """
    if not enrollment_periods:
        raise ValueError("No enrollments were provided.")
    if not isinstance(enrollment_periods, list):
        enrollment_periods = [enrollment_periods]

    for enrollment in enrollment_periods:
        if not get(enrollment, 'SchoolStatus.SystemSchoolStatus.SystemStatusCategory'):
            raise KeyError("There is an enrollment that does not contain a System Status Category.")

    current_enrollment = enrollment_periods[0]
    if not get(current_enrollment, 'SchoolStatus.SystemSchoolStatus.SystemStatusCategory') in ('A', 'Y'):
        for enrollment in enrollment_periods[1:]:
            if get(enrollment, 'SchoolStatus.SystemSchoolStatus.SystemStatusCategory') in ('A', 'Y'):
                current_enrollment = enrollment
                break

    return current_enrollment

def parse_query(url: str):
    """
    Parses a query string into a dictionary where the keys are the query option 
    variable names and the values are the corresponing query option values.

    Example dict to be returned: {
        '$select': Id, CourseCode, 
        '$expand': Terms,
        '$orderby': LastModifiedDateTime
    }

    Arguments:
        url: The query string to be parsed

    Returns:
        query_dict: A dictionary containing data from the given query string.
    """
    query_dict = {}
    for param in url.query.split('&'):
        if param:
            [key, value] = param.split('=', 1)
            query_dict[key] = value

    return query_dict
    
def get_current_enrollment(enrollment_periods):
    """
    This function gets the current enrollment simliar to the SQL function 'dbo.if_AdCurrentEnrollment'

    Requires an enrollment period to include SchoolStatus.SystemSchoolStatus.Code and EnrollmentDate
    """
    if not enrollment_periods:
        raise ValueError("No enrollments were provided.")
    if not isinstance(enrollment_periods, list):
        enrollment_periods = [enrollment_periods]

    for enrollment in enrollment_periods:
        if not get(enrollment, 'SchoolStatus.SystemSchoolStatus.Code'):
            raise KeyError("There is an enrollment that does not contain a System Status Category.")
        if not get(enrollment, 'EnrollmentDate'):
            raise KeyError("There is an enrollment that does not contain an EnrollmentDate.")
    
    enrollment_periods.sort(reverse=True, key=lambda x: get(x, 'EnrollmentDate'))
    enrollment_periods.sort(key=lambda x: STATUS_HIERARCHY[get(x, 'SchoolStatus.SystemSchoolStatus.Code')])

    return enrollment_periods[0]
