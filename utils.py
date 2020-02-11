from datetime import datetime


def attendance(value):
    if value == '○':
        return 'Present'
    elif value == '●':
        return 'Absent'
    elif value == '◐':
        return 'Late'
    elif value == '□':
        return 'Evening Present'
    elif value == '▣':
        return 'Evening Late'
    else:
        return ''


def format_int(val):
    if val != '':
        return int(val)
    else:
        return None


def format_date(value):
    value = value + str(datetime.now().year)
    return datetime.strptime(value, '%d/%m%Y').date()
