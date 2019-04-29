import statistics
import string


def altered_mean(data):
    if not data:
        return None
    elif len(data) == 1:
        return data[0]
    else:
        return statistics.mean(data)


def altered_stddev(data):
    if not data:
        return None
    elif len(data) == 1:
        return 0.0
    else:
        return statistics.stdev(data)


def bytes_to_str(b):
    if isinstance(b, (bytes, bytearray)):
        try:
            result = b.decode('ascii')
            if not all(c in string.printable for c in result):
                result = repr(b)[2:-1]
        except:
            result = repr(b)[2:-1]
    else:
        result = b
    return result
