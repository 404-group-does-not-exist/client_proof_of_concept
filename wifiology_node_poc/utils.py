import statistics


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