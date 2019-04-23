import time

def convert_time(sec_time):
    """
    Function that converts seconds into hours, minutes, and seconds.
    """
    mins, secs = divmod(sec_time, 60)
    hours, mins = divmod(mins, 60)
    return hours, mins, secs

def timer(process):
    def shell_timer(_function):
        """
        Decorator for computing the length of time a process takes.
        """
        def decorated(*args, **kwargs):
            start_time = time.time()
            result = _function(*args, **kwargs)
            total_time = time.time() - start_time
            h, m, s = convert_time(total_time)
            print(f'{process} completed in %d:%02d:%02d' % (h, m, s)) 
            return result
        return decorated
    return shell_timer