#logs

import datetime

total_new_rows = 0
total_updated_rows = 0

start_time: datetime
end_time: datetime

#total_new_rows
def get_total_new_rows():
    global total_new_rows
    return total_new_rows

def set_total_new_rows(value: int):
    global total_new_rows
    total_new_rows = value

#total_updated_rows
def get_total_updated_rows():
    global total_updated_rows
    return total_updated_rows

def set_total_updated_rows(value: int):
    global total_updated_rows
    total_updated_rows = value

#start_time
def get_start_time():
    global start_time
    return start_time

def set_start_time(value: datetime):
    global start_time
    start_time = value

#end_time
def get_end_time():
    global end_time
    return end_time

def set_end_time(value: datetime):
    global end_time
    end_time = value
