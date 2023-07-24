##################################################################################################
##                        Raspberry Pi Passenger Counter 
###################################################################################################

import mysql.connector
import RPi.GPIO as GPIO
import time

sensor_pin = 2
counter = 0
operator_name = 'ASTRA GmbH'
vehicle_num = 111222333

# Function to UPDATE the counter value of the 'passenger_count_epochtime' table
def update_counter():
    connection = mysql.connector.connect(**db_config)
    mycursor = connection.cursor()
    update_counter_query = 'UPDATE passenger_count_epochtime SET count = count + 1, epoch_time = %s WHERE operator = %s AND vehicle_id = %s'
    eptime = int(time.time())
    mycursor.execute(update_counter_query, (eptime,operator_name,vehicle_num))
    connection.commit()
    mycursor.close()
    connection.close()

# Function to INSERT INTO time-series table for Grafana called 'passenger_count_epochtime_grafana'
def insert_timeseries_table():
    # Connect to the 'passenger_count_epochtime' table and read current counter value
    connection = mysql.connector.connect(**db_config)
    mycursor = connection.cursor()
    current_count_query = 'SELECT count FROM passenger_count_epochtime WHERE operator = %s'
    mycursor.execute(current_count_query, (operator_name, ))
    current_count = mycursor.fetchone()[0]
    eptime = int(time.time())

    # Connect to passenger_count_epochtime_grafana table and insert a new row
    # with current counter value and current epoch time
    insert_timeseries_table_query = 'INSERT INTO passenger_count_epochtime_grafana (operator, vehicle_id, count, epoch_time) VALUES (%s, %s, %s, %s)'        
    mycursor.execute(insert_timeseries_table_query, (operator_name,vehicle_num,current_count,eptime))
    connection.commit()
    mycursor.close()
    connection.close()

def increment_counter(channel):
    global counter
    counter += 1
    print("Passenger Detected. Local counter: ", counter)
    print("channel: ", channel)
    update_counter()
    insert_timeseries_table()

def main():
    # Use the pin numbering scheme BCM (Broadcom)
    GPIO.setmode(GPIO.BCM)

    # Define GPIO pin as an input pin
    GPIO.setup(sensor_pin, GPIO.IN)

    # Identify the voltage drop in GPIO pin
    # When detects, run callback function using 2nd thread, in parallel to main function
    # https://sourceforge.net/p/raspberry-gpio-python/wiki/Inputs/
    GPIO.add_event_detect(sensor_pin, GPIO.FALLING, callback=increment_counter, bouncetime=200)


if __name__ == "__main__":
    main()
