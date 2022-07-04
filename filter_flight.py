import sqlite3

sqlite_connection = sqlite3.connect('flight.db')
cursor = sqlite_connection.cursor()


def filter_flight(date_from, date_to):
    return cursor.execute('SELECT * FROM flight WHERE depdate BETWEEN date(?) AND date(?);', (date_from, date_to))
