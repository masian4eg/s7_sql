import sqlite3
from os.path import isfile
from loguru import logger


def db_write(file_name: str, flt: int, depdate: str, dep: str):

    ''' внесение данных в БД '''

    sqlite_connection = sqlite3.connect('flight.db')
    cursor = sqlite_connection.cursor()

    # если база еще не создана - создаем
    if not isfile('flight.db'):
        try:

            sqlite_create_table_query = '''CREATE TABLE flight (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        file_name TEXT NOT NULL,
                                        flt INTEGER NOT NULL,
                                        depdate DATE NOT NULL,
                                        dep TEXT NOT NULL);'''

            cursor.execute(sqlite_create_table_query)
            sqlite_connection.commit()
            cursor.close()
            logger.info(f'База данных flight.db создана')

        # отлавливаем ошибки
        except sqlite3.Error as error:
            logger.error(f'Ошибка БД: {error}')

        # вносим первую запись
        finally:
            cursor.execute('''INSERT INTO flight (file_name, flt, depdate, dep) VALUES (?, ?, ?, ?);''',
                           (file_name, flt, depdate, dep))
            sqlite_connection.commit()
            cursor.close()
            logger.info(f'Внесены данные файла {file_name}')

    # если база уже создана - просто вносим новые данные
    else:
        cursor.execute('''INSERT INTO flight (file_name, flt, depdate, dep) VALUES (?, ?, ?, ?);''',
                       (file_name, flt, depdate, dep))
        sqlite_connection.commit()
        cursor.close()
        logger.info(f'Внесены данные файла {file_name}')
