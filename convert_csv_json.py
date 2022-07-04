import os
import time
import pandas as pd
from pydantic import ValidationError
import json
import shutil
import numpy as np
from loguru import logger

from db import db_write

# получаем список файлов в каталоге
files_name = os.listdir('In')

file_source = 'In'
file_destination = 'Out'
file_ok = 'Ok'
file_error = 'Err'


def convert(object):

    ''' вспомогательная функция для изменения датафрейма нампай из int64 в int '''

    if isinstance(object, np.int64):
        return int(object)
    raise TypeError


def convert_func():

    ''' Конвертация файлов csv в json и перенос в папку "Ok" '''

    while True:
        if files_name:
            logger.info(f'В папке есть новые файлы: {files_name}')
            for file in files_name:
                try:

                    # проверка наличия всех рабочих папок, если их нет - создаем
                    path_dest_exist = os.path.exists(file_destination)
                    if not path_dest_exist:
                        os.makedirs(file_destination)
                        logger.info(f'Папка {file_destination} создана')

                    path_err_exist = os.path.exists(file_error)
                    if not path_err_exist:
                        os.makedirs(file_error)
                        logger.info(f'Папка {file_error} создана')

                    path_ok_exist = os.path.exists(file_ok)
                    if not path_ok_exist:
                        os.makedirs(file_ok)
                        logger.info(f'Папка {file_ok} создана')

                    # Обработка имени файла
                    text_name_file = file.replace('.csv', '').split('_')

                    # создаем пустой словарь
                    for_json = {}

                    # заполняем основную часть словаря
                    for_json['flt'] = int(text_name_file[1])
                    for_json['date'] = pd.to_datetime(text_name_file[0]).strftime('%Y-%m-%d')
                    for_json['dep'] = text_name_file[2]
                    for_json['prl'] = []

                    # Формируем из csv датафрейм
                    df = pd.read_csv(f'{file_source}/' + file, sep=';')

                    # заполнем значением ключ prl
                    col = df.columns

                    for i in range(df.shape[0]):

                        id_pers = {}

                        for col_name in col:
                            if col_name == 'bdate':
                                id_pers[col_name] = pd.to_datetime(df[col_name].iloc[i]).strftime('%Y-%m-%d')
                            else:
                                id_pers[col_name] = df[col_name].iloc[i]

                        for_json['prl'].append(id_pers)

                    # Сохраняем файл

                    name_json = file.replace('csv', 'json')

                    with open(f'{file_destination}/' + name_json, 'w') as outfile:
                        json.dump(for_json, outfile, default=convert)

                    # сохраняем в БД
                    db_write(file, for_json['flt'], for_json['date'], for_json['dep'])

                    # перемещаем обработанный csv в папку "Ок"
                    shutil.move(f'{file_source}/' + file, file_ok)

                    logger.info(f'Файл {file} обработан!')

                # отлавливаем возможные ошибки на не предоставленые данные
                except ValidationError:
                    shutil.move(f'{file_source}/' + file, file_error)
                    logger.error(f'Ошибка {ValidationError}')

                # не верный формат данных
                except ValueError:
                    shutil.move(f'{file_source}/' + file, file_error)
                    logger.error(f'Ошибка {ValueError}')

                # не верный формат файла
                except IndexError:
                    shutil.move(f'{file_source}/' + file, file_error)
                    logger.error(f'Ошибка {IndexError}')


        # делаем паузу 2 минуты до следующей проверки папки на новые файлы
        logger.info(f'Папка {file_source} пустая')
        time.sleep(120)


print(convert_func())
