from typing import List

from .connector import StoreConnector
from pandas import DataFrame, Series
from datetime import datetime

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.
    
    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""


def select_all_from_source_files(connector: StoreConnector) -> List[tuple]:
    """ Вывод списка обработанных файлов с сортировкой по дате в порядке убывания (DESCENDING) """
    query = f'SELECT * FROM source_files ORDER BY processed DESC'
    result = connector.execute(query).fetchall()
    return result


def insert_into_source_files(connector: StoreConnector, filename: str):
    """ Вставка в таблицу обработанных файлов """
    now = datetime.now()        # текущая дата и время
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")   # преобразуем дату в формат SQL, например, '2022-11-15 22:03:16'
    query = f'INSERT INTO source_files (filename, processed) VALUES (\'{filename}\', \'{date_time}\')'
    result = connector.execute(query)
    return result


def insert_rows_into_processed_data(connector: StoreConnector, dataframe: DataFrame):
    """ Вставка строк из DataFrame в БД с привязкой данных к последнему обработанному файлу (по дате) """
    rows = dataframe.to_dict('records')
    files_list = select_all_from_source_files(connector)    # получаем список обработанных файлов
    # т.к. строка БД после выполнения SELECT возвращается в виде объекта tuple, например:
    # row = (1, 'seeds_dataset.csv', '2022-11-15 22:03:16'),
    # то значение соответствующей колонки можно получить по индексу, например id = row[0]
    last_file_id = files_list[0][0]  # получаем индекс последней записи из таблицы с файлами
    if len(files_list) > 0:
        for row in rows:
            connector.execute(f'INSERT INTO wood_removal ('
            f'country, '
            f'year1990, '
            f'year1991, '
            f'year1992, '
            f'year1993, '
            f'year1994, '
            f'year1995, '
            f'year1996, '
            f'year1997, '
            f'year1998, '
            f'year1999, '
            f'year2000, '
            f'year2001, '
            f'year2002, '
            f'year2003, '
            f'year2004, '
            f'year2005, '
            f'year2006, '
            f'year2007, '
            f'year2008, '
            f'year2009, '
            f'year2010, '
            f'year2011, '
            f'source_file)'
            f'VALUES ('
            f'\'{row["country"]}\', '
            f'\'{row["1990"]}\', '
            f'\'{row["1991"]}\', '
            f'\'{row["1992"]}\', '
            f'\'{row["1993"]}\', '
            f'\'{row["1994"]}\', '
            f'\'{row["1995"]}\', '
            f'\'{row["1996"]}\', '
            f'\'{row["1997"]}\', '
            f'\'{row["1998"]}\', '
            f'\'{row["1999"]}\', '
            f'\'{row["2000"]}\', '
            f'\'{row["2001"]}\', '
            f'\'{row["2002"]}\', '
            f'\'{row["2003"]}\', '
            f'\'{row["2004"]}\', '
            f'\'{row["2005"]}\', '
            f'\'{row["2006"]}\', '
            f'\'{row["2007"]}\', '
            f'\'{row["2008"]}\', '
            f'\'{row["2009"]}\', '
            f'\'{row["2010"]}\', '
            f'\'{row["2011"]}\', '
            f'{last_file_id})')
        print('Data was inserted successfully')
    else:
        print('File records not found. Data inserting was canceled.')
