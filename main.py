import mysql.connector
import csv
import datetime
# from mysql.connector import Error

conn = mysql.connector.connect(host='127.0.0.1', database='movies', user='root', password='mynewpassword',
                               auth_plugin='mysql_native_password')


def read_csv_file(csv_file):
    with open(csv_file, 'r') as InputFile:
        raw_data = []
        rv_data = csv.DictReader(InputFile, delimiter=';')
        for dataset1 in rv_data:
            raw_data.append(dataset1)
        return raw_data


def validate(date_text):
    try:
        datetime.date.fromisoformat(date_text)
    except ValueError:
        if '-' not in date_text or '-0-' in date_text:
            return '9999-12-31'
        else:
            rv_list = date_text.split('-')
            for digit in rv_list:
                if len(digit) == 4 or len(digit) == 2:
                    continue
                else:
                    ID = rv_list.index(digit)
                    value = rv_list.pop(ID)
                    value = '0' + value
                    rv_list.insert(ID, value)
            rv_list = '-'.join(rv_list)
            return rv_list


def clear_data(untidy_data, column_name, list_stopgap_1, list_stopgap_2, list_stopgap_3):
    if column_name == 'date_of_birth' or column_name == 'release_date':
        for dset in untidy_data:
            date = dset[column_name]
            rv_date = validate(date)
            if rv_date is None:
                pass
            else:
                dset[column_name] = rv_date
            for key2 in dset:
                if dset[key2] == '':
                    if key2 in list_stopgap_1:
                        dset[key2] = 'NO ENTRY!'
                    elif key2 in list_stopgap_2:
                        dset[key2] = '9999-12-31'
                    elif key2 in list_stopgap_3:
                        dset[key2] = '0'
    else:
        for dset in untidy_data:
            for key2 in dset:
                if dset[key2] == '':
                    if key2 in list_stopgap_1:
                        dset[key2] = 'NO ENTRY!'
                    elif key2 in list_stopgap_2:
                        dset[key2] = '00:00:00'
                    elif key2 in list_stopgap_3:
                        dset[key2] = '0'


def dict_to_sql_and_insert(db_input, tabel_name):
    for i, dataset2 in enumerate(db_input, start=1):
        sql_statement = 'INSERT INTO ' + tabel_name + ' '
        keys = '('
        values = '('
        for key, value in dataset2.items():
            keys += key + ', '
            if value.isdigit():
                values += value + ', '
            else:
                values += "'" + value + "'" + ', '
        keys = keys[:-2]
        keys += ')'
        sql_statement += keys + ' VALUES '
        values = values[:-2]
        values += ')'
        sql_statement += values + ';'
        # print(sql_statement)
        cursor = conn.cursor()
        cursor.execute(sql_statement)
        conn.commit()
        print('{}. Datensatz der Datenbank zugeführt!'.format(i))


if conn.is_connected():
    data = read_csv_file('table actors.csv')
    list_stop_1 = ['name', 'birth_city', 'birth_country', 'biography', 'gender', 'ethnicity']
    list_stop_2 = ['date_of_birth']
    list_stop_3 = ['height_inches', 'networth']
    clear_data(data, 'date_of_birth', list_stop_1, list_stop_2, list_stop_3)
    dict_to_sql_and_insert(data, 'actor')
    data_2 = read_csv_file('table movies.csv')
    list_stop_4 = ['title', 'mpaa_rating', 'genre', 'summary']
    list_stop_5 = ['release_date']
    list_stop_6 = ['budget', 'gross', 'runtime', 'rating', 'rating_count']
    clear_data(data_2, 'release_date', list_stop_4, list_stop_5, list_stop_6)
    dict_to_sql_and_insert(data_2, 'movie')
    data_3 = read_csv_file('table characters.csv')
    list_stop_7 = ['character_name']
    list_stop_8 = ['screentime']
    list_stop_9 = ['creditorder', 'pay']
    clear_data(data_3, '', list_stop_7, list_stop_8, list_stop_9)
    dict_to_sql_and_insert(data_3, 'kharakter')
    print('Daten sind eingelesen!')
