#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import configparser
import pandas as pd
import pyodbc
from datetime import datetime
import getpass
import sys
from odf import opendocument, table


def load_config(config_path='config.ini'):
    """Загрузка конфигурации из файла"""
    config = configparser.ConfigParser()
    if not os.path.exists(config_path):
        print(f"Ошибка: Файл конфигурации {config_path} не найден!")
        sys.exit(1)

    config.read(config_path)
    return config


def get_odf_data(file_path):
    """Чтение данных из ODF файла (ODS)"""
    try:
        doc = opendocument.load(file_path)
        sheets = doc.spreadsheet.getElementsByType(table.Table)

        data = []
        for sheet in sheets:
            for row in sheet.getElementsByType(table.TableRow):
                row_data = []
                for cell in row.getElementsByType(table.TableCell):
                    text = "".join([str(t) for t in cell.childNodes if t.nodeType == 1])
                    row_data.append(text)
                if row_data:  # пропускаем пустые строки
                    data.append(row_data)

        # Преобразуем в DataFrame (предполагаем, что первая строка - заголовки)
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {str(e)}")
        return None


def prepare_data(df, filename):
    """Подготовка данных для загрузки в MSSQL"""
    try:
        # Преобразование данных в нужный формат
        prepared_data = []
        for _, row in df.iterrows():
            prepared_row = {
                'код_МТР': str(row['Код МТР'])[:7] if pd.notna(row['Код МТР']) else None,
                'склад_мол': str(row['Склад/МОЛ'])[:50] if pd.notna(row['Склад/МОЛ']) else None,
                'номер_документа': str(row['№ ЛЗК'])[:50] if pd.notna(row['№ ЛЗК']) else None,
                'Дата': datetime.strptime(row['Дата ЛЗК'], '%Y-%m-%d') if pd.notna(row['Дата ЛЗК']) else None,
                'старый_заказ': str(row['№ заказа до переноса'])[:13] if pd.notna(
                    row['№ заказа до переноса']) else None,
                'количество': float(row['Количество МТР'].replace(',', '.')) if pd.notna(
                    row['Количество МТР']) else None,
                'стоимость': float(row['Стоимость без ТЗР'].replace(',', '.')) if pd.notna(
                    row['Стоимость без ТЗР']) else None,
                'реестр': str(row['Номер реестра'])[:50] if pd.notna(row['Номер реестра']) else None,
                'новый_заказ': str(row['№ заказа после переноса'])[:13] if pd.notna(
                    row['№ заказа после переноса']) else None,
                'паспорт': None,  # Это поле не заполняется из ODS
                'имя_файла': filename,
                'дата_загрузки': datetime.now(),
                'кто_загрузил': getpass.getuser()
            }
            prepared_data.append(prepared_row)
        return prepared_data
    except Exception as e:
        print(f"Ошибка при подготовке данных: {str(e)}")
        return None


def create_connection(config):
    """Создание подключения к MSSQL"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config['MSSQL']['server']};"
            f"DATABASE={config['MSSQL']['database']};"
            f"UID={config['MSSQL'].get('username', '')};"
            f"PWD={config['MSSQL'].get('password', '')}"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f"Ошибка подключения к MSSQL: {str(e)}")
        return None


def insert_data(conn, data, table_name):
    """Вставка данных в MSSQL"""
    try:
        cursor = conn.cursor()

        # SQL-запрос для вставки
        sql = f"""
        INSERT INTO {table_name} (
            код_МТР, склад_мол, номер_документа, Дата, старый_заказ, 
            количество, стоимость, реестр, новый_заказ, паспорт,
            имя_файла, дата_загрузки, кто_загрузил
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Подготовка данных для вставки
        insert_data = []
        for row in data:
            insert_data.append((
                row['код_МТР'], row['склад_мол'], row['номер_документа'], row['Дата'],
                row['старый_заказ'], row['количество'], row['стоимость'], row['реестр'],
                row['новый_заказ'], row['паспорт'], row['имя_файла'], row['дата_загрузки'],
                row['кто_загрузил']
            ))

        # Выполнение вставки
        cursor.executemany(sql, insert_data)
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при вставке данных: {str(e)}")
        return 0
    finally:
        cursor.close()


def process_files(config):
    """Обработка всех ODS файлов в указанной директории"""
    input_dir = config['DEFAULT']['input_dir']
    table_name = config['MSSQL']['table_name']

    # Проверка существования директории
    if not os.path.isdir(input_dir):
        print(f"Ошибка: Директория {input_dir} не существует!")
        return

    # Подключение к MSSQL
    conn = create_connection(config)
    if not conn:
        return

    try:
        # Поиск всех ODS файлов в директории
        ods_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.ods')]

        if not ods_files:
            print(f"В директории {input_dir} не найдено ODS файлов!")
            return

        total_rows = 0
        for filename in ods_files:
            file_path = os.path.join(input_dir, filename)
            print(f"Обработка файла: {filename}")

            # Чтение данных из ODS
            df = get_odf_data(file_path)
            if df is None or df.empty:
                print(f"Не удалось прочитать данные из файла {filename} или файл пуст")
                continue

            # Подготовка данных
            prepared_data = prepare_data(df, filename)
            if not prepared_data:
                print(f"Не удалось подготовить данные из файла {filename}")
                continue

            # Вставка данных в MSSQL
            inserted_rows = insert_data(conn, prepared_data, table_name)
            total_rows += inserted_rows
            print(f"Добавлено {inserted_rows} строк из файла {filename}")

        print(f"\nВсего обработано файлов: {len(ods_files)}")
        print(f"Всего добавлено строк: {total_rows}")
    finally:
        conn.close()


def main():
    print("=== Загрузчик данных из ODS в MSSQL ===")

    # Проверка наличия необходимых модулей
    try:
        import pyodbc
        from odf import opendocument, table
    except ImportError as e:
        print(f"Ошибка: Не установлены необходимые модули: {str(e)}")
        print("Пожалуйста, установите их с помощью команд:")
        print("pip install pyodbc odfpy")
        sys.exit(1)

    # Загрузка конфигурации
    config = load_config()

    # Обработка файлов
    process_files(config)


if __name__ == "__main__":
    main()