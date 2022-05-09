# Скрипт для автоматизации ETL процесса, который ежедневно получает выгрузку,
# данных в формате excel, загружает ее в хранилище данных согласно структуре
# хранилища и ежедневно строит отчет.

import time
import datetime
import glob
import cx_Oracle
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import Integer, Date, Float, String, create_engine

# Подключение к БД
user = "user" # Ввод имени пользователя в кавычках
password = "password" # Ввод пароля в кавычках
dsn = "IP:1521/nameDB" # Ввод IP сервера БД, порта(1521 по дефолту) и имени БД

# Директория с файлами и скриптами (по дефолту папка с проектом)
directory = ''

# Коннекты к БД
con = cx_Oracle.connect(user=user, password=password, dsn=dsn)
engine = create_engine(f'oracle+cx_oracle://{user}:{password}@{dsn}')


def open_file(sql_script):
    """Для открытия файлов со скриптами
    и последовательного считывания скриптов из них"""

    with open(f'{directory}sql_scripts\\{sql_script}.sql') as file_object:
        full_sql = file_object.read()
        sql_commands = full_sql.split('//')
        with con.cursor() as cursor:
            for sql_command in sql_commands:
                # Отлавливает ошибку при удалении несуществующей таблицы
                try:
                    cursor.execute(sql_command)
                    con.commit()
                except cx_Oracle.DatabaseError:
                    continue
        print(f'Скрипт {sql_script} отработал успешно')

# Запуск скрипта DDL
print("Запуск скрипта DDL")
open_file('DDL')

class EventHandler(FileSystemEventHandler):
    """Для отслеживания появления новых файлов"""

    def on_created(self, event):
        """Вызывается на событие создания файла или директории"""

        print(event.event_type, event.src_path)
        # Поиск последнего файла в директории
        time.sleep(5)
        print("Поиск последнего добавленного файла:")
        file = glob.glob(f'{directory}files\\*.xlsx')[-1]
        print(f"\t\t\t\t\t\tОК")
        time.sleep(5)
        # Перевод excel в df
        print("Перевод excel в df:")
        data = pd.read_excel(file)
        print(f"\t\t\t\t\t\tОК")

        # Загрузка df в БД
        data.to_sql(name='src_increment',
                    con=engine,
                    if_exists='replace',
                    index=False,
                    dtype={"trans_id": Integer(),
                           "date": Date(),
                           "card": String(length=255),
                           "account": String(length=255),
                           "account_valid_to": Date(),
                           "client": String(length=255),
                           "last_name": String(length=255),
                           "first_name": String(length=255),
                           "patronymic": String(length=255),
                           "date_of_birth": Date(),
                           "passport": Integer,
                           "passport_valid_to": Date(),
                           "phone": String(length=255),
                           "oper_type": String(length=255),
                           "amount": Float(),
                           "oper_result": String(length=255),
                           "terminal": String(length=255),
                           "terminal_type": String(length=255),
                           "city": String(length=255),
                           "address": String(length=255)})
        time.sleep(5)
        # Запуск скрипта ETL
        print("Запуск скрипта ETL")
        open_file('ETL')
        time.sleep(5)
        # Запуск скрипта Report
        print("Запуск скрипта Report")
        open_file('Report')

        # Экспорт отчета в Excel
        print("Экспорт отчета в Excel")
        now = datetime.datetime.now()
        df = pd.read_sql('SELECT * FROM report', engine)
        df.to_excel(f'{directory}reports\\report_'
                    f'{now.date()}_'
                    f'{now.hour}-'
                    f'{now.minute}-'
                    f'{now.second}.xlsx',
                    index=False
                    )
        print(f"\t\t\t\t\t\tОК")

if __name__ == "__main__":

    path = f'{directory}files_in' # отслеживаемая директория с нужным файлом
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()