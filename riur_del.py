# -*- coding: utf-8 -*-

import psycopg2
from psycopg2 import Error

try:
    # Подключение к существующей базе данных
    connection = psycopg2.connect(user="", password="", host="", port="", database="")

    # Курсор для выполнения операций с базой данных
    cursor = connection.cursor()

    # Выполнение SQL-запроса
    query = "SELECT * FROM riur_del"
    cursor.execute(query)

    # Массив с данными 
    result = cursor.fetchall()

    for i in range(len(result)):
        fio = str(result[i][1]).replace("'","`").split()
        last_name = fio[0].strip().title()
        first_name = fio[1].strip().title()
        patronymic = "-"
        if len(fio) > 2:
            patronymic = fio[2].strip().title()
        if len(fio) == 4 and fio[3]!= "-":   
            patronymic += " " + fio[3].strip().title()

        #Получение даты рождения
        birth_date = str(result[i][2]).replace(" ","")
        #Получение номера документа
        doc_full_raw = str(result[i][8]).replace(" ","")

        try:
            if doc_full_raw.find("сер.") != -1:
                serial = doc_full_raw.split("сер.")[1].split("№")[0]
                numb = doc_full_raw.split("сер.")[1].split("№")[1]
            else:
                serial = doc_full_raw[0:4]
                numb = doc_full_raw[4:]
        except:
            numb = doc_full_raw
            serial = ""

        query = "DELETE FROM civilians WHERE doc_full = {}".format("'"+serial+numb+"'")
        cursor.execute(query)
        connection.commit()
        print(str(i))
        count = cursor.rowcount
        print(count, "Record deletet successfully")
        if count == 0:
            query = "DELETE FROM civilians WHERE fio = {} and date_of_birth = {}".format("'"+last_name + " " + first_name + " " + patronymic+"'", "'"+birth_date+"'")
            cursor.execute(query)
            connection.commit()
            count = cursor.rowcount
            print(count, "Record DELETED successfully")


except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error.pgerror)
finally:
    if connection:
        cursor.close()
        connection.close()
        #print("Соединение с PostgreSQL закрыто")
