from pymongo import MongoClient
import pandas as pd
import json

# Подключение к MongoDB
def connect_to_mongo(db_name):
    client = MongoClient("mongodb://localhost:27017/") 
    db = client[db_name]
    return db

# Загрузка данных из CSV и запись в MongoDB
def load_data_to_mongo(file_path, collection):
    data = pd.read_csv(file_path, delimiter=";")
    data.fillna("", inplace=True)  # Заполняем пропущенные значения
    data['salary'] = pd.to_numeric(data['salary'], errors='coerce')  
    records = json.loads(data.to_json(orient="records"))  # Конвертируем в JSON
    collection.insert_many(records)

# Запросы к MongoDB
def perform_queries(db):
    query1_collection = db["task_1_q1"]
    query2_collection = db["task_1_q2"]
    query3_collection = db["task_1_q3"]
    query4_collection = db["task_1_q4"]

    # 1. Первые 10 записей, отсортированных по убыванию по полю salary
    query1 = list(db["Employees"].find({}, {"_id": 0}).sort("salary", -1).limit(10))
    with open("1.json", "w", encoding="utf-8") as file:
        json.dump(query1, file, ensure_ascii=False, indent=4)
    query1_collection.insert_many(query1)

    # 2. Первые 15 записей, отфильтрованных по age < 30, отсортированных по salary по убыванию
    query2 = list(db["Employees"].find({"age": {"$lt": 30}}, {"_id": 0}).sort("salary", -1).limit(15))
    with open("2.json", "w", encoding="utf-8") as file:
        json.dump(query2, file, ensure_ascii=False, indent=4)
    query2_collection.insert_many(query2)

    # 3. Первые 10 записей, отфильтрованных по профессии "оператор call-центра", отсортированных по возрасту по возрастанию
    job = ["Оператор call-центра"]
    query3 = list(db["Employees"].find({"job": {"$in": job}},  {"_id": 0}).sort("age", 1).limit(10))
    with open("3.json", "w", encoding="utf-8") as file:
        json.dump(query3, file, ensure_ascii=False, indent=4)
    query3_collection.insert_many(query3)

    # 4. Количество записей, отфильтрованных по заданным условиям
    age_range = {"$gte": 25, "$lte": 40}  
    salary_condition = {"$or": [
        {"salary": {"$gt": 50000, "$lte": 75000}},
        {"salary": {"$gt": 125000, "$lt": 150000}}
    ]}
    year_range = {"year": {"$gte": 2019, "$lte": 2022}}
    count_query = db["Employees"].count_documents(
        {"$and": [year_range, {"age": age_range}, salary_condition]}
    )
    with open("4.json", "w", encoding="utf-8") as file:
        json.dump({"count": count_query}, file, ensure_ascii=False, indent=4)
    query4_collection.insert_one({"count": count_query})

def main():
    
    file_path = "task_1_item.csv"
    db_name = "ForFavouriteED"

    # Подключение к MongoDB и загрузка данных
    db = connect_to_mongo(db_name)
    db["Employees"].drop()  
    load_data_to_mongo(file_path, db["Employees"])

    # Выполнение запросов
    perform_queries(db)

if __name__ == "__main__":
    main()
