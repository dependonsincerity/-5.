from pymongo import MongoClient
import pandas as pd

def connect_to_mongo(db_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    return db

# Загрузка данных из CSV и TXT и запись в MongoDB
def load_data_to_mongo(csv_file_path, txt_file_path, collection):
    
    csv_data = pd.read_csv(csv_file_path, delimiter=",")
    csv_data.columns = csv_data.columns.str.strip()  # Убираем пробелы в именах столбцов
    csv_data.fillna("", inplace=True)
    records_csv = csv_data.to_dict(orient="records")
    collection.insert_many(records_csv)

    txt_data = pd.read_csv(txt_file_path, sep='\t')
    txt_data.columns = txt_data.columns.str.strip()
    txt_data.fillna("", inplace=True)
    records_txt = txt_data.to_dict(orient="records")
    collection.insert_many(records_txt)

# Функция для сохранения данных в коллекцию
def save_to_mongo(collection, data):
    if data:
        for record in data:
            record.pop("_id", None)
        collection.insert_many(data)

# Запросы к MongoDB
def perform_queries(db):
    # 1. Первые 10 записей, отсортированных по убыванию по полю "Price"
    query1 = list(db["4 quest(ED)"].find({}, {"_id": 0}).sort("Price", -1).limit(10))
    query1_collection = db["task_4_q1"]
    save_to_mongo(query1_collection, query1)

    # 2. Первые 10 записей, отфильтрованных по цене больше 30, отсортированных по названию
    query2 = list(db["4 quest(ED)"].find({"Price": {"$gt": 30}}, {"_id": 0}).sort("Product Name", 1).limit(10))
    query2_collection = db["task_4_q2"]
    save_to_mongo(query2_collection, query2)

    # 3. Первые 10 записей, отфильтрованных по категории "Men's Fashion", отсортированных по цене
    query3 = list(db["4 quest(ED)"].find({"Category": "Men's Fashion"}, {"_id": 0}).sort("Price", 1).limit(10))
    query3_collection = db["task_4_q3"]
    save_to_mongo(query3_collection, query3)

    # 4. Количество товаров в категории "Women's Fashion", которые стоят меньше 50
    query4 = list(db["4 quest(ED)"].find({"Category": "Women's Fashion", "Price": {"$lt": 50}}, {"_id": 0}))
    query4_collection = db["task_4_q4"]
    save_to_mongo(query4_collection, query4)

    # 5. Количество товаров, которые стоят от 30 до 80, отсортированных по имени
    query5 = list(db["4 quest(ED)"].find({"Price": {"$gte": 30, "$lte": 80}}, {"_id": 0}).sort("Product Name", 1))
    query5_collection = db["task_4_q5"]
    save_to_mongo(query5_collection, query5)

    # 6. Подсчет количества товаров по категориям
    aggregation1 = list(db["4 quest(ED)"].aggregate([
        {"$group": {"_id": "$Category", "count": {"$sum": 1}}},
        {"$project": {"Category": "$_id", "count": 1, "_id": 0}}
    ]))
    aggregation1_collection = db["task_4_q6"]
    save_to_mongo(aggregation1_collection, aggregation1)

    # 7. Средняя цена товаров по категориям
    aggregation2 = list(db["4 quest(ED)"].aggregate([
        {"$group": {"_id": "$Category", "avg_price": {"$avg": "$Price"}}},
        {"$project": {"Category": "$_id", "avg_price": 1, "_id": 0}}
    ]))
    aggregation2_collection = db["task_4_q7"]
    save_to_mongo(aggregation2_collection, aggregation2)

    # 8. Общая стоимость товаров, которые стоят более 30
    aggregation3 = list(db["4 quest(ED)"].aggregate([
        {"$match": {"Price": {"$gt": 30}}},
        {"$group": {"_id": "$Category", "total_price": {"$sum": "$Price"}}},
        {"$project": {"Category": "$_id", "total_price": 1, "_id": 0}}
    ]))
    aggregation3_collection = db["task_4_q8"]
    save_to_mongo(aggregation3_collection, aggregation3)

    # 9. Подсчет количества товаров по брендам, сортировка по количеству
    aggregation4 = list(db["4 quest(ED)"].aggregate([
        {"$group": {"_id": "$Brand", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {"Brand": "$_id", "count": 1, "_id": 0}}
    ]))
    aggregation4_collection = db["task_4_q9"]
    save_to_mongo(aggregation4_collection, aggregation4)

    # 10. Минимальная цена товара в каждой категории
    aggregation5 = list(db["4 quest(ED)"].aggregate([
        {"$group": {"_id": "$Category", "min_price": {"$min": "$Price"}}},
        {"$project": {"Category": "$_id", "min_price": 1, "_id": 0}}
    ]))
    aggregation5_collection = db["task_4_q10"]
    save_to_mongo(aggregation5_collection, aggregation5)

    # 11. Увеличить цену на 10% для товаров в категории "Men's Fashion"
    db["4 quest(ED)"].update_many(
        {"Category": "Men's Fashion"},
        {"$mul": {"Price": 1.1}}
    )
    update1 = list(db["4 quest(ED)"].find({"Category": "Men's Fashion"}))
    update1_collection = db["task_4_q11"]
    save_to_mongo(update1_collection, update1)

    # 12. Применить скидку 10% для товаров, цена которых больше 50
    db["4 quest(ED)"].update_many(
        {"Price": {"$gt": 50}},
        {"$set": {"Discount": 0.1}}
    )
    update2 = list(db["4 quest(ED)"].find({"Discount": 0.1}))
    update2_collection = db["task_4_q12"]
    save_to_mongo(update2_collection, update2)

    # 13. Удалить товары с ценой менее 40
    db["4 quest(ED)"].delete_many({"Price": {"$lt": 40}})
    delete1 = list(db["4 quest(ED)"].find())
    delete1_collection = db["task_4_q13"]
    save_to_mongo(delete1_collection, delete1)

    # 14. Установить цену на 45 для товаров, которые стоят больше 70
    db["4 quest(ED)"].update_many(
        {"Price": {"$gt": 70}},
        {"$set": {"Price": 0.025}}
    )
    update3 = list(db["4 quest(ED)"].find({"Price": 0.025}))
    update3_collection = db["task_4_q14"]
    save_to_mongo(update3_collection, update3)

    # 15. Установить цену на 45 для товаров, которые стоят больше 70
    db["4 quest(ED)"].update_many(
        {"Price": {"$gt": 35}},
        {"$set": {"Price": 666}}
    )
    update3 = list(db["4 quest(ED)"].find({"Price": 666}))
    update3_collection = db["task_4_q15"]
    save_to_mongo(update3_collection, update3)

def main():
    # Путь к файлам
    csv_file_path = "for task 4(in 5).csv"
    txt_file_path = "for task 4(in 5).txt"
    db_name = "ForFifthPrac"

    # Подключение к MongoDB и загрузка данных
    db = connect_to_mongo(db_name)
    db["4 quest(ED)"].drop()  
    load_data_to_mongo(csv_file_path, txt_file_path, db["4 quest(ED)"])

    # Выполнение запросов
    perform_queries(db)

if __name__ == "__main__":
    main()
