import pickle
from pymongo import MongoClient

# Подключение к MongoDB
def connect_to_mongo(db_name, collection_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    return db, collection

# Загрузка данных из .pkl файла и запись в MongoDB
def load_data_from_pkl(file_path, collection):
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
    # Добавление записей в коллекцию
    collection.insert_many(data)

def perform_queries(db, collection):
    # 1. Удаление документов по предикату: salary < 25000 || salary > 175000
    query1_result = list(collection.find({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]}))
    db["task_3_q1"].delete_many({})  # Удаляем старые данные, если они есть
    db["task_3_q1"].insert_many(query1_result)
    collection.delete_many({"$or": [{"salary": {"$lt": 25000}}, {"salary": {"$gt": 175000}}]})

    # 2. Увеличение возраста (age) всех документов только в task_3_q2
    original_documents = list(collection.find())
    for doc in original_documents:
        doc['age'] += 1 
        del doc['_id']  
    db["task_3_q2"].delete_many({})
    db["task_3_q2"].insert_many(original_documents)
    
    # 3. Повышение зарплаты на 5% для выбранных профессий
    selected_jobs = ["Медсестра", "Повар"]
    query3_result = list(collection.find({"job": {"$in": selected_jobs}}))
    db["task_3_q3"].delete_many({})
    db["task_3_q3"].insert_many(query3_result)
    collection.update_many({"job": {"$in": selected_jobs}}, {"$mul": {"salary": 1.05}})

    # 4. Повышение зарплаты на 7% для выбранных городов
    selected_cities = ["Загреб", "Тирана"]
    query4_result = list(collection.find({"city": {"$in": selected_cities}}))
    db["task_3_q4"].delete_many({})
    db["task_3_q4"].insert_many(query4_result)
    collection.update_many({"city": {"$in": selected_cities}}, {"$mul": {"salary": 1.07}})

    # 5. Повышение зарплаты на 10% для сложного предиката
    complex_predicate = {
        "$and": [
            {"city": "Осера"},
            {"job": {"$in": ["Программист", "Менеджер"]}},
            {"age": {"$gte": 30, "$lte": 60}}
        ]
    }
    query5_result = list(collection.find(complex_predicate))
    db["task_3_q5"].delete_many({})
    db["task_3_q5"].insert_many(query5_result)
    collection.update_many(complex_predicate, {"$mul": {"salary": 1.10}})

    # 6. Удаление записей по произвольному предикату
    random_predicate = {"city": "Камбадос"}
    query6_result = list(collection.find(random_predicate))
    db["task_3_q6"].delete_many({})
    db["task_3_q6"].insert_many(query6_result)
    collection.delete_many(random_predicate)

if __name__ == "__main__":
    db_name = "ForFavouriteED"
    collection_name = "Employees"
    file_path = 'task_3_item.pkl'

    # Подключение к MongoDB и загрузка данных
    db, collection = connect_to_mongo(db_name, collection_name)
    load_data_from_pkl(file_path, collection)

    # Выполнение запросов
    perform_queries(db, collection)