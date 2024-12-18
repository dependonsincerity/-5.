from pymongo import MongoClient

# Подключение к MongoDB
def connect_to_mongo(db_name, collection_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    return db, collection

# Загрузка данных из текстового файла и запись в MongoDB
def load_data_from_text(file_path, collection):
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_text_data = f.read()
    
    records = []
    for block in raw_text_data.split("====="):
        record = {}
        for line in block.strip().split("\n"):
            if "::" in line:
                key, value = line.split("::", 1)
                record[key.strip()] = value.strip()
        if record:
            record["salary"] = int(record["salary"])
            record["age"] = int(record["age"])
            records.append(record)
    
    # Добавление записей в коллекцию
    collection.insert_many(records)

def perform_queries(db, collection):
    # 1. Минимальная, средняя, максимальная salary
    salary_stats = collection.aggregate([
        {"$group": {
            "_id": "summary",
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ])
    db["task_2_q1"].delete_many({})  # Удаляем старые данные, если они есть
    db["task_2_q1"].insert_many(list(salary_stats))

    # 2. Количество данных по представленным профессиям
    job_count = collection.aggregate([
        {"$group": {"_id": "$job", "count": {"$sum": 1}}}
    ])
    db["task_2_q2"].delete_many({})
    db["task_2_q2"].insert_many(list(job_count))

    # 3. Минимальная, средняя, максимальная salary по городу
    city_salary_stats = collection.aggregate([
        {"$group": {
            "_id": "$city",
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ])
    db["task_2_q3"].delete_many({})
    db["task_2_q3"].insert_many(list(city_salary_stats))

    # 4. Минимальная, средняя, максимальная salary по профессии
    job_salary_stats = collection.aggregate([
        {"$group": {
            "_id": "$job",
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ])
    db["task_2_q4"].delete_many({})
    db["task_2_q4"].insert_many(list(job_salary_stats))

    # 5. Минимальный, средний, максимальный возраст по городу
    city_age_stats = collection.aggregate([
        {"$group": {
            "_id": "$city",
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }}
    ])
    db["task_2_q5"].delete_many({})
    db["task_2_q5"].insert_many(list(city_age_stats))

    # 6. Минимальный, средний, максимальный возраст по профессии
    job_age_stats = collection.aggregate([
        {"$group": {
            "_id": "$job",
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }}
    ])
    db["task_2_q6"].delete_many({})
    db["task_2_q6"].insert_many(list(job_age_stats))

    # 7. Максимальная зарплата при минимальном возрасте
    max_salary_min_age = collection.aggregate([
        {"$sort": {"age": 1}},
        {"$limit": 1},
        {"$group": {"_id": "min_age", "max_salary": {"$max": "$salary"}}}
    ])
    db["task_2_q7"].delete_many({})
    db["task_2_q7"].insert_many(list(max_salary_min_age))

    # 8. Минимальная зарплата при максимальном возрасте
    min_salary_max_age = collection.aggregate([
        {"$sort": {"age": -1}},
        {"$limit": 1},
        {"$group": {"_id": "max_age", "min_salary": {"$min": "$salary"}}}
    ])
    db["task_2_q8"].delete_many({})
    db["task_2_q8"].insert_many(list(min_salary_max_age))

    # 9. Возрастные статистики по городам с зарплатой > 50 000
    filtered_city_age_stats = collection.aggregate([
        {"$match": {"salary": {"$gt": 50000}}},
        {"$group": {
            "_id": "$city",
            "min_age": {"$min": "$age"},
            "avg_age": {"$avg": "$age"},
            "max_age": {"$max": "$age"}
        }},
        {"$sort": {"avg_age": -1}}
    ])
    db["task_2_q9"].delete_many({})
    db["task_2_q9"].insert_many(list(filtered_city_age_stats))

    # 10. Статистика по возрастным диапазонам с фильтрацией по зарплате, профессии и городу
    age_salary_stats = collection.aggregate([
        {"$match": {
            "$and": [
                {"$or": [
                    {"age": {"$gte": 18, "$lte": 25}},
                    {"age": {"$gte": 50, "$lte": 65}}
                ]},
                {"city": "Сараево"}  # Укажите конкретный город
            ]
        }},
        {"$group": {
            "_id": {"city": "$city", "job": "$job"},
            "min_salary": {"$min": "$salary"},
            "avg_salary": {"$avg": "$salary"},
            "max_salary": {"$max": "$salary"}
        }}
    ])
    db["task_2_q10"].delete_many({})
    db["task_2_q10"].insert_many(list(age_salary_stats))


    # 11. Произвольный запрос
    custom_query = collection.aggregate([
        {"$match": {"salary": {"$gt": 100000}}},
        {"$group": {
            "_id": "$job",
            "avg_age": {"$avg": "$age"}
        }},
        {"$sort": {"avg_age": 1}}
    ])
    db["task_2_q11"].delete_many({})
    db["task_2_q11"].insert_many(list(custom_query))


# Основная функция
def main():
    file_path = "task_2_item.text"
    db_name = "ForFavouriteED"
    collection_name = "Employees"

    # Подключение к MongoDB
    db, collection = connect_to_mongo(db_name, collection_name)

    # Загрузка данных из текстового файла
    load_data_from_text(file_path, collection)

    # Выполнение запросов
    perform_queries(db, collection)

if __name__ == "__main__":
    main()
