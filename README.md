# data_mart_for_store

Данный проект нацелен на демонстрацию работы с базой данных postres и оркестратором airflow.
Среда разворачивается в docker контейнерах.

База данных состоит из трех таблиц: Users, Purchases, Items
В проекте реализован даг, инициализирующий записи для трёх таблиц, а также формирующий две витрины

## Запуск

Для начала работы необходимо переименовать файл `.env.example` в `.env`:
```
mv .env.example .env
```

Инициализировать airflow:
```
docker compose up airflow-init
```

И запустить контейнеры:
```
docker compose up 
# docker compose up -d    для запуска в detach mode
```

После веб-интерфейс airflow будет доступен по адресу http://localhost:8080/, а база данных будет доступна по порту **5030**

Для инициализации записей необходимо в веб-интерфейсе активировать даг **init_store_data**, а затем запустить даг **create_datamarts**.
В последующем даг **create_datamarts** будет запускаться каждый день в 0:00 (в 3:00 по МСК)

## Результаты

В результате работы получается следующая структура базы данных:

![created_data_marts](/img/created_data_marts.png)

В таблице **purchase** инициализируются следующие данные (рандомные):

![purchases](/img/purchases.png)

Витрина **main_sales_metrics**:

![main_sales_metrics](/img/main_sales_metrics.png)

Витрина **top_items_for_current_year**:

![top_items_for_current_year](/img/top_items_for_current_year.png)