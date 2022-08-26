# Импортируем всё то, что нам нужно для создания DAG-а и его «содержания»
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
from datetime import date, timedelta, datetime

# Подключаемся к CH
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220620',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# Получаем доступ к боту
my_token = '5519778373:AAHucNeCKIIUd0t9XZRWGiPnrazrhev2ALs'
bot = telegram.Bot(token=my_token)
chat_id = -714461138

# Задаем дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e.titova-8',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 7),
}

# Задаем интервал запуска DAG
schedule_interval = '0 11 * * *'

# Пишем DAG cо всеми нужными тасками 
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_titova8_task72():
   
    # Делаем выгрузку 1
    @task()
    def extract_source():
        query = """SELECT event_date,
            source,
            count(DISTINCT user_id)/1000 AS "DAU"

            FROM

            (SELECT user_id,
                    toDate(time) as event_date,
                    source
            from simulator_20220620.message_actions
            WHERE event_date >= today()-7 and event_date < today()

            union all

            SELECT user_id,
                    toDate(time) as event_date,
                    source
            from simulator_20220620.feed_actions)
            WHERE event_date >= today()-7 and event_date < today()
            GROUP BY event_date, source
            ORDER BY event_date DESC"""

        df_source = pandahouse.read_clickhouse(query, connection=connection)
        df_source.DAU = round(df_source.DAU, 1)
        return df_source
    
    # Делаем выгрузку 2  
    @task()
    def extract_action():
        query2 = """SELECT event_date,
            action,
            count(user_id)/1000 AS "events"

            FROM

            (SELECT user_id,
                    toDate(time) as event_date,
                    'messages_sent' as action
            from simulator_20220620.message_actions
            WHERE event_date >= today()-7 and event_date < today()

            union all

            SELECT user_id,
                    toDate(time) as event_date,
                    action
            from simulator_20220620.feed_actions)
            WHERE event_date >= today()-7 and event_date < today()
            GROUP BY event_date, action
            ORDER BY event_date DESC"""

        df_action = pandahouse.read_clickhouse(query2, connection=connection)
        return df_action
    
    # Отправляем графики и сообщение
    @task()
    def load_task(df_action, df_source):
        today = datetime. today()
        yesterday = today - timedelta(days=1)
        yesterday = yesterday.strftime("%Y-%m-%d")
        DAU = df_source[['event_date', 'DAU']].query('event_date == @yesterday').groupby(['event_date'], as_index=False).agg({'DAU': 'sum'}).iloc[0]['DAU']
        views = round(df_action.query('event_date == @yesterday & action == "view"').groupby(['event_date'], as_index=False).agg({'events': 'sum'}).iloc[0]['events'], 1)
        likes = round(df_action.query('event_date == @yesterday & action == "like"').groupby(['event_date'], as_index=False).agg({'events': 'sum'}).iloc[0]['events'], 1)
        messages = round(df_action.query('event_date == @yesterday & action == "messages_sent"').groupby(['event_date'], as_index=False).agg({'events': 'sum'}).iloc[0]['events'], 1)
        msg = f'Привет! Сообщаю, ключевые метрики приложения за вчера ({yesterday}): DAU {DAU} тыс. уников, просмотров {views} тыс., лайков {likes} тыс., отправленных сообщений {messages} тыс. Графики для метрик за последние 7 дней прилагаю. Больше подрообностей в дэшборде: http://superset.lab.karpov.courses/r/1536. Хорошего дня!'
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        plt.figure(figsize=(9, 9))
        plt.suptitle('Ключевые метрики приложения', fontsize=16)
        plt.subplots_adjust(wspace=0, hspace=0.5)
        
        plt.subplot(4,1,1)
        plt.plot(df_source.query('source == "ads"').event_date, df_source.query('source == "ads"').DAU, label = 'ads', color = 'blue')
        plt.plot(df_source.query('source == "organic"').event_date, df_source.query('source == "organic"').DAU,  label = 'organic', color = 'green')
        plt.legend()
        plt.title('DAU приложения по источнику траффика (тыс. уников)')
        
        plt.subplot(4,1,2)
        plt.title('События приложения по категориям (тыс. )')
        plt.plot(df_action.query('action == "like"').event_date, df_action.query('action == "like"').events, label = 'likes')
        plt.legend()
        
        plt.subplot(4,1,3)
        plt.plot(df_action.query('action == "view"').event_date, df_action.query('action == "view"').events, label = 'view')
        plt.legend()
        
        plt.subplot(4,1,4)
        plt.plot(df_action.query('action == "messages_sent"').event_date, df_action.query('action == "messages_sent"').events, label = 'messages sent')
        plt.legend()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'homework_plot.png'
        plt.show()
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    # Инициализируем таски:
    df_source = extract_source()
    df_action = extract_action()
    
    load_task(df_action, df_source)
    
dag_titova8_task72 = dag_titova8_task72()
