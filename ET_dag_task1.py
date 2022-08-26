# Импортируем всё то, что нам нужно для создания DAG-а и его «содержания»
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pandahouse
import pandas as pd
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io

# Получаем доступ к боту
my_token = '5519778373:AAHucNeCKIIUd0t9XZRWGiPnrazrhev2ALs'
bot = telegram.Bot(token=my_token)
chat_id = -714461138

# Задаем параметры для запроса данных из CH
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220620',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

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
def dag_titova8_auto_v3():
    
    # Выгружаем нужные данные из feed_actions
    @task()
    def extract_feed():
    
        query = """SELECT toDate(time) AS event_date,
                count(DISTINCT user_id)/1000 as DAU,
                countIf(action='view')/1000 AS views,
                countIf(action='like')/1000 AS likes,
                likes/views as CTR,
                'this week' as week
            FROM simulator_20220620.feed_actions
            where toDate(time) < today() and toDate(time)>= today()-7
            GROUP BY event_date

            UNION ALL

            SELECT toDate(time) AS event_date,
                count(DISTINCT user_id)/1000 as DAU,
                countIf(action='view')/1000 AS views,
                countIf(action='like')/1000 AS likes,
                likes/views as CTR,
                'previous week' as week
            FROM simulator_20220620.feed_actions
            where toDate(time) < today()-7 and toDate(time)>= today()-14
            GROUP BY event_date"""
    
        df_feed = pandahouse.read_clickhouse(query, connection=connection)
        df_feed.DAU = round(df_feed.DAU, 1)
        df_feed.views = round(df_feed.views, 1)
        df_feed.likes = round(df_feed.likes, 1)
        df_feed.CTR = round(df_feed.CTR, 2)
        return df_feed      
    
    @task()
    def load(df_feed):
        # Отправляем сообщение        
        today = datetime. today()
        yesterday = today - timedelta(days=1)
        yesterday = yesterday.strftime("%Y-%m-%d")
        DAU = df_feed.query('event_date == @yesterday').iloc[0]['DAU']
        views = df_feed.query('event_date == @yesterday').iloc[0]['views']
        likes = df_feed.query('event_date == @yesterday').iloc[0]['likes']
        CTR = df_feed.query('event_date == @yesterday').iloc[0]['CTR']
        msg = f'Привет! Сообщаю, ключевые метрики ленты за вчера ({yesterday}): DAU {DAU} тыс. уников, просмотров {views} тыс., лайков {likes} тыс., CTR {CTR}. Графики для метрик за последние 7 дней прилагаю. Больше подробностей в дэшборде: http://superset.lab.karpov.courses/r/1534. Хорошего дня!'
        bot.sendMessage(chat_id=chat_id, text=msg)
    
    # Отправляем графики
        plt.figure(figsize=(9, 9))
        plt.suptitle('Ключевые метрики ленты', fontsize=16)
        plt.subplots_adjust(wspace=0, hspace=0.5)

        #График DAU
        plt.subplot(4,1,1)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "this week"').DAU, label = 'this week', color = 'darkblue', linewidth = 5)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "previous week"').DAU,  label = 'previous week', color = 'grey', linestyle = '--')
        plt.legend()
        plt.title('DAU')

        #График views 
        plt.subplot(4,1,2)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "this week"').views, label = 'this week', color = 'darkblue', linewidth = 5)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "previous week"').views,  label = 'previous week', color = 'grey', linestyle = '--')
        plt.legend()
        plt.title('Views')

        #График likes
        plt.subplot(4,1,3)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "this week"').likes, label = 'this week', color = 'darkblue', linewidth = 5)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "previous week"').likes,  label = 'previous week', color = 'grey', linestyle = '--')
        plt.legend()
        plt.title('Likes')

        #График CTR
        plt.subplot(4,1,4)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "this week"').CTR, label = 'this week', color = 'darkblue', linewidth = 5)
        plt.plot(df_feed.query('week == "this week"').event_date,df_feed.query('week == "previous week"').CTR,  label = 'previous week', color = 'grey', linestyle = '--')
        plt.legend()
        plt.title('CTR')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'homework_plot.png'
        plt.show()
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    # Инициализируем таски:
    df_feed = extract_feed()
       
    load(df_feed)
        
dag_titova8_auto_v3 = dag_titova8_auto_v3()
