# Бот написан на версии airflow без асинхронности


from datetime import timedelta, datetime

import pandas as pd
import numpy as np

import pandahouse as ph

import matplotlib.pyplot as plt
import seaborn as sns

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import io


# Подключение к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 
    'user': 'student',
    'database': 'simulator'
}
db='simulator_20240320'
default_args = {
    'owner': 'v.grabchuk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 4, 23),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def gvs_report_feed():
    @task
    def get_metrics():
        # Статистика юзеров в feed_actions
        context = get_current_context()
        today = context['ds']
        today = f"'{today}'"
        q = f"""
        SELECT
            toDate(time) AS date,
            COUNT(DISTINCT user_id) AS DAU,
            countIf(action='view') AS views,
            countIf(action='like') AS likes,
            ROUND(likes / views, 3) as CTR
        FROM {db}.feed_actions
        WHERE 
            date <= {today}
            AND date+7 > {today}
        GROUP BY date
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def get_prev_day_text_report(df):
        # Текстовый отчёт за предыдущий день
        data = df.astype({'date': str}).iloc[-1]
        prev_day_report = ''.join(f'{k}:\t{v}\n' for k, v in zip(data.index, data.values))
        return prev_day_report
    
    def get_lineplot(df, x, y, xrotation=0, color='b', **kwargs):
        # Возвращает lineplot
        ax = sns.lineplot(data=df, x=x, y=y, color=color, marker='o', **kwargs)
        ax.grid()
        ax.set_title(y)
        ax.tick_params(axis='x', labelrotation=xrotation)

        for x, y, m in zip(df[x], df[y], df[y].astype(str)):
            ax.text(x, y, m, color=color)
        return ax
    
    @task
    def get_plots(df):
        # Возвращает все графики на одном полотне
        size = (4, 1)
        figsize = (8, 16)

        fig, ax = plt.subplots(*size, figsize=figsize)

        ax = plt.subplot(*size, 1);
        get_lineplot(df, x='date', y='DAU', color='r', ax=ax);
        ax = plt.subplot(*size, 2);
        get_lineplot(df, x='date', y='views', color='g', ax=ax);
        ax = plt.subplot(*size, 3);
        get_lineplot(df, x='date', y='likes', color='b', ax=ax);
        ax = plt.subplot(*size, 4);
        get_lineplot(df, x='date', y='CTR', color='y', ax=ax);

        fig.tight_layout()

        return fig
        
    @task
    def report_message(bot, chat_id, text):
        # Отправка сообщения
        bot.sendMessage(chat_id=chat_id, text=text)
    
    @task
    def report_image(bot, chat_id, plot):
        # Отправка изображения
        plot;
        plot = io.BytesIO()
        plt.savefig(plot, format='png', bbox_inches='tight')
        plot.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot)
    
    
    # Получаем метрики
    df = get_metrics()
    # Подрубаем бота
    my_token = 
    bot = telegram.Bot(token=my_token)
    chat_id = 
    # Текстовый отчёт
    text_report = get_prev_day_text_report(df)
    report_message(bot, chat_id, text_report)
    # Графический отчёт
    plot = get_plots(df);
    report_image(bot, chat_id, plot)
    
    
gvs_report_feed = gvs_report_feed()

