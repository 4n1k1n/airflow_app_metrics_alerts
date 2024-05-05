from datetime import datetime, timedelta
import pandas as pd
import requests
import pandahouse as ph
import numpy as np
import datetime as dt
import io
import telegram
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates

import seaborn as sns
sns.set_style("whitegrid") 

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.anikin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 4, 29),
}
# Интервал запуска DAG
schedule_interval = '*/15 * * * *' # каждые 15 минут

# Вход для запросов, задаются параметры подключения к базе данных:
connection = {
    'host': use_host_url,
    'password': use_db_password,
    'user': use_user_name,
    'database': use_database
}
# Задаются токен бота и id чата, в который будут отправляться алерты
my_token = use_tg_bot_token
chat_id = use_chat_id
bot = telegram.Bot(token=my_token) # получаем доступ

bi_links = {'users_feed': 'http://superset.lab.karpov.courses/r/5401',
            'views': 'http://superset.lab.karpov.courses/r/5402',
            'likes': 'http://superset.lab.karpov.courses/r/5403',
            'ctr': 'http://superset.lab.karpov.courses/r/5404',
            'users_message': 'http://superset.lab.karpov.courses/r/5405',
            'messages': 'http://superset.lab.karpov.courses/r/5406'}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_n_anikin_tg_alerts():

    @task
    def extract_data():
        '''
        Извлекаем необходимые метрики для исследования: 'users_feed', 'views', 'likes', 'ctr', 'users_message', 'messages'
        '''
        
        query = '''
        SELECT
            f.ts AS ts,
            f.date AS date,
            f.hm AS hm,
            f.users_feed AS users_feed,
            f.views AS views,
            f.likes AS likes,
            f.ctr,
            m.users_message AS users_message,
            m.messages AS messages
        FROM
            (SELECT
                toStartOfFifteenMinutes(time) as ts,
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm,
                uniqExact(user_id) as users_feed,
                countIf(user_id, action = 'view') AS views,
                countIf(user_id, action = 'like') AS likes,
                likes / views AS ctr
            FROM {db}.feed_actions
            WHERE ts >=  today() - 7 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts) f
        
        FULL JOIN
        
            (SELECT
                toStartOfFifteenMinutes(time) as ts,
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm,
                uniqExact(user_id) as users_message,
                count(user_id) AS messages
            FROM {db}.message_actions
            WHERE ts >=  today() - 7 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts) m
            
        USING(ts, date, hm)
        '''
        df_metrics = ph.read_clickhouse(query=query, connection=connection)
        return df_metrics

    @task
    def is_alert(df_metrics, days=7, values=5, first_sigma=2, second_sigma=3):
        '''
        Отбираем 5 значений метрики (values >= 1) за последние 7 дней (days):
        Для каждого дня:
        - Две 15-ти минутки до текущего времени + текущее время + две 15-ти минутки после текущего времени
        - В текущее время будут отобраны значения до текущего времени.
        
        Внутри каждого дня отбираем значения в интервале +-2 сигма (first_sigma),
        чтобы исключить влияние прошлых выбросов.
        
        Для отобранных значений считаем доверительный интервал +-3сигма (second_sigma).
        Если последнее значение выходит за пределы этого интервала добавляется 1 в df с состоянием алертов для каждой метрики.
        
        Результат функции - dataframe:
        metric - название метрки:
        is_alert - 0: значение внутри доверительного интервала, 1: значение за пределами доверительного интервала
        lower_border - нижняя граница для определенной метрики
        check_value - проверяемое (последнее) значение метрики
        upper_border - верхняя граница для определенной метрики
        '''
        
        df_interval_values = pd.DataFrame() # создаем пустой df для отбора значений
        # отберем данные за последние 7 дней
        for day in range(days):
            # отберем последние 5 значений метрики за каждый день
            start_daytime = df_metrics['ts'].max() \
                            - pd.DateOffset(days=day,
                                            minutes=((round(values/2 + 0.01) - 1) * 15))        
            finish_daytime= df_metrics['ts'].max() \
                            - pd.DateOffset(days=day) \
                            + pd.DateOffset(minutes=((round(values/2 + 0.01) - 1) * 15))        
            day_values = df_metrics[(df_metrics['ts'] >= start_daytime) & (df_metrics['ts'] <= finish_daytime)]
            # уберем выбросы из данных, если значение выходит за пределы интервала first_sigma
            for metric in bi_links.keys():
                upper_day_border = df_metrics[metric].mean() + first_sigma*df_metrics[metric].std()
                lower_day_border = df_metrics[metric].mean() - first_sigma*df_metrics[metric].std()
                day_values = day_values[(day_values[metric] >= lower_day_border) & (day_values[metric] <= upper_day_border)]
            df_interval_values = pd.concat([df_interval_values, day_values])
    
        # создадим итоговый df со значением метрики, границами и наличием аномалий
        df_alerts = pd.DataFrame(data={'metric': bi_links.keys(),
                                       'is_alert': 0,
                                       'lower_border': 0,
                                       'check_value': 0,
                                       'upper_border': 0})
        # ставим 1 в колонку is_alert, если значение метрики выходит за пределы интервала second_sigma
        for metric in bi_links.keys():
            upper_border = df_interval_values[metric].mean() + second_sigma*df_interval_values[metric].std()
            lower_border = df_interval_values[metric].mean() - second_sigma*df_interval_values[metric].std()
            check_value = df_metrics[df_metrics['ts'] == df_metrics['ts'].max()][metric].iloc[0]
            
            df_alerts.loc[df_alerts['metric'] == metric, 'lower_border'] = lower_border
            df_alerts.loc[df_alerts['metric'] == metric, 'check_value'] = check_value
            df_alerts.loc[df_alerts['metric'] == metric, 'upper_border'] = upper_border
        
            if (check_value < upper_border) & (check_value > lower_border):
                continue
            else:
                df_alerts.loc[df_alerts['metric'] == metric, 'is_alert'] = 1
        return df_alerts

    @task
    def send_info_if_alert(df_metrics, df_alerts):
        '''
        Для каждой метрики проверяется значение is_alert, если is_alert = 1:
        - отправляются сообщение и график изменения метрики за последние 48 часов
        '''
        for metric in bi_links.keys():
            if df_alerts[df_alerts['metric'] == metric].iloc[0, 1]:
                # Сообщение
                msg = f"""{df_metrics["ts"].max().strftime("%d-%m-%Y %H:%M")}
Метрика {metric}
Текущее значение: {df_alerts[df_alerts['metric'] == metric]['check_value'].iloc[0].round(2)}
Границы доверительного интервала: ({df_alerts[df_alerts['metric'] == metric]['lower_border'].iloc[0].round(2)} - {df_alerts[df_alerts['metric'] == metric]['upper_border'].iloc[0].round(2)})
Ссылка на дашборд:
{bi_links[metric]}"""
                bot.sendMessage(chat_id=chat_id, text=msg)
                
                # График
                last_day = df_metrics['ts'].max() - pd.DateOffset(days=2)
                fig, ax = plt.subplots(1, figsize=(11, 7))
                
                sns.lineplot(x=df_metrics[df_metrics['ts'] >= last_day]['ts'],
                             y=df_metrics[df_metrics['ts'] >= last_day][metric],
                             color='#2e1e3b')
                
                sns.scatterplot(x=df_metrics[df_metrics['ts'] >= (df_metrics['ts'].max())]['ts'].iloc[0],
                                y=df_alerts.loc[df_alerts['metric'] == metric, 'upper_border'],
                                color='red',
                                marker='1')
                sns.scatterplot(x=df_metrics[df_metrics['ts'] >= (df_metrics['ts'].max())]['ts'].iloc[0],
                                y=df_alerts.loc[df_alerts['metric'] == metric, 'lower_border'],
                                color='red',
                                marker='2')
                
                ax.set(xlim=(last_day,
                             df_metrics['ts'].max() + pd.DateOffset(minutes=15)),
                       xlabel=None,
                       ylim=(None, None))
                ax.xaxis.set_major_locator(mdates.HourLocator(interval=3))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b %H:%M'))
                if metric != 'ctr':
                    ax.yaxis.set_major_formatter(ticker.EngFormatter())
                else:
                    ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
                    
                fig.suptitle(f'Последнее значение {metric} за {df_metrics["ts"].max().strftime("%d-%m-%Y %H:%M")}')
                
                fig.autofmt_xdate()
                fig.tight_layout()
        
                plot_object = io.BytesIO()
                fig.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'plot.png'
                plt.close()
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            else:
                continue

    df_metrics = extract_data()
    df_alerts = is_alert(df_metrics)
    send_info_if_alert(df_metrics, df_alerts)

dag_n_anikin_tg_alerts = dag_n_anikin_tg_alerts()
