import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
from pandas.tseries.offsets import BDay
import time
import psycopg2 as pg2
from sqlalchemy import create_engine
import mplfinance as mpf
from ta.volatility import BollingerBands
from ta.momentum import RSIIndicator
from pandas.tseries.offsets import BDay
import eikon as ek
import matplotlib.animation as animation
from matplotlib.animation import FuncAnimation

from config import eikon_api_key, postgre_access, postgre_engine

ek.set_app_key(eikon_api_key)
engine = create_engine(postgre_engine)

conn = pg2.connect(postgre_access)
cur = conn.cursor()
conn.autocommit = True

streaming_prices = ek.StreamingPrices(instruments=['EUR=', 'GBP=', 'JPY='], fields=['CF_BID', 'CF_ASK'])
streaming_prices.open()

j = 0

fig = mpf.figure(style='yahoo', figsize=(16, 8))
ax = fig.add_subplot()

j = 0


def add_bollinger_band(reform_df, window, window_dev, window_dev2):
    indicator_bb = BollingerBands(close=reform_df["close"], window=window, window_dev=window_dev)
    indicator_cc = BollingerBands(close=reform_df["close"], window=window, window_dev=window_dev2)

    reform_df['bb_bbm'] = indicator_bb.bollinger_mavg()
    reform_df['bb_bbh'] = indicator_bb.bollinger_hband()
    reform_df['bb_bbl'] = indicator_bb.bollinger_lband()

    reform_df['cc_bbh'] = indicator_cc.bollinger_hband()
    reform_df['cc_bbl'] = indicator_cc.bollinger_lband()

    reform_df['bb_bbh_indicator'] = reform_df["high"] > reform_df['bb_bbh']
    reform_df['bb_bbl_indicator'] = reform_df["low"] < reform_df['bb_bbl']
    reform_df['cc_bbh_indicator'] = reform_df["high"] > reform_df['cc_bbh']
    reform_df['cc_bbl_indicator'] = reform_df["low"] < reform_df['cc_bbl']

    reform_df['bb_bbh_value'] = indicator_bb.bollinger_hband() * reform_df['bb_bbh_indicator']
    reform_df['bb_bbl_value'] = indicator_bb.bollinger_lband() * reform_df['bb_bbl_indicator']

    reform_df['cc_bbh_stop_loss'] = indicator_cc.bollinger_hband() * reform_df['cc_bbh_indicator']
    reform_df['cc_bbl_stop_loss'] = indicator_cc.bollinger_lband() * reform_df['cc_bbl_indicator']

    reform_df = reform_df.replace(0, np.NaN)
    reform_df = reform_df[window - 1:]

    apds = [mpf.make_addplot(reform_df[['bb_bbm', 'bb_bbh', 'bb_bbl']], ax=ax),
            mpf.make_addplot(reform_df['bb_bbh_value'], type='scatter', markersize=50, marker="o", color='k', ax=ax),
            mpf.make_addplot(reform_df['bb_bbl_value'], type='scatter', markersize=50, marker="o", color='k', ax=ax),
            mpf.make_addplot(reform_df['cc_bbh_stop_loss'], type='scatter', markersize=50, marker="o", color='y',
                             ax=ax),
            mpf.make_addplot(reform_df['cc_bbl_stop_loss'], type='scatter', markersize=50, marker="o", color='y',
                             ax=ax),
            ]

    return reform_df, apds


def animate(i):
    df = streaming_prices.get_snapshot()

    date = datetime.datetime.now()

    # if date.hour ==

    jpy_bid = df.loc[0, "CF_BID"]
    jpy_ask = df.loc[0, "CF_ASK"]
    jpy_mid = (jpy_bid + jpy_ask) / 2

    gbp_bid = df.loc[1, "CF_BID"]
    gbp_ask = df.loc[1, "CF_ASK"]
    gbp_mid = (gbp_bid + gbp_ask) / 2

    eur_bid = df.loc[0, "CF_BID"]
    eur_ask = df.loc[0, "CF_ASK"]
    eur_mid = (eur_bid + eur_ask) / 2

    # sql = "INSERT INTO Price6 (Date, Bid, Ask, Mid) VALUES (%s, %s, %s, %s)"
    # val = (date, bid, ask, mid)

    sql = "INSERT INTO jpy_price (Date, Bid, Ask, Mid) VALUES (%s, %s, %s, %s)"
    sql2 = "INSERT INTO gbp_price (Date, Bid, Ask, Mid) VALUES (%s, %s, %s, %s)"
    sql3 = "INSERT INTO eur_price (Date, Bid, Ask, Mid) VALUES (%s, %s, %s, %s)"

    val1 = (date, jpy_bid, jpy_ask, jpy_mid)
    val2 = (date, gbp_bid, gbp_ask, gbp_mid)
    val3 = (date, eur_bid, eur_ask, eur_mid)

    cur.execute(sql, val1)
    cur.execute(sql2, val2)
    cur.execute(sql3, val3)
    # time.sleep(1)
    # streaming_prices.close()

    print(i)
    rows = 72000 * 2
    # rows = 2400
    if i > 0:

        eur_df = pd.read_sql(
            "select * from (select * from eur_price order by date desc limit {}) as temp order by date".format(rows),
            engine)

        eur_df.set_index('date', inplace=True)
        eur_df = eur_df['mid']
        resample_freq = 60
        reform_df = eur_df.resample('{}min'.format(resample_freq)).ohlc()
        reform_df.dropna(inplace=True)

        window = 20
        window_dev = 2.0
        window_dev2 = 2.5

        if len(reform_df) > window:
            bollinger_band = add_bollinger_band(reform_df, window, window_dev, window_dev2)

            reform_df = bollinger_band[0]
            apds = bollinger_band[1]

            ax.clear()
            mpf.plot(reform_df, ax=ax, addplot=apds, type='candle')

        else:
            print("wait2")

    else:
        print("wait")


ani = animation.FuncAnimation(fig, animate, interval=1000)

mpf.show()
