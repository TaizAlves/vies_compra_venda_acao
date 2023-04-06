import datetime

import pandas as pd
import numpy as np

import datapungi_fed as dpf
from bcb import sgs
import yfinance as yf
import talib
from talib import RSI, ROC,  WILLR

import bs4 as bs
import requests
import json


class Price_Historical_Data_technical_analysis:
    """
    Coleta informações de diferentes fontes para montar df
    """

    def __init__(self):
        self.home_path = 'G:\DADOS\Documents\CURSOS_Dev\Tera\Projeto\projeto_acao'

    def setStock(self, stock):
        self.stock = stock

    def getStock(self):
        return self.stock

    def risco_brasil(self):
        resp = requests.get(
            'http://www.ipeadata.gov.br/ExibeSerie.aspx?serid=40940&module=M')
        soup = bs.BeautifulSoup(resp.text)
        table = soup.find('table', {'class': 'dxgvTable'})
        embi_risco_brasil = []
        data = []
        df_risco_brasil = pd.DataFrame()
        for row in table.findAll('tr')[3:]:

            date = row.findAll('td')[0].text
            embi = row.findAll('td')[1].text
            data.append(date)
            embi_risco_brasil.append(embi)

        df_risco_brasil['data'] = data
        df_risco_brasil['Risco_Brasil'] = embi_risco_brasil

        df_risco_brasil['data'] = pd.to_datetime(
            df_risco_brasil['data'], format='%d/%m/%Y')

        df_risco_brasil = df_risco_brasil.sort_values('data')
        df_risco_brasil.set_index("data", inplace=True)

        #df_risco_brasil.to_csv(
        #    self.home_path + '/data/external/risco_brasil_{}.csv'.format(pd.to_datetime('today').date()))

        return df_risco_brasil

    def consulta_bcb(self, nome, codigo, start='2000-01-01'):
        """
        Coleta informações da api no Banco Central
        nome: 'cdi'
        codigo: 12
        return: dataframe

        """
        nome_ = sgs.get((nome, codigo), start=start)

        nome_.index.name = 'data'
        return nome_



    def coleta_dados_yf(self, stock, start, end, interval='1d'):
        """
        Coleta valores Open, High, Low , Close, Adj Close, Volume de ação  api yfinance
        stock format: 'ABEV3.SA' para as ações brasileiras
        start, end format: '2000-01-01'
        interval: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo   default:'1d'
        Returns: dataFrame
        index.name = 'data'
        """
        acao = yf.download(stock, start=start, end=end)
        acao.index.name = 'data'

        return acao

    def cotacao_conjunto_acao_somente_adj_close(self, lista, start, end, interval='1d'):
        """
        Coleta somente ['Adj Close'] de multiplas ações api yfinance
        lista: []
        stock format: 'ABEV3.SA' para as ações brasileiras
        start, end format: '2000-01-01'
        interval: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo   default:'1d'
        Returns: dataFrame
        index.name = 'data'
        """
        # Cotações
        cotacoes = yf.download(lista, start=start, end=end)['Adj Close']
        cotacoes.index.name = 'data'
        return cotacoes

    def indicadores_tecnicos(self, stock, start, end):
        """
        Get techinical analysis data from historical data api talib
        RSI / ROC / WILLIANS %R
        Returns: DataFrame
        """
        acao = self.coleta_dados_yf(stock, start, end)
        price = acao.iloc[::-1]
        price = price.dropna()
        close = price.Close.values
        open = price.Open.values
        high = price.High.values
        low = price.Low.values
        volume = price.Volume.values

        # RSI
        rsi = RSI(close, timeperiod=14)

        # ROC
        roc = ROC(close, timeperiod=10)

        # Williams %R moves between zero and -100.above -20 is overbought.-80 is oversold.
        willr = WILLR(high, low, close, timeperiod=14)

        data = {'rsi': rsi,
                'roc': roc,
                'willr': willr
                }

        df = pd.DataFrame(data)

        return df

    def get_american_economic_data(self, token, code):
        """
        American Economical data from FRED
        code:'UNRATE'
        Returns: list

        """
        data = dpf.data(token)
        info = data.series(code)

        return info





#data = Price_Historical_Data_technical_analysis()
#acao = data.cotacao_conjunto_acao_somente_adj_close(['USDBRL=X','EURBRL=X', '^BVSP','HEIA.AS', 'JBSS3.SA', 'BRFS3.SA' ], '2021-01-01',  datetime.datetime.today() - datetime.timedelta(days=1), '1m')
#indi_tecnicos = data.indicadores_tecnicos('HEIA.AS', '2021-01-01', '2021-04-01')

##with open('./fred_token.json') as arquivo:
   # fed_credencial = json.load(arquivo)

#token = fed_credencial['token']
#america = data.get_american_economic_data(token, 'UNRATE')
#print(america)
