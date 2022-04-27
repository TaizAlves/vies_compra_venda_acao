
#from scipy.stats import zscore

import datetime

import pandas as pd
import numpy as np

import datapungi_fed as dpf
from bcb import sgs
import yfinance as yf
import investpy as inv
import talib
from talib import RSI, ROC,  WILLR

import bs4 as bs
import pickle
import requests
import json



#import plotly.express as px
#import matplotlib.pyplot as plt
#import seaborn as sns
import sys
  
# adding Folder_2 to the system path
sys.path.insert(0, 'G:\DADOS\Documents\CURSOS_Dev\Tera\Projeto\projeto_acao')


import price_economics_technical_analysis_class
from utils import Dados_Fundamentalista
from price_economics_technical_analysis_class import Price_Historical_Data_technical_analysis


class Coletar_dados():
    
    def __init__(self):
        self.home_path = 'G:\DADOS\Documents\CURSOS_Dev\Tera\Projeto\projeto_acao'
        
    def american_info(self, token):
        # instanciando classes importadas
        data = Price_Historical_Data_technical_analysis()
        #Producao Industrial Industrial Production Index - MENSAL - economic indicator that measures real output for all facilities located in the United States
        industry_production = data.get_american_economic_data(token, 'INDPRO')


        #Taxa de Desemprego = MENSAL
        desemprego_americano = data.get_american_economic_data(token,'UNRATE')

        #Em Dolar The Consumer Price Index for All Urban Consumers (CPIAUCSL) is a price index of a basket of goods and services paid by urban consumers
        inflacao_americana = data.get_american_economic_data(token,'CPIAUCSL')

        # Tx de Juros americano- mês
        tx_juros_americano = data.get_american_economic_data(token,'FEDFUNDS')
        
        aux1 =pd.merge(industry_production, desemprego_americano, on="date", how='left')
        aux2 = pd.merge(aux1, inflacao_americana, on="date", how='left' )
        df_dados_americanos = pd.merge(aux2, tx_juros_americano, on="date", how='left' )

        df_dados_americanos.rename({'INDPRO': 'ind_prod_americana', 'UNRATE': 'desemprego_americano', 'CPIAUCSL': 'consumer_priceUSD', 'FEDFUNDS': 'tx_juros_americano'}, axis= 'columns', inplace = True )
        
        return df_dados_americanos

if __name__ == '__main__':


    with open('./fred_token.json') as arquivo:
        fed_credencial = json.load(arquivo)

    token = fed_credencial['token']
    
    # instanciando classes importadas
    #data = Price_Historical_Data_technical_analysis()
    ticker = Dados_Fundamentalista('ABEV3.SA')
    
    # instanciando Coletar_dados
    cd = Coletar_dados()

    #get american data
    df_dados_americanos = cd.american_info(token)
    print(df_dados_americanos)
    