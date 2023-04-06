
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
sys.path.insert(0, 'C:/Users/taiz_/OneDrive/Documents/Documentos/CURSOS_Dev/Tera/Projeto/projeto_acao')


import price_economics_technical_analysis_class
from utils import *
from price_economics_technical_analysis_class import Price_Historical_Data_technical_analysis


class Coletar_dados():
    
    def __init__(self):
        self.home_path = 'C:/Users/taiz_/OneDrive/Documents/Documentos/CURSOS_Dev/Tera/Projeto/projeto_acao'
        self.data = Price_Historical_Data_technical_analysis()
        
    def american_info(self, token):
        # instanciando classes importadas
        #data = Price_Historical_Data_technical_analysis()
        #Producao Industrial Industrial Production Index - MENSAL - economic indicator that measures real output for all facilities located in the United States
        industry_production = self.data.get_american_economic_data(token, 'INDPRO')


        #Taxa de Desemprego = MENSAL
        desemprego_americano = self.data.get_american_economic_data(token,'UNRATE')

        #Em Dolar The Consumer Price Index for All Urban Consumers (CPIAUCSL) is a price index of a basket of goods and services paid by urban consumers
        inflacao_americana = self.data.get_american_economic_data(token,'CPIAUCSL')

        # Tx de Juros americano- mês
        tx_juros_americano = self.data.get_american_economic_data(token,'FEDFUNDS')
        
        aux1 =pd.merge(industry_production, desemprego_americano, on="date", how='left')
        aux2 = pd.merge(aux1, inflacao_americana, on="date", how='left' )
        df_dados_americanos = pd.merge(aux2, tx_juros_americano, on="date", how='left' )

        df_dados_americanos.rename({'INDPRO': 'ind_prod_americana', 'UNRATE': 'desemprego_americano', 'CPIAUCSL': 'consumer_priceUSD', 'FEDFUNDS': 'tx_juros_americano'}, axis= 'columns', inplace = True )
        
        return df_dados_americanos


    def mudar_granularidade(self, info_american_from2000 ):
        # Tx de juros - equivalencia financeira - a partir de 2000
        tx_juros_americano_day = mensal_diario_rate_nome(info_american_from2000['tx_juros_americano'], 'tx_juros_americano_day')
        inflacao_americana_daily = mensal_diario_rate_nome(info_american_from2000['consumer_priceUSD'], 'inflacao_americana_daily')
        # Conversão mensal para diário - conversão simple (/business day) - somente a partir de 2000
        industry_production_daily = conversao_simples_mensal_diario_B_nome(info_american_from2000['ind_prod_americana'], 'industry_production_daily')
        desemprego_americano_daily = conversao_simples_mensal_diario_B_nome(info_american_from2000['desemprego_americano'], 'desemprego_americano_day')

        aux1 =pd.merge(tx_juros_americano_day, industry_production_daily, on="date", how='left')
        aux2 = pd.merge(aux1, desemprego_americano_daily, on="date", how='left' )
        df_dados_americanos_daily = pd.merge(aux2, inflacao_americana_daily, on="date", how='left' )
        df_dados_americanos_daily.index.name= 'data'

        return df_dados_americanos_daily

    def brazil_info(self, stock):

        cotacao_acoesbr = self.data.cotacao_conjunto_acao_somente_adj_close(['USDBRL=X', '^BVSP'],'2000-01-01',  datetime.datetime.today() - datetime.timedelta(days=1) )
        cotacao_acoesbr.rename({ 'USDBRL=X': 'DOLAR','^BVSP':'IBOVESPA' },axis= 'columns', inplace= True)

        stock_all = self.data.coleta_dados_yf(stock, '2000-01-01',  datetime.datetime.today() - datetime.timedelta(days=1))
        volume_ambev = stock_all['Volume']
        price_stock_adj_close = stock_all['Adj Close']
        price_stock_high = stock_all['High']
        price_stock_low = stock_all['Low']

        ibovespa_all = self.data.coleta_dados_yf('^BVSP', '2000-01-01',  datetime.datetime.today() - datetime.timedelta(days=1))
        volume_ibovespa= ibovespa_all['Volume']

        df = pd.DataFrame()
        df['volume_ibovespa'] = volume_ibovespa
        df['volume'] = volume_ambev
        df['close_stock'] = price_stock_adj_close
        df['high_stock'] = price_stock_high
        df['low_stock'] = price_stock_low

        df.index.name= 'data'
        

        indi_tecnicos = self.data.indicadores_tecnicos(stock, '2000-01-01',  datetime.datetime.today() - datetime.timedelta(days=1))

        aux = pd.merge(df,cotacao_acoesbr, on= 'data', how= 'left' )
        #juntando indicadores e cotação da ação
        aux2 = pd.concat([aux, indi_tecnicos], axis = 1)

        risco_brasil = self.data.risco_brasil()

        cdi = self.data.consulta_bcb('cdi',12)

        junta_cdi = pd.merge(aux, cdi, on= 'data', how= 'left')
        junta_risco = pd.merge(junta_cdi, risco_brasil, on= 'data', how= 'left')

        # MENSAL - CONSULTA Banco Central
        ipca_mensal = self.data.consulta_bcb('ipca_mensal',433)
        ipca_alimentos_bebidas_mensal = self.data.consulta_bcb('ipca_alimentos_bebidas',1635)
        igpm_mensal = self.data.consulta_bcb('igpm',189)
        selic_meta_mensal = self.data.consulta_bcb('selic_meta_mensal',432)
        selic_overnight_mensal = self.data.consulta_bcb('selic_overnight_mensal',4189)
        taxa_desemprego_pnad_mensal = self.data.consulta_bcb('taxa_desemprego_pnad_mensal', 24369)
        indice_volume_vendas_varejo_setor_mensal = self.data.consulta_bcb('indice_volume_vendas_varejo_setor_mensal',1496)

        indice_producao_bens_consumo_mensal = self.data.consulta_bcb('indice_producao_bens_consumo_mensal',21867)


                #indicadores_economicos_mensal_br 
        ipca_alimentos = pd.DataFrame(ipca_alimentos_bebidas_mensal)
        ipca = pd.DataFrame(ipca_mensal)
        igpm = pd.DataFrame(igpm_mensal)
        selic_meta = pd.DataFrame(selic_meta_mensal)
        selic_overnight = pd.DataFrame(selic_overnight_mensal)
        taxa_desemprego = pd.DataFrame(taxa_desemprego_pnad_mensal)
        volume_vendas_setor = pd.DataFrame(indice_volume_vendas_varejo_setor_mensal)
        producao_bens_consumo = pd.DataFrame(indice_producao_bens_consumo_mensal)

        # merge

        aux = pd.merge(ipca_alimentos, ipca , on='data',how='left')
        aux1 = pd.merge(aux, igpm , on='data',how='left')
        aux2 = pd.merge(aux1, selic_meta , on='data',how='left')
        aux3 = pd.merge(aux2, selic_overnight , on='data',how='left')
        aux4 = pd.merge(aux3, taxa_desemprego , on='data',how='left')
        aux5 =pd.merge(aux4, volume_vendas_setor , on='data',how='left')
        indicador_econ_mensal_br = pd.merge(aux5, producao_bens_consumo , on='data',how='left')


                ##DIARIO - CONVERSAO DO MENSAL

        # Txa juros, selic: Tem que fazer a equivalencia financeira
        selic_meta_diario = mensal_diario_rate(selic_meta_mensal)
        selic_overnight_diario = mensal_diario_rate(selic_overnight_mensal)
        ipca_diario = mensal_diario_rate(ipca_mensal)


        # Conversão simples valor mensal / 22 ( médias business day)
        igpm_diario = cenversao_simples_mensal_diario_B(igpm_mensal)
        taxa_desemprego_pnad_diario = cenversao_simples_mensal_diario_B(taxa_desemprego_pnad_mensal)
        indice_volume_vendas_varejo_setor_diario = cenversao_simples_mensal_diario_B(indice_volume_vendas_varejo_setor_mensal)

        #renomear
        indice_volume_vendas_varejo_setor_diario.rename({'indice_volume_vendas_varejo_setor_mensal_diario': 'volume_vendas_varejo_setor_diario' }, axis= 'columns', inplace = True)


        indice_producao_bens_consumo_diario = cenversao_simples_mensal_diario_B(indice_producao_bens_consumo_mensal)

        #renomear
        indice_producao_bens_consumo_diario.rename({'indice_producao_bens_consumo_mensal_diario': 'producao_bens_consumo_diario' }, axis= 'columns', inplace = True)
        indice_volume_vendas_varejo_setor_diario.rename({'indice_volume_vendas_varejo_setor_mensal_diario': 'volume_vendas_varejo_setor_diario' }, axis= 'columns', inplace = True)

        # Juntando TUDO
        ax = pd.merge(selic_meta_diario, selic_overnight_diario, on= 'data', how= 'left' )
        ax1  = pd.merge(ax,taxa_desemprego_pnad_diario , on= 'data', how= 'left' )
        ax2  = pd.merge(ax1,indice_volume_vendas_varejo_setor_diario , on= 'data', how= 'left' )
        ax3  = pd.merge(ax2,ipca_diario  , on= 'data', how= 'left' )
        ax4  = pd.merge(ax3,igpm_diario  , on= 'data', how= 'left' )
        ax5  = pd.merge(ax4,indice_volume_vendas_varejo_setor_diario  , on= 'data', how= 'left' )
        ax6  = pd.merge(ax5,indice_producao_bens_consumo_diario , on= 'data', how= 'left' )
        ax7  = pd.merge(ax6,df_dados_americanos_daily , on= 'data', how= 'left' )

        df_diaria_final = pd.merge(junta_risco, ax7, on='data', how='left')


        df_diaria_final= df_diaria_final.round(3)

        return df_diaria_final



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
    #print(df_dados_americanos)

    #filter date from 2000
    info_american_from2000= df_dados_americanos.loc['2000':]

    # change granularity from monthly to daily
    df_dados_americanos_daily = cd.mudar_granularidade( info_american_from2000 )
    #print(df_dados_americanos_daily)

    
    df_diaria_final= cd.brazil_info('ABEV3.SA') 
    df_diaria_final.to_csv('./data/raw/df_raw0.csv')



    