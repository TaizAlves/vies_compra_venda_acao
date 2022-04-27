import yfinance as yf
import investpy as inv

# Instanciando o Ticker para pegar informações mais específicas da empresa



class Dados_Fundamentalista:
    """
    Informações para análise Fundamentalista
    
    """
    def __init__(self, stock):
        self.stock = stock
        

    def setStock(self, stock):
        self.stock = stock

    def getStock(self):
        return self.stock



    def ticker_yf_dadosempresa_fundamentalista(self):
        """
        Coletando dados fundamentalista através da ipi yfinance

        stock format: 'ABEV3.SA' para as ações brasileiras
        Return: info, market_data, actions, financial, major_holders, balance_sheet, calendar

        """

        ticker = yf.Ticker(self.stock)

        #get stock info
        info = ticker.info

        #get historical market data
        market_data =  ticker.history(period="max")

        #show actions (dividends, splits)
        actions = ticker.actions
        # show financials
        financial = ticker.financials
        

        # show major holders
        major_holders = ticker.major_holders


        # show balance sheet
        balance_sheet = ticker.balance_sheet
        

        # show next event (earnings, etc)
        calendar = ticker.calendar

        return info, market_data, actions, financial, major_holders, balance_sheet, calendar

    
    def fundamental_analysis (self, country= 'brazil'):
        """
        Coletando dados fundamentalista através da ipi investpy 

        country: país da ação, default = 'brazil'
        stock format: 'ABEV3'

        Returns:	dataframe, Today Range, 52 wk Range, EPS, 	Market Cap, Dividend (Yield), Average Vol. (3m) ,	P/E Ratio, 	Beta ,	1-Year Change ,	Shares Outstanding, 	Next Earnings Date

        """

        dados_fundamentalista = inv.get_stock_information(self.stock, country= country)
        
        return dados_fundamentalista

    def today_technical_stock_analysis_trend(self, country= 'brazil', product_type='stock'):
        """
        Coletando dados técnicos e tendências através da ipi investpy 
        country: país da ação, default = 'brazil'
        product_type: default='stock'
        stock format: 'ABEV3'
        Returns: dataframe, 12 indicadores técnicos e a respectiva leitura da tendência de cada indicador
        """

        indicadores_tecnicos_sinal = inv.technical_indicators(self.stock, country= country, product_type=product_type)
        
        return indicadores_tecnicos_sinal

    def  stock_signal_moving_average(self, country= 'brazil', product_type='stock'):
        """
        Sinal de têndencia de compra, venda ação de acordo com o moving average
        country: país da ação, default = 'brazil'
        product_type: default='stock'
        stock format: 'ABEV3'
        Returns: dataframe
        """
        moving_average_signal = inv.moving_averages(self.stock, country=country, product_type= product_type)

        return moving_average_signal




        

        

if __name__ == '__main__':
    #inicializando
    ticker = Dados_Fundamentalista('ABEV3.SA')
    #info, market_data, actions, financial, major_holders, balance_sheet, calendar= ticker.#ticker_yf_dadosempresa_fundamentalista()
    ticker.setStock('ABEV3') 

    print(ticker.getStock())
    #dados_fundamentalistas = ticker.fundamental_analysis()
    #tecnico = ticker.today_technical_stock_analysis_trend()
    mov = ticker.stock_signal_moving_average()
    print(mov)

    


