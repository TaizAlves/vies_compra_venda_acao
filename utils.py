import yfinance as yf

# Instanciando o Ticker para pegar informações mais específicas da empresa



class Dados_Fundamentalista:
    def __init__(self, stock):
        self.stock = stock
        

    def setStock(self, stock):
        self.stock = stock



    def ticker_yf_dadosempresa_fundamentalista(self):

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

        

        


#inicializando
ticker = Dados_Fundamentalista('ABEV3.SA')
info, market_data, actions, financial, major_holders, balance_sheet, calendar= ticker.ticker_yf_dadosempresa_fundamentalista()

print(info)

