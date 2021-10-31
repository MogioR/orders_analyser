from Modules.order_analyser_service import OrderAnalyserService

service = OrderAnalyserService()
service.load_csv_data('orders.csv')
service.download_locations('1PonCXwHgKoRCFNnlm0ZwIZdSqCWKsIf4_E3IF8GFiUE', 'Environment/google_token.json')
service.download_sitemap('1PonCXwHgKoRCFNnlm0ZwIZdSqCWKsIf4_E3IF8GFiUE', 'Environment/google_token.json')

service.mark_data()

# # import pandas
# # service.data = pandas.read_csv('output.csv', sep='\t')

service.export('report.csv', '1PonCXwHgKoRCFNnlm0ZwIZdSqCWKsIf4_E3IF8GFiUE', 'Environment/google_token.json')

# print(service.regularity_check('Игорь ходил домой', ['Домой']))
# print(service.regularity_check('Домой ходил Игрорь', ['Домой']))
# print(service.regularity_check('Игорь ходил домой.', ['Домой']))
# print(service.regularity_check('Игорь домой ходил.', ['Домой']))
# print(service.regularity_check('Игорь домойходил.', ['Домой']))
# print(service.check_currency('Срубили сруб'))
# print(service.check_currency('Цена 10 руб.'))
# print(service.check_currency('Доллар упал'))
# print(service.check_currency('доллар упал'))
# print(service.check_currency('Пропало 10 рублей'))
#print(service.check_stop('Нужно выровнять дощатый пол в доме без вскрытия'))
# print(service.check_full_including_h1('Репетиторы польского языка'))
# print(service.check_full_including_h1('Репетиторы польского языка.'))
# print(service.check_full_including_h1('Репетиторы  язык'))