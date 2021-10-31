import re

import pandas as pd
from natasha import (
    Segmenter,
    NewsEmbedding,
    NewsNERTagger,
    Doc
)

from Modules.google_sheets_api import GoogleSheetsApi
import nltk
import pymorphy2
nltk.download('punkt')


class OrderAnalyserService:
    def __init__(self):
        self.data = pd.DataFrame()
        self.locations = []
        self.h1 = []

        # Natasha parser
        self.segmenter = Segmenter()
        self.emb = NewsEmbedding()
        self.ner_tagger = NewsNERTagger(self.emb)

        # pymorphy2 parser
        self.prob_thresh = 0.8
        self.morph = pymorphy2.MorphAnalyzer()

    # Download locations from google sheets table_id, list temp with token
    def download_locations(self, table_id, token):
        service = GoogleSheetsApi(token)
        data = service.get_data_from_sheets(table_id, 'temp', 'A1', 'A' +
                                            str(service.get_list_size(table_id, 'temp')[1]), 'COLUMNS')
        self.locations = data[0]

    # Download h1 from google sheets table_id, list temp with token
    def download_sitemap(self, table_id, token):
        service = GoogleSheetsApi(token)
        data = service.get_data_from_sheets(table_id, 'sitemap', 'B2', 'B' +
                                            str(service.get_list_size(table_id, 'sitemap')[1]), 'COLUMNS')
        self.h1 = data[0]

    # Load strings from csv_file
    def load_csv_data(self, csv_file):
        raw = pd.read_csv(csv_file, sep='\t', header=None, error_bad_lines=False)
        self.data['id'] = raw[0]
        self.data['containerID'] = raw[1]
        self.data['orderPrice'] = raw[2]
        self.data['shortDesc'] = raw[3]
        self.data['number'] = raw[4]
        self.data['description'] = raw[5]
        self.data['createdAt'] = raw[6]
        self.data['toloka'] = raw[7]
        self.data['vacancyID'] = raw[8]

    # Make marks in data
    def mark_data(self):
        print(1)
        self.data['currency'] = list(map(self.check_currency, self.data['shortDesc']))
        print(2)
        self.data['date'] = list(map(self.check_date, self.data['shortDesc']))
        print(3)
        self.data['punct'] = list(map(self.check_punkt, self.data['shortDesc']))
        print(4)
        self.data['stop'] = list(map(self.check_stop, self.data['shortDesc']))
        print(5)
        self.data['location'] = list(map(self.check_location, self.data['shortDesc']))
        print(6)
        self.data['count'] = list(map(self.check_count, self.data['shortDesc']))
        print(7)
        self.data['lowercase'] = list(map(self.check_lowercase, self.data['shortDesc']))
        print(9)
        self.data['backspace'] = list(map(self.check_backspace, self.data['shortDesc']))
        print(10)
        self.data['person'] = list(map(self.check_person_pymorphy2, self.data['shortDesc']))
        print(11)
        self.data['dublicates'] = self.data.duplicated(subset=['shortDesc']).values
        print(12)
        self.data['dublicates_h1'] = list(map(self.check_full_including_h1, self.data['shortDesc']))
        print(13)
        self.data.to_csv('output.csv', sep='\t', index=False, header=True)
        print(14)

    # Return problems string
    def make_problems_string(self, row):
        problems = []
        if row['currency']:
            problems.append('currency')
        if row['date']:
            problems.append('date')
        if row['punct']:
            problems.append('punkt')
        if row['stop']:
            problems.append('stop')
        if row['location']:
            problems.append('location')
        if row['count']:
            problems.append('count')
        if row['lowercase']:
            problems.append('lowercase')
        if row['backspace']:
            problems.append('backspace')
        if row['person']:
            problems.append('person')
        if row['dublicates']:
            problems.append('dublicates')
        if row['dublicates_h1']:
            problems.append('dublicates_h1')

        return ', '.join(problems)

    # Make export csv file with name file
    def export(self, file, document_id, token):
        export_id = ['id']
        export_container_id = ['containerID']
        export_order_price = ['orderPrice']
        export_short_desc = ['shortDesc']
        export_number = ['number']
        export_description = ['description']
        export_created_at = ['createdAt']
        export_toloka = ['toloka']
        export_vacancy_id = ['vacancyID']
        export_problems = ['problem']

        output_df = pd.DataFrame()
        for i, row in self.data.iterrows():
            problems = self.make_problems_string(row)
            if problems != "":
                output_df = output_df.append({'name': row['shortDesc'], 'problem': problems}, ignore_index=True)
                export_id.append(str(row['id']))
                export_container_id.append(str(row['containerID']))
                export_order_price.append(str(row['orderPrice']))
                export_short_desc.append(str(row['shortDesc']))
                export_number.append(str(row['number']))
                export_description.append(str(row['description']))
                export_created_at.append(str(row['createdAt']))
                export_toloka.append(str(row['toloka']))
                export_vacancy_id.append(str(row['vacancyID']))
                export_problems.append(problems)

        service = GoogleSheetsApi(token)
        service.put_column_to_sheets(document_id, 'output', 'A', 1, export_id)
        service.put_column_to_sheets(document_id, 'output', 'B', 1, export_container_id)
        service.put_column_to_sheets(document_id, 'output', 'C', 1, export_order_price)
        service.put_column_to_sheets(document_id, 'output', 'D', 1, export_short_desc)
        service.put_column_to_sheets(document_id, 'output', 'E', 1, export_number)
        service.put_column_to_sheets(document_id, 'output', 'F', 1, export_description)
        service.put_column_to_sheets(document_id, 'output', 'G', 1, export_created_at)
        service.put_column_to_sheets(document_id, 'output', 'H', 1, export_toloka)
        service.put_column_to_sheets(document_id, 'output', 'I', 1, export_vacancy_id)
        service.put_column_to_sheets(document_id, 'output', 'J', 1, export_problems)

        output_df.to_csv(file, sep='\t', index=False, header=True)

    # Return true if data including price
    @staticmethod
    def check_currency(data: str):
        direct_dict = ['руб']
        include_dict = ['рубл', 'доллар']

        direct_including = OrderAnalyserService.regularity_check(data, direct_dict)
        just_including = OrderAnalyserService.including_check(data, include_dict)

        return direct_including or just_including

    # Return true if data including date
    @staticmethod
    def check_date(data: str):
        direct_dict = ['мая']
        include_dict = ['2020', '2021', '2019', 'январ', 'феврал', 'март', 'апрел', 'май', 'июн', 'июл',
                        'август', 'сентябр', 'октябр', 'ноябр', 'декабр', 'понедельник', 'вторник', 'среда', 'среду',
                        'средам', 'четверг', 'пятниц', 'суббот', 'воскресень']

        direct_including = OrderAnalyserService.regularity_check(data, direct_dict)
        just_including = OrderAnalyserService.including_check(data, include_dict)

        return direct_including or just_including

    # Return true if data including incorrect punctuation
    @staticmethod
    def check_punkt(data: str):
        data_str = str(data)
        # Del кв.м.
        data_str = re.sub(r'[^A-Za-zА-ЯЁа-яё]кв.м.[^A-Za-zА-ЯЁа-яё]', '', data_str)
        data_str = re.sub(r'[^A-Za-zА-ЯЁа-яё]кв.м[^A-Za-zА-ЯЁа-яё]', '', data_str)
        # Del 8,3
        data_str = re.sub(r'\d[,.]\d', '', data_str)

        punkt_flag = re.search(r'[^\w\d\-\*,\ ]', data_str) is not None      # Stop symbols
        comma_flag = data_str.count(',') > 1                        # More then 1 comma

        return punkt_flag or comma_flag

    # Return true if data including stop word
    @staticmethod
    def check_stop(data: str):
        direct_dict = ["срочно", "нужно", "нужен", "нужны", "профессионально", "качественно", "ищем", "ищу", "шт"]
        include_dict = ['спасибо', 'пожалуйста', 'благодар', 'вчера', 'сегодня', 'завтра', 'недел',  'специалист',
                        'мастер', 'поиск', 'штук']

        direct_including = OrderAnalyserService.regularity_check(data, direct_dict)
        just_including = OrderAnalyserService.including_check(data, include_dict)

        return direct_including or just_including

    # Return true if data including location in location list
    def check_location(self, data: str):
        include_dict = self.locations
        just_including = OrderAnalyserService.including_check(data, include_dict)

        return just_including

    # Return true if data including more then 8 words or less then 4
    @staticmethod
    def check_count(data: str):
        data = str(data)
        minimum_flag = len(data.split(' ')) < 4                    # minimum 4 words
        maximum_flag = len(data.split(' ')) > 8                    # maximum 8 words

        return minimum_flag or maximum_flag

    # Return true if data including more then one capital char
    @staticmethod
    def check_uppercase(data: str):
        data = str(data)
        return sum(map(str.isupper, data)) > 1

    # Return true if first char not capital
    @staticmethod
    def check_lowercase(data: str):
        data = str(data)
        return data[0].isupper is False

    # Return true if data including double space
    @staticmethod
    def check_backspace(data: str):
        data = str(data)
        return data.find('  ') != -1

    # Return true if data including name
    def check_person_natasha(self, data: str):
        data = str(data)
        doc = Doc(data)
        doc.segment(self.segmenter)
        doc.tag_ner(self.ner_tagger)

        person_flag = False
        for span in doc.spans:
            if span.type == 'PER':
                person_flag = True
                print(span.text)
                break

        return person_flag

    def check_person_pymorphy2(self, data: str):
        tokens = nltk.word_tokenize(str(data))

        for word in tokens:
            for p in self.morph.parse(word):
                if 'Name' in p.tag and p.score >= self.prob_thresh:
                    #print(word, '', p.score)
                    return True
        return False

    # Return true if data including h1
    def check_full_including_h1(self, data: str):
        return data in self.h1

    # Return "direct" including words in list dictionary in string data.
    @staticmethod
    def regularity_check(data: str, dictionary: list):
        including = False
        data_str = str(data).lower()

        for word in dictionary:
            including = including or (re.search(r'^'+word.lower()+'[^A-Za-zА-ЯЁа-яё]|[^A-Za-zА-ЯЁа-яё]'+word.lower()+
                                                '[^A-Za-zА-ЯЁа-яё]|[^A-Za-zА-ЯЁа-яё]'+word.lower()+'$'
                                                , data_str) is not None)
            if including is True:
                break

        return including

    # Return including words in list dictionary in string data.
    @staticmethod
    def including_check(data: str, dictionary: list):
        including = False
        data_str = str(data).lower()

        for word in dictionary:
            including = including or (str(data_str).find(word.lower()) != -1)
            if including is True:
                break

        return including
