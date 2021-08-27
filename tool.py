from os import walk
import csv
import re
from datetime import datetime
import requests

csv.field_size_limit(100000000)

class ImportTools:
    weeks = {
        '-40': 'P1', '-39': 'P2', '-38': 'P3', '-37': 'P4', '-36': 'P5', '-35': 'P6', '-34': 'P7',
         '-33': 'P8', '-32': 'P9', '-31': 'P10', '-30': 'P11', '-29': 'P12', '-28': 'P13', '-27': 'P14',
         '-26': 'P15', '-25': 'P16', '-24': 'P17', '-23': 'P18', '-22': 'P19', '-21': 'P20', '-20': 'P21',
         '-19': 'P22', '-18': 'P23', '-17': 'P24', '-16': 'P25', '-15': 'P26', '-14': 'P27', '-13': 'P28',
         '-12': 'P29', '-11': 'P30', '-10': 'P31', '-9': 'P32', '-8': 'P33', '-7': 'P34', '-6': 'P35',
         '-5': 'P36', '-4': 'P37', '-3': 'P38', '-2': 'P39', '-1': 'P40', '1': 'B1', '2': 'B2', '3': 'B3',
         '4': 'B4', '5': 'B5', '6': 'B6', '7': 'B7', '8': 'B8', '9': 'B9', '10': 'B10', '11': 'B11', '12': 'B12',
         '13': 'B13', '14': 'B14', '15': 'B15', '16': 'B16', '17': 'B17', '18': 'B18', '19': 'B19', '20': 'B20',
         '21': 'B21', '22': 'B22', '23': 'B23', '24': 'B24', '25': 'B25', '26': 'B26', '27': 'B27', '28': 'B28',
         '29': 'B29', '30': 'B30', '31': 'B31', '32': 'B32', '33': 'B33', '34': 'B34', '35': 'B35', '36': 'B36',
         '37': 'B37', '38': 'B38', '39': 'B39', '40': 'B40', '41': 'B41', '42': 'B42', '43': 'B43', '44': 'B44',
         '45': 'B45', '46': 'B46', '47': 'B47', '48': 'B48', '49': 'B49', '50': 'B50', '51': 'B51', '52': 'B52',
         '53': 'B53', '54': 'B54', '55': 'B55', '56': 'B56', '57': 'B57', '58': 'B58', '59': 'B59', '60': 'B60',
         '61': 'B61', '62': 'B62', '63': 'B63', '64': 'B64', '65': 'B65', '66': 'B66', '67': 'B67', '68': 'B68',
         '69': 'B69', '70': 'B70', '71': 'B71', '72': 'B72', '73': 'B73', '74': 'B74', '75': 'B75', '76': 'B76',
         '77': 'B77', '78': 'B78', '79': 'B79', '80': 'B80', '81': 'B81', '82': 'B82', '83': 'B83', '84': 'B84',
         '85': 'B85', '86': 'B86', '87': 'B87', '88': 'B88', '89': 'B89', '90': 'B90', '91': 'B91', '92': 'B92',
         '93': 'B93', '94': 'B94', '95': 'B95', '96': 'B96', '97': 'B97', '98': 'B98', '99': 'B99', '100': 'B100',
         '101': 'B101', '102': 'B102', '103': 'B103', '104': 'B104', '105': 'B105',
    }

    def __init__(self, filename = '', workdir = './', path_to_feeds_dir = './', delimiter = ';', content_type = 'node',
                 multilingual = 0,
                 prefix = '_en'):
        self.filename = filename
        self.workdir = workdir
        self.path_to_feeds_dir = path_to_feeds_dir
        self.content_type = content_type
        self.delimiter = delimiter
        self.multilingual = multilingual
        self.prefix = prefix
        self.pattern = '(?P<url>https?://\S+\.(?:jpg|gif|png))'

    def get_content(self):
        with open(f'{self.workdir}/{self.filename}') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter = self.delimiter, quotechar='~')
            content = [row for row in csv_reader]
            return content

    def generate_new_file(self):
        content = self.get_content()
        name_field = ''

        if self.content_type == 'node':
	        content[0].append('shared_content')

        if self.multilingual == 0:
            for i in range(len(content[0])):
                content[0][i] = replace_ending(content[0][i], self.prefix, '')

        if self.content_type == 'taxonomy':
            label_col = content[0].index('name_field')
            content = list(filter(lambda row: row[label_col] and row[label_col].strip(), content))
        elif self.content_type == 'node':
            label_col = content[0].index('title_field')
            content = list(filter(lambda row: row[label_col] and row[label_col].strip(), content))

        for row in content[1::]:
            if self.content_type == 'node':
            	row.append('Local')

            if self.content_type == 'taxonomy':
                name_field = row[content[0].index('name_field')]
            elif self.content_type == 'node':
                name_field = row[content[0].index('title_field')].strip()
                row[content[0].index('title_field')] = name_field

            if 'stage_weeks' in content[0] and \
                    len(row[content[0].index('stage_weeks')]) > 1:
                weeks_arr = row[content[0].index('stage_weeks')].split('|')
                new_str = [self.weeks[item] for item in weeks_arr if item in self.weeks]
                row[content[0].index('stage_weeks')] = '|'.join(new_str)

            if 'stages' in content[0] and \
                    len(row[content[0].index('stages')]) > 1:
                stage_arr = [item.strip() for item in row[content[0].index('stages')].split('|')]
                row[content[0].index('stages')] = '|'.join(stage_arr)

            if 'field_ingredients' in content[0] and \
                    len(row[content[0].index('field_ingredients')]) > 1:
                ingredients_arr = [item.strip() for item in row[content[0].index('field_ingredients')].split('|')]
                row[content[0].index('field_ingredients')] = '|'.join(ingredients_arr)


            if 'field_image_alt' in content[0] and \
                    len(row[content[0].index('field_image_alt')]) < 1:
                row[content[0].index('field_image_alt')] = name_field.strip()

            if 'field_image_alt' in content[0] and len(row[content[0].index('field_image_alt')]) < 1:
                   row[content[0].index('field_image_alt')] = name_field.strip()

            if 'field_image_title' in content[0] and len(row[content[0].index('field_image_title')]) < 1:
                row[content[0].index('field_image_title')] = name_field.strip()

            if 'field_article_subtitle' in content[0] and \
                    len(row[content[0].index('field_article_subtitle')]) < 1:
                row[content[0].index('field_article_subtitle')] = '-'

            if 'field_brand_subtitle_value' in content[0] and \
                    len(row[content[0].index('field_brand_subtitle_value')]) < 1:
                row[content[0].index('field_brand_subtitle_value')] = '-'

            if 'created' in content[0] and content[0].index('created') in row:
	                timestamp = int(datetime.fromisoformat(row[content[0].index('created')]).timestamp())
        	        row[content[0].index('created')] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            if 'url_alias' in content[0] and \
                    len(row[content[0].index('url_alias')]) > 1:
                row[content[0].index('url_alias')] = '/'+row[content[0].index('url_alias')]

            if 'body_value' in content[0]:
                body_value = row[content[0].index('body_value')]
                links_arr = re.findall(self.pattern, body_value)
                new_str = body_value

                '''
                    Если не прислали картинки, то раскоментировать строки ниже 
                '''

                for link in links_arr:
                    # p = requests.get(link)

                    new_link = '/sites/default/files/migration_images/'+link.split('/')[-1]
                    new_str = new_str.replace(link, new_link)

                    # out = open(f"./migration_image/{link.split('/')[-1]}", "wb")
                    # out.write(p.content)
                    # out.close()

                    row[content[0].index('body_value')] = new_str

            if 'field_cooking_text' in content[0]:
                field_cooking_text = row[content[0].index('field_cooking_text')]
                links_arr = re.findall(self.pattern, ' '.join(field_cooking_text))
                for link in links_arr:
                    field_cooking_text.replace(link, '/sites/default/files/migration_images/'+link.split('/')[-1])

                row[content[0].index('field_cooking_text')] = field_cooking_text

        with open(self.path_to_feeds_dir+self.filename, 'w+') as file:
            file_writer = csv.writer(file, delimiter = ";", lineterminator="\n", quotechar='"')
            for row in content:
                file_writer.writerow(row)


def replace_ending(sentence, old, new):
    # Check if the old string is at the end of the sentence 
    if sentence.endswith(old):
        # Using i as the slicing index, combine the part
        # of the sentence up to the matched string at the 
        # end with the new string
        i = sentence.index(old)
        new_sentence = sentence[0:i] + new
        return new_sentence

    # Return the original sentence if there is no match 
    return sentence

# Тут меняйте/добавляйте названия файлов CSV
csvfiles = {
'TH_content_export_article_2021-08-19.csv': 'node',
'TH_content_export_brand_2021-08-19.csv': 'node',
'TH_content_export_elearn_article_2021-08-19.csv': 'node',
'TH_content_export_names_2021-08-19.csv': 'node',
'TH_content_export_names_origin_2021-08-19.csv': 'taxonomy',
'TH_content_export_product_2021-08-19.csv': 'node',
'TH_content_export_recipe_2021-08-19.csv': 'node',
'TH_content_export_stage_themes_2021-08-19.csv': 'taxonomy',
'TH_content_export_stages_2021-08-19.csv': 'taxonomy',
}

for csvfile, entitytype in csvfiles.items():
	obj1 = ImportTools(
	    filename = csvfile,
	    workdir = './content files/',
	    path_to_feeds_dir = './Result/',
	    delimiter = ';',
	    content_type = entitytype,
	    multilingual = 0,
	    prefix = '_th'
	)
	obj1.generate_new_file()
