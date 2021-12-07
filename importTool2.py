from os import walk
import csv
import re
from datetime import datetime
import requests
import collections

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

    def __init__(self, filename = '', workdir = './', path_to_feeds_dir = './', delimiter = ';',
                 quotechar = '~',
                 params = [],
                 langcodes = (''),
                 real_langcodes = ('pt-pt')
                 ):
        self.filename = filename
        self.workdir = workdir
        self.path_to_feeds_dir = path_to_feeds_dir
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.params = params
        self.langcodes = langcodes
        self.real_langcodes = real_langcodes
        self.pattern = '(?P<url>https?://\S+\.(?:jpg|gif|png))'


    def get_content(self, filename):
        with open(filename) as csvfile:

            csv_reader = csv.reader(csvfile, delimiter = self.delimiter, quotechar = self.quotechar)
            content = [row for row in csv_reader]
            return content

    def generate_new_file(self):
        name_field = ''
        header_dict = {}
        content = self.get_content(f'{self.workdir}/{self.filename}')
        new_content = []

        new_content.append(content[0])
        print(self.filename)
        term_names = []
        for row in content[1::]:

            for lc_index, langcode in enumerate(self.langcodes):

                new_row = [item.strip() for item in row]
                row = new_row

                if self.params[0] == 'taxonomy':
                    name_field = row[new_content[0].index(f'name_field{langcode}')]
                    term_names.append(name_field)
                elif self.params[0] == 'node':
                    name_field = row[new_content[0].index(f'title_field{langcode}')].strip()
                    row[content[0].index(f'title_field{langcode}')] = name_field

#                if f'stage_weeks{langcode}' in content[0] and \
#                        len(row[content[0].index(f'stage_weeks{langcode}')]) > 0:
#                    weeks_arr = row[content[0].index(f'stage_weeks{langcode}')].split('|')
#                    new_str = [self.weeks[item] for item in weeks_arr if item in self.weeks]
#                    row[content[0].index(f'stage_weeks{langcode}')] = '|'.join(new_str)

                if f'stages{langcode}' in content[0] and \
                        len(row[content[0].index(f'stages{langcode}')]) > 1:
                    stage_arr = [item.strip() for item in row[content[0].index(f'stages{langcode}')].split('|')]
                    row[content[0].index(f'stages{langcode}')] = '|'.join(stage_arr)

                if f'field_ingredients{langcode}' in content[0] and \
                        len(row[content[0].index(f'field_ingredients{langcode}')]) > 1:
                    ingredients_arr = [item.strip() for item in row[content[0].index(f'field_ingredients{langcode}')].split('|')]
                    row[content[0].index(f'field_ingredients{langcode}')] = '|'.join(ingredients_arr)

                if f'field_image_alt{langcode}' in content[0] and \
                        len(row[content[0].index(f'field_image_alt{langcode}')]) < 1:
                    row[content[0].index(f'field_image_alt{langcode}')] = name_field.strip()

                if f'field_image_alt{langcode}' in content[0] and len(row[content[0].index(f'field_image_alt{langcode}')]) < 1:
                    row[content[0].index(f'field_image_alt{langcode}')] = name_field.strip()

                if f'field_image_title{langcode}' in content[0] and len(row[content[0].index(f'field_image_title{langcode}')]) < 1:
                    row[content[0].index(f'field_image_title{langcode}')] = name_field.strip()

                if f'field_brand_bg_image{langcode}' in content[0] and len(row[content[0].index(f'field_brand_bg_image{langcode}')]) > 0 and \
                        f'field_brand_bg_image_alt{langcode}' in content[0] and len(row[content[0].index(f'field_brand_bg_image_alt{langcode}')]) < 1:
                    row[content[0].index(f'field_brand_bg_image_alt{langcode}')] = name_field.strip()

                if 'created' in content[0] and content[0].index('created') in row:
                    timestamp = int(datetime.fromisoformat(row[content[0].index('created')]).timestamp())
                    row[content[0].index('created')] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


                if f'names_gender{langcode}' in content[0] and \
                        len(row[content[0].index(f'names_gender{langcode}')]) > 1:
                    if row[content[0].index(f'names_gender{langcode}')] == 'Niño':
                        row[content[0].index(f'names_gender{langcode}')] = 'boy'
                    elif row[content[0].index(f'names_gender{langcode}')] == 'Niña':
                        row[content[0].index(f'names_gender{langcode}')] = 'girl'

 
                if f'url_alias{langcode}' in content[0] and \
                        len(row[content[0].index(f'url_alias{langcode}')]) > 1:
                    row[content[0].index(f'url_alias{langcode}')] = '/' + row[content[0].index(f'url_alias{langcode}')]

                if f'field_product_bg_image_alt{langcode}' in content[0] and \
                        len(row[content[0].index(f'field_product_bg_image_alt{langcode}')]) < 1:
                    row[content[0].index(f'field_product_bg_image_alt{langcode}')] = \
                        row[content[0].index(f'field_product_bg_image{langcode}')].split('/')[-1]
                if f'field_product_bg_image_title{langcode}' in content[0] and \
                        len(row[content[0].index(f'field_product_bg_image_title{langcode}')]) < 1:
                    row[content[0].index(f'field_product_bg_image_title{langcode}')] = \
                        row[content[0].index(f'field_product_bg_image{langcode}')].split('/')[-1]

                if f'field_images{langcode}' in content[0]:
                    images_url = [item.split('$')[0] for item in row[content[0].index(f'field_images{langcode}')].split('|')]
                    row[content[0].index(f'field_images{langcode}')] = '|'.join(images_url)

                    
                if f'field_article_subtitle{langcode}' in content[0] and \
                        len(row[content[0].index(f'field_article_subtitle{langcode}')]) < 1:
                    row[content[0].index(f'field_article_subtitle{langcode}')] = ' '

                if f'body_value{langcode}' in content[0]:
                    body_value = row[content[0].index(f'body_value{langcode}')]
                    links_arr = re.findall(self.pattern, body_value)
                    new_str = body_value.strip()
#
#                    if (not new_str == 0):
#                        new_str = ' '

                    '''
                        Если не прислали картинки, idxто раскоментировать строки ниже
                    '''

                    for link in links_arr:
                        # p = requests.get(link)

                        new_link = '/sites/default/files/migration_images/' + link.split('/')[-1]
                        new_str = new_str.replace(link, new_link)

                        # out = open(f"./migration_image/{link.split('/')[-1]}", "wb")
                        # out.write(p.content)
                        # out.close()

                        row[content[0].index(f'body_value{langcode}')] = new_str

                if f'field_cooking_text{langcode}' in content[0]:
                    field_cooking_text = row[content[0].index(f'field_cooking_text{langcode}')]
                    links_arr = re.findall(self.pattern, ' '.join(field_cooking_text))
                    for link in links_arr:
                        field_cooking_text.replace(link, '/sites/default/files/migration_images/' + link.split('/')[-1])

                    row[content[0].index(f'field_cooking_text{langcode}')] = field_cooking_text

                if f'langcode{langcode}' in content[0] and len(row[content[0].index(f'langcode{langcode}')]) > 1:
                    row[content[0].index(f'langcode{langcode}')] = self.real_langcodes[lc_index]

            new_content.append(row)

            with open(f'{self.path_to_feeds_dir}/' + params[1], 'w+') as file:
                file_writer = csv.writer(file, delimiter=";", lineterminator="\n", quotechar='"')
                for row in new_content:
                    file_writer.writerow(row)


        if self.params[0] == 'taxonomy':
             duplicates = ([item for item, count in collections.Counter(term_names).items() if count > 1])
             if (len(duplicates)):
                  print('Druplicates:', duplicates)



csvfiles = {
    'CL_content_export_article_2021-10-25.csv':            ['node', 'article.csv'],
    'CL_content_export_brand_2021-10-25.csv':              ['node', 'brand.csv'],
    'CL_content_export_elearn_article_2021-10-25.csv':     ['node', 'article2.csv'],
    'CL_content_export_mpi_list_2021-10-25.csv':           ['node', 'article3.csv'],
    'CL_content_export_names_2021-10-25.csv':              ['node', 'names.csv'],
    'CL_content_export_names_origin_2021-10-25.csv':       ['taxonomy', 'names_origin.csv'],
    'CL_content_export_product_2021-10-31.csv':            ['node', 'product.csv'],
    'CL_content_export_recipe_2021-10-25.csv':             ['node', 'recipe.csv'],
    'CL_content_export_stage_themes_2021-10-25.csv':       ['taxonomy', 'stage_themes.csv'],
    'CL_content_export_stages_2021-10-25.csv':             ['taxonomy', 'stages.csv']
}


for csvfile, params in csvfiles.items():
    obj1 = ImportTools(
        filename=csvfile,
        workdir='./prepared',
        path_to_feeds_dir='/home/nikit/Develop/babyme/chile/site/web/sites/default/files/migrations/',
        delimiter = ';',
        quotechar = '"', 
        params = params,
        langcodes = [''],
        real_langcodes = ['es']
    )
    obj1.generate_new_file()
