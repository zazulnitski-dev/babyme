import csv
import re
from datetime import datetime
import glob
import os

csv.field_size_limit(100000000)


class ImportTool:

    IMG_PATTERN = '(?P<url>https?://\S+\.(?:jpg|gif|png))'

    def __init__(self, workdir='./',
                 delimiter=';',
                 quotechar='~',
                 difficult_level={},
                 stages_dict={}
                 ):
        self.workdir = workdir
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.difficult_level = difficult_level
        self.stages_dict=stages_dict

    @staticmethod
    def get_content(filename='') -> list:
        try:
            with open(f'{filename}') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=';')
                return [row for row in csv_reader]

        except (FileNotFoundError, IOError):
            print("Wrong file or file path")

        return []

    def writing_to_file(self, content=None, filename='', file_delimiter=";") -> str:
        if content is None:
            content = []

        results_dir = f'{self.workdir}/updated_files'

        if 'updated_files' not in os.listdir(self.workdir):
            os.mkdir(results_dir)

        with open(f'{results_dir}/{filename}', 'w+') as file:
            file_writer = csv.writer(file, delimiter=file_delimiter, lineterminator="\n", quotechar='"')
            for row in content:
                file_writer.writerow(row)

        return f'File {filename} was created'

    def check_content(self):
        images_dict = {}
        taxonomy_dict = {
            'ingredients': {},
            'main_topic': {},
            'allergens': {},
            'stage': {},
            'product_category': {},
            'secondary_topics': {},
            'stage_themes': {},
            'tools': {}
        }
        files_dict = {}
        files_arr = [file.split('/')[-1] for file in glob.glob(f'{self.workdir}/*.csv')]

        'Разбиение файлов по категориям'
        for item in files_arr:
            key = ''

            if item.find('mpi') != -1:
                key = 'mpi_article'
            elif item.find('elearn_article') != -1:
                key = 'elearn_article'
            elif item.find('article') != -1:
                key = 'article'
            elif item.find('recipe') != -1:
                key = 'recipe'
            elif item.find('stages') != -1:
                key = 'stages'
            elif item.find('allergens') != -1:
                key = 'allergens'
            elif item.find('names_origin') != -1:
                key = 'names_origin'
            elif item.find('names') != -1:
                key = 'names'
            elif item.find('product_category') != -1 or item.find('product_formats') != -1:
                key = 'product_category'
            elif item.find('product') != -1:
                key = 'product'
            elif item.find('stage_themes') != -1:
                key = 'stage_themes'
            elif item.find('brand') != -1:
                key = 'brand'
            elif item.find('ingredients') != -1:
                key = 'ingredients'
            elif item.find('meal_type') != -1:
                key = 'meal_type'

            if f'{key}.csv' in files_dict:
                files_dict[f'{key}.csv'].append(item)
            else:
                files_dict[f'{key}.csv'] = [item]

        for key, files in files_dict.items():
            print(key)
            content = []

            for file in files:
                filepath = f'{self.workdir}/{file}'

                if len(content) == 0:
                    content = self.get_content(filepath)
                else:
                    content += self.get_content(filepath)[1::]

            if len(content) == 0:
                print(f'Empty file: {file}')
                continue

            header = content[0]
            new_content = [header]

            is_adimo = False
            adimo_array = [['nid', 'adimo_id']]

            is_pricespider = False
            pricespider_array = [['nid', 'price_spider_id']]

            is_bazaarvoice = False
            bazaarvoice_array = [['nid', 'field_bv_product_id']]

            if 'bazaarvoice_id' in header:
                is_bazaarvoice = True

            if 'adimo' in header:
                is_adimo = True

            if 'pricespider' in header:
                is_pricespider = True

            for row in content[1::]:
                name_field = ''
                row = [item.strip() for item in row]
                prefixes = ['', '_lng'] if 'langcode_lng' in header else ['']

                if 'stages' in header and self.stages_dict:
                    stages_arr = '|'.join(
                        [self.stages_dict[key.strip()] for key in row[content[0].index(f'stages')].split('|') if key.strip() != ''])
                    row[content[0].index(f'stages')] = stages_arr

                for prefix in prefixes:

                    if f'name_field{prefix}' in header:
                        name_field = row[header.index(f'name_field{prefix}')]
                    elif f'title_field{prefix}' in header:
                        name_field = row[header.index(f'title_field{prefix}')].strip()

                    if f'stages{prefix}' in content[0] and \
                            len(row[content[0].index(f'stages{prefix}')]) > 1:
                        stage_arr = [item.strip() for item in row[content[0].index(f'stages{prefix}')].split('|')]
                        row[content[0].index(f'stages{prefix}')] = '|'.join(stage_arr)

                    if f'field_ingredients{prefix}' in content[0] and \
                            len(row[content[0].index(f'field_ingredients{prefix}')]) > 1:
                        ingredients_arr = [item.strip() for item in
                                           row[content[0].index(f'field_ingredients{prefix}')].split('|')]
                        row[content[0].index(f'field_ingredients{prefix}')] = '|'.join(ingredients_arr)

                    if 'created' in content[0] and content[0].index('created') in row:
                        timestamp = int(datetime.fromisoformat(row[content[0].index('created')]).timestamp())
                        row[content[0].index('created')] = datetime.fromtimestamp(timestamp).strftime(
                            '%Y-%m-%d %H:%M:%S')

                    if f'url_alias{prefix}' in content[0] and \
                            len(row[content[0].index(f'url_alias{prefix}')]) > 1:
                        row[content[0].index(f'url_alias{prefix}')] = \
                            '/' + row[content[0].index(f'url_alias{prefix}')].split('/')[-1]

                    if f'field_product_bg_image_alt{prefix}' in content[0] and \
                            len(row[content[0].index(f'field_product_bg_image_alt{prefix}')]) < 1:
                        row[content[0].index(f'field_product_bg_image_alt{prefix}')] = \
                            row[content[0].index(f'field_product_bg_image{prefix}')].split('/')[-1]

                    if f'field_product_bg_image_title{prefix}' in content[0] and \
                            len(row[content[0].index(f'field_product_bg_image_title{prefix}')]) < 1:
                        row[content[0].index(f'field_product_bg_image_title{prefix}')] = \
                            row[content[0].index(f'field_product_bg_image{prefix}')].split('/')[-1]

                    if f'field_images{prefix}' in content[0]:
                        images_url = [item.split('$')[0] for item in
                                      row[content[0].index(f'field_images{prefix}')].split('|')]
                        row[content[0].index(f'field_images{prefix}')] = '|'.join(images_url)

                    if f'field_article_subtitle{prefix}' in content[0] and \
                            len(row[content[0].index(f'field_article_subtitle{prefix}')]) < 1:
                        row[content[0].index(f'field_article_subtitle{prefix}')] = ' '

                        body_value = row[content[0].index(f'body_value{prefix}')]
                        links_arr = re.findall(self.IMG_PATTERN, body_value)
                        new_str = body_value.strip()

                        for link in links_arr:
                            new_link = '/sites/default/files/migration_images/' + link.split('/')[-1]
                            new_str = new_str.replace(link, new_link)
                            row[content[0].index(f'body_value{prefix}')] = new_str

                    if f'field_cooking_text{prefix}' in content[0]:
                        field_cooking_text = row[content[0].index(f'field_cooking_text{prefix}')]
                        links_arr = re.findall(self.IMG_PATTERN, ' '.join(field_cooking_text))
                        for link in links_arr:
                            field_cooking_text.replace(link,
                                                       '/sites/default/files/migration_images/' + link.split('/')[-1])

                        row[content[0].index(f'field_cooking_text{prefix}')] = field_cooking_text

                    if f'field_image_alt{prefix}' in content[0] and \
                            len(row[content[0].index(f'field_image_alt{prefix}')]) < 1:
                        row[content[0].index(f'field_image_alt{prefix}')] = name_field.strip()

                    if f'field_image_title{prefix}' in content[0] and len(
                            row[content[0].index(f'field_image_title{prefix}')]) < 1:
                        row[content[0].index(f'field_image_title{prefix}')] = name_field.strip()

                    if f'field_images_titles{prefix}' in content[0] and len(
                            row[content[0].index(f'field_images_titles{prefix}')]) < 1:

                        item_str = [item.split('/')[-1] for item in
                                    row[content[0].index(f'field_images{prefix}')].split('|')]
                        row[content[0].index(f'field_images_titles{prefix}')] = '|'.join(item_str)

                    if f'field_images_alts{prefix}' in content[0] and len(
                            row[content[0].index(f'field_images_alts{prefix}')]) < 1:

                        item_str = [item.split('/')[-1] for item in row[content[0].index(f'field_images{prefix}')].split('|')]
                        row[content[0].index(f'field_images_alts{prefix}')] = '|'.join(item_str)


                    if f'field_brand_bg_image{prefix}' in content[0] and len(
                            row[content[0].index(f'field_brand_bg_image{prefix}')]) > 0 and \
                            f'field_brand_bg_image_alt{prefix}' in content[0] and \
                            len(row[content[0].index(f'field_brand_bg_image_alt{prefix}')]) < 1:
                        row[content[0].index(f'field_brand_bg_image_alt{prefix}')] = name_field.strip()

                new_content.append(row)

                if 'pricespider' in header:
                    pricespider_index = header.index('pricespider')
                    pricespider_item = row[pricespider_index]

                    if pricespider_item != '':
                        pricespider_array.append([
                            row[header.index('nid')],
                            pricespider_item.strip(),
                        ])

                if 'adimo' in header and is_adimo:
                    adimo_index = header.index('adimo')
                    adimo_item = row[adimo_index]

                    if adimo_item != '':
                        adimo_array.append([
                            row[header.index('nid')],
                            adimo_item.strip(),
                        ])

                if 'bazaarvoice_id' in header and is_bazaarvoice:
                    bazaarvoice_index = header.index('bazaarvoice_id')
                    bazaarvoice_item = row[bazaarvoice_index]

                    if bazaarvoice_item != '':
                        bazaarvoice_array.append([
                            row[header.index('id_d8')],
                            bazaarvoice_item.strip(),
                        ])

            if is_adimo:
                self.writing_to_file(content=adimo_array, filename='adimo.csv', file_delimiter=',')

            if is_pricespider:
                self.writing_to_file(content=pricespider_array, filename='price_spider.csv', file_delimiter=',')

            if is_pricespider:
                self.writing_to_file(content=pricespider_array, filename='bazaarvoice_ids.csv', file_delimiter=';')

            print(self.writing_to_file(content=new_content, filename=key))


tool = ImportTool(
    workdir='/home/asus/Загрузки/gr',
)
tool.check_content()
