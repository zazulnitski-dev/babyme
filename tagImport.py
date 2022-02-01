
import os
import glob

csv.field_size_limit(100000000)


class TaggingImport:
    """

    """

    def __init__(self, workdir='./', file_delimiter=';', difficulty_level=None, langcodes=None, development_mode=False):
        if langcodes is None:
            langcodes = []
        if difficulty_level is None:
            difficulty_level = {}
        self.workdir = workdir
        self.delimiter = file_delimiter
        self.difficultyLevel = difficulty_level
        self.langcodes = langcodes
        self.devMode = development_mode

        self.taxonomy_dicts = {
            'ingredients': {},
            'main_topic': {},
            'allergens': {},
            'stage': {},
            'product_category': {},
            'secondary_topics': {},
            'stage_themes': {},
            'tools': {}
        }

    def getContent(self, filename='') -> list:
        try:
            with open(f'{filename}') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=';')
                return [row for row in csv_reader]

        except (FileNotFoundError, IOError):
            print("Wrong file or file path")

        return []

    def writing_to_file(self, content=None, filename='tagging.csv', file_delimiter=";") -> str:
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

    def tax_validation(self, string='',
                       character_array=None,
                       substring='|',
                       dict_key='') -> str:
        new_str = ''
        tax_arr = []

        if character_array is None:
            character_array = []

        for character in character_array:

            if string.find(character) != -1:
                new_str = string.replace(character, substring)

        if len(new_str) == 0:
            new_str = string.strip()

        if dict_key != '' and len(string) != 0:
            item_arr = new_str.split(substring) if (new_str.find(substring) != -1) else [new_str.strip()]
            for tax_item in item_arr:

                if tax_item.lower() not in self.taxonomy_dicts[dict_key]:
                    self.taxonomy_dicts[dict_key][tax_item.lower()] = tax_item.strip()

                if tax_item.lower() in self.taxonomy_dicts[dict_key]:
                    tax_arr.append(self.taxonomy_dicts[dict_key][tax_item.lower()])

            new_str = '|'.join(tax_arr)

        return new_str

    "Checking the file for correctness"

    def file_validation(self) -> str:
        files_arr = [file.split('/')[-1] for file in glob.glob(f'{self.workdir}/*.csv')]

        for filename in files_arr:
            content = self.getContent(self.workdir + '/' + filename)

            if not self.devMode:
                print(f'\n{filename} opened\n')

            if not content:
                return 'Empty content'

            rows_count = len(content) - 1
            rows_processed = 0

            file_header = content[0]
            new_content = [file_header]

            is_adimo = False
            adimo_array = [['nid', 'adimo_id']]

            is_pricespider = False
            pricespider_array = [['nid','price_spider_id']]

            is_bazaarvoice = False
            bazaarvoice_array = [['nid', 'field_bv_product_id']]

            if 'bazaarvoice_id' in file_header:
                is_bazaarvoice = True

            if 'adimo' in file_header:
                is_adimo = True

            if 'pricespider' in file_header:
                is_pricespider = True

            for row in content[1::]:
                rows_processed += 1
                new_row = [item.strip() for item in row]

                if 'id' in file_header:
                    new_content[0][file_header.index('id')] = 'id_d8'

                if 'servings' in file_header:
                    new_content[0][file_header.index('servings')] = 'serving'

                if 'allergens' in file_header:
                    allergens_index = file_header.index('allergens')

                    new_row[allergens_index] = self.tax_validation(
                        string=new_row[allergens_index],
                        character_array=None,
                        dict_key='allergens'
                    )

                if 'main_topic' in file_header:
                    main_topic_index = file_header.index('main_topic')

                    new_row[main_topic_index] = self.tax_validation(
                        string=new_row[main_topic_index],
                        character_array=None,
                        dict_key='main_topic'
                    )

                if 'stage' in file_header:
                    stage_special_characters = [' ,', ', ', ',', ')', '(', '/']
                    stage_index = file_header.index('stage')

                    new_row[stage_index] = self.tax_validation(
                        string=new_row[stage_index],
                        character_array=stage_special_characters,
                        dict_key='stage'
                    )

                if 'stage_themes' in file_header:
                    stages_theme_special_characters = [' ,', ', ', ',', ')', '(', '/', ' |', ' | ', '| ']
                    stages_theme_index = file_header.index('stage_themes')

                    new_row[stages_theme_index] = self.tax_validation(
                        string=new_row[stages_theme_index],
                        character_array=stages_theme_special_characters,
                        dict_key='stage_themes'
                    )

                if 'tools' in file_header:
                    tools_special_characters = [' ,', ', ', ',', ')', '(', '/', ' |', ' | ', '| ']
                    tools_index = file_header.index('tools')

                    new_row[tools_index] = self.tax_validation(
                        string=new_row[tools_index],
                        character_array=tools_special_characters,
                        dict_key='tools'
                    )

                if 'weeks' in file_header:
                    weeks_special_characters = [' ,', ', ', ',', ')', '(', '/', ' |', ' | ', '| ']
                    weeks_index = file_header.index('weeks')

                    new_row[weeks_index] = self.tax_validation(
                        string=new_row[weeks_index],
                        character_array=weeks_special_characters,
                        dict_key=''
                    )

                if 'ingredients' in file_header:
                    ingredients_special_characters = [' |', ' | ', '| ']
                    ingredients_index = file_header.index('ingredients')

                    ingredients_str = self.tax_validation(
                        string=new_row[ingredients_index],
                        character_array=ingredients_special_characters,
                        dict_key=''
                    )


                    if ingredients_str.count('(') != ingredients_str.count(')') or \
                            (ingredients_str.count('(') == ingredients_str.count(')') != 0):

                       print(new_row[0])
                       print(new_row[ingredients_index])

                    else:
                        ingredients_arr = ingredients_str.split('|')
                        for ingredient in ingredients_arr:
                            if ingredient.find('(') != -1 or ingredient.find(')') != -1 \
                                    or ingredient.find('-') != -1 or ingredient.find('.') != -1:

                                if ingredient.lower().strip() not in self.taxonomy_dicts['ingredients']:
                                    ingredient = ingredient.strip()

                            self.taxonomy_dicts['ingredients'][ingredient.lower()] = ingredient.strip()

                    new_row[ingredients_index] = '|'.join(ingredients_arr)

                if 'product_category' in file_header:
                    product_category_special_characters = [' ,', ', ', ',', ')', '(', '/', ' |', ' | ', '| ']
                    product_category_index = file_header.index('product_category')

                    new_row[product_category_index] = self.tax_validation(
                        string=new_row[product_category_index],
                        character_array=product_category_special_characters,
                        dict_key='product_category'
                    )

                if 'difficulty_level' in file_header:
                    difficulty_level_index = file_header.index('difficulty_level')
                    difficulty_level_item = new_row[difficulty_level_index]

                    if difficulty_level_item in self.difficultyLevel:
                        new_row[difficulty_level_index] = self.difficultyLevel[difficulty_level_item]

                if 'meal_type' in file_header:
                    meal_type_special_characters = [' ,', ', ', ',', ')', '(', '/', ' |', ' | ', '| ']
                    meal_type_index = file_header.index('meal_type')

                    new_row[meal_type_index] = self.tax_validation(
                        string=new_row[meal_type_index],
                        character_array=meal_type_special_characters,
                        dict_key='product_category'
                    )

                if 'secondary_topics' in file_header:
                    secondary_topics_special_characters = [' ,', ', ', ',', ')', '(', '/', ' |', ' | ', '| ']
                    secondary_topics_index = file_header.index('secondary_topics')

                    new_row[secondary_topics_index] = self.tax_validation(
                        string=new_row[secondary_topics_index],
                        character_array=secondary_topics_special_characters,
                        dict_key='secondary_topics'
                    )

                new_content.append(new_row)

                if 'pricespider' in file_header:
                    pricespider_index = file_header.index('pricespider')
                    pricespider_item = new_row[pricespider_index]

                    if pricespider_item != '':
                        pricespider_array.append([
                            new_row[file_header.index('id_d8')],
                            pricespider_item.strip(),
                        ])

                if 'adimo' in file_header and is_adimo:
                    adimo_index = file_header.index('adimo')
                    adimo_item = new_row[adimo_index]

                    if adimo_item != '':
                        adimo_array.append([
                            new_row[file_header.index('id_d8')],
                            adimo_item.strip(),
                        ])

                if 'bazaarvoice_id' in file_header and is_bazaarvoice:
                    bazaarvoice_index = file_header.index('bazaarvoice_id')
                    bazaarvoice_item = new_row[bazaarvoice_index]

                    if bazaarvoice_item != '':
                        bazaarvoice_array.append([
                            new_row[file_header.index('id_d8')],
                            bazaarvoice_item.strip(),
                        ])

                if not self.devMode:
                    print("Lines processed: {0} out of {1}".format(rows_processed, rows_count))

            if self.devMode:
                for taxonomy_key in self.taxonomy_dicts:
                    print(f'key:{taxonomy_key}\n')

                    for item in sorted(self.taxonomy_dicts[taxonomy_key]):
                        print(f'  {self.taxonomy_dicts[taxonomy_key][item]}\t')

            if is_adimo:
                self.writing_to_file(content=adimo_array, filename='adimo.csv', file_delimiter=',')

            if is_pricespider:
                self.writing_to_file(content=pricespider_array, filename='price_spider.csv', file_delimiter=',')


            if is_pricespider:
                self.writing_to_file(content=pricespider_array, filename='bazaarvoice_ids.csv', file_delimiter=';')

            print(self.writing_to_file(new_content, filename))

            if not self.devMode:
                print(f'{filename} was closed')

tool = TaggingImport(
    workdir='',
    difficulty_level={
        'easy': 1,
        'normal': 2,
        'hard': 3
    },
    langcodes=['en'],
    development_mode=True
)
