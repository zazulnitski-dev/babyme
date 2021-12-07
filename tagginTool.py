import csv

csv.field_size_limit(100000000)

WEEKS = {
    'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7',
    'P8', 'P9', 'P10', 'P11', 'P12', 'P13', 'P14',
    'P15', 'P16', 'P17', 'P18', 'P19', 'P20', 'P21',
    'P22', 'P23', 'P24', 'P25', 'P26', 'P27', 'P28',
    'P29', 'P30', 'P31', 'P32', 'P33', 'P34', 'P35',
    'P36', 'P37', 'P38', 'P39', 'P40', 'B1', 'B2', 'B3',
    'B4', 'B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'B11', 'B12',
    'B13', 'B14', 'B15', 'B16', 'B17', 'B18', 'B19', 'B20',
    'B21', 'B22', 'B23', 'B24', 'B25', 'B26', 'B27', 'B28',
    'B29', 'B30', 'B31', 'B32', 'B33', 'B34', 'B35', 'B36',
    'B37', 'B38', 'B39', 'B40', 'B41', 'B42', 'B43', 'B44',
    'B45', 'B46', 'B47', 'B48', 'B49', 'B50', 'B51', 'B52',
    'B53', 'B54', 'B55', 'B56', 'B57', 'B58', 'B59', 'B60',
    'B61', 'B62', 'B63', 'B64', 'B65', 'B66', 'B67', 'B68',
    'B69', 'B70', 'B71', 'B72', 'B73', 'B74', 'B75', 'B76',
    'B77', 'B78', 'B79', 'B80', 'B81', 'B82', 'B83', 'B84',
    'B85', 'B86', 'B87', 'B88', 'B89', 'B90', 'B91', 'B92',
    'B93', 'B94', 'B95', 'B96', 'B97', 'B98', 'B99', 'B100',
    'B101', 'B102', 'B103', 'B104', 'B105',
}


def tagging_file(file='', workdir='./', delimiter=';', quotechar='\"'):
    content = []
    with open(f'{workdir}/{file}') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        content = [row for row in csv_reader]

        stages_uniq_array = {}
        stages_theme_uniq_array = {}
        bugs_report = []

        weeks_index = content[0].index('weeks') if 'weeks' in content[0] else -1
        stage_index = content[0].index('stage') if 'stage' in content[0] else -1
        stage_theme_index = content[0].index('stage_themes') if 'stage_themes' in content[0] else -1
        main_topic_index = content[0].index('main_topic') if 'stage_themes' in content[0] else -1

        if 'servings' in content[0]:
            content[0][content[0].index('servings')] = 'serving'

        if 'id' in content[0]:
            content[0][content[0].index('id')] = 'id_d8'

        def change_delimiter(index, row):
            if row.find(',')  != -1 or row.find(';') != -1 or row.find('.') != -1:

                if row.find(',') != -1:
                    row = row.replace(',', '|')

                if row.find(';') != -1:
                    row = row.replace(';', '|')

                if row.find('.') != -1:
                    row = row.replace('.', '|')

                bugs_report.append(f'On line {index} delimiter problem')

                row = '|'.join([item.strip() for item in row.split('|')])
                return row
            return row

        for index, row in enumerate(content[1::]):
            if index != 0:

                if main_topic_index != 1:
                    row[main_topic_index] = change_delimiter(index, row[main_topic_index])

                if weeks_index != -1:
                    weeks_item = change_delimiter(index, row[weeks_index])
                    weeks_arr = [item for item in weeks_item.split('|') if item not in WEEKS and item != '']
                    if len(weeks_arr) > 0:
                        row[weeks_index] = '|'.join([item for item in weeks_item.split('|') if item not in weeks_arr])
                        bugs_report.append('New element(s): [' + ', '.join(weeks_arr) + f'] on line {index}')
                    else:
                        row[weeks_index] = weeks_item

                if stage_index != -1:
                    row[stage_index] = change_delimiter(index, row[stage_index])
                    stages_arr = row[stage_index].split('|')

                    for i in stages_arr:
                        if i not in stages_uniq_array:
                            stages_uniq_array[i.lower()] = i
                        else:
                            if stages_uniq_array[i.lower()] != i:
                                stages_arr.remove(i)
                                stages_arr.append(stages_uniq_array[i.lower()])

                    row[stage_index] = '|'.join(stages_arr)

                if stage_theme_index != -1:
                    row[stage_theme_index] = change_delimiter(index, row[stage_theme_index])
                    stages_theme_arr = row[stage_theme_index].split('|')

                    for i in stages_theme_arr:
                        if i not in stages_theme_uniq_array:
                            stages_theme_uniq_array[i.lower()] = i
                        else:
                            if stages_theme_uniq_array[i.lower()] != i:
                                stages_theme_arr.remove(i)
                                stages_theme_arr.append(stages_theme_uniq_array[i.lower()])

                    row[stage_theme_index] =  '|'.join(stages_theme_arr)

        with open(f'{workdir}/NEW_{file}', 'w+') as file:
            file_writer = csv.writer(file, delimiter=";", lineterminator="\n", quotechar='"')
            for row in content:
                file_writer.writerow(row)


        report = '\t'.join(bugs_report)
        print(report)

tagging_file('tagging_article.csv', '/home/zazulnitski/Загрузки')
# tagging_file('tagging_recipe.csv', '/home/zazulnitski/Загрузки')
# tagging_file('tagging_product.csv', '/home/zazulnitski/Загрузки')
