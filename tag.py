from os import walk
import csv
import re

csv.field_size_limit(100000000)

class ExportTools:

    def __init__(self, sourcefile, tagfile, url, source_col_nid = 2, source_col_title = 5, tag_col_title = 1, tag_col_nid = 4, tag_col_url = 5):
        self.sourcefile = sourcefile
        self.tagfile = tagfile
        self.url = url
        self.source_col_nid = source_col_nid
        self.source_col_title = source_col_title
        self.tag_col_title = tag_col_title
        self.tag_col_nid = tag_col_nid
        self.tag_col_url = tag_col_url

    def get_content(self):
        with open(f'{self.filename}') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter = ';')
            content = [row for row in csv_reader]
            return content

    def tagging(self):
        self.filename = self.tagfile
        tagfile = self.get_content()

        self.filename = self.sourcefile
        sourcefile = self.get_content()

        i = 0
        for item_2 in tagfile[1::]:
           for item_1 in sourcefile[1::]:
                if item_1[self.source_col_title].strip() == item_2[self.tag_col_title].strip():
                    id_node = item_1[self.source_col_nid]
                    item_2[self.tag_col_nid] = id_node
                    item_2[self.tag_col_url] = self.url + '/node/' + id_node
                    i += 1
                    print(i)
                    break

        with open(self.tagfile, 'w+') as file:
            file_writer = csv.writer(file, delimiter=";", lineterminator="\n")
            for row in tagfile:
                file_writer.writerow(row)

obj1 = ExportTools('CO_content_export_article_2021-08-19.csv', 'article_tagging.csv', 'https://www.babyandme.nestle.co')
obj1.tagging()
obj1 = ExportTools('CO_content_export_elearn_article_2021-08-19.csv', 'article_tagging.csv', 'https://www.babyandme.nestle.co')
obj1.tagging()
obj1 = ExportTools('CO_content_export_mpi_list_2021-08-19.csv', 'article_tagging.csv', 'https://www.babyandme.nestle.co')
obj1.tagging()

obj1 = ExportTools('CO_content_export_product_2021-08-19.csv', 'product_tagging.csv', 'https://www.babyandme.nestle.co')
obj1.tagging()

obj1 = ExportTools('CO_content_export_recipe_2021-08-19.csv', 'recipe_tagging.csv', 'https://www.babyandme.nestle.co')
obj1.tagging()
