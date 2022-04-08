csvfile = open('comments.csv', 'r').readlines()
fileinc = 1
for i in range(len(csvfile)):
    if i == 0:
       header = csvfile[0]
    if i % 7000 == 0:
        spl_comments = open('spl_comments' + str(fileinc).zfill(2) + '.csv', 'w+')
        if i > 0:
            spl_comments.writelines(header)
        spl_comments.writelines(csvfile[i:i+7000])
        fileinc += 1