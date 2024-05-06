import pandas as pd
import argparse


def process_arguments():
    parser = argparse.ArgumentParser(description='Description of your script')
    parser.add_argument('-i', '--indexmarkercsv', type=str, help='Path to the CSV file')
    parser.add_argument('-d', '--datasetfile', type=str, help='Path to the new strings file')
    parser.add_argument('-n', '--number', type=int, help='An integer argument')
    # 添加其他参数

    args = parser.parse_args()
    return args

def writeNewStringsToCSV(csvFilename, newStringsFilename, n):
    try:
        # 尝试读取原始CSV文件的内容为DataFrame对象
        df = pd.read_csv(csvFilename)
    except pd.errors.EmptyDataError:
        # 如果CSV文件为空，则创建一个空的DataFrame
        df = pd.DataFrame()

    # 读取新字符串文件的第一行
    with open(newStringsFilename, 'r') as newFile:
        line = newFile.readline()
        newStrings = [data.strip() for data in line.split(',') if data.strip()]

    # 查找不存在于原始数据中的新字符串
    missingStrings = [newString for newString in newStrings if newString not in df.values.flatten().tolist()]

    # # 打印不存在于原始数据中的新字符串
    # print("Missing strings:")
    # for string in missingStrings:
    #     print(string)

    if df.empty or len(df.columns) == 0:
        numEmptyCells = 0

        numCols = len(df.columns)
        # 计算需要新增的列数
        numNewCols = (len(missingStrings) - 1) // n + 1

        # 计算需要新增的空白单元格数
        numEmptyCells = numNewCols * n - len(missingStrings)

        # 将新字符串按照每n个字符串的方式写入原始数据的DataFrame
        newCols = [missingStrings[i:i+n] for i in range(0, len(missingStrings), n)]

        # 填充空白单元格
        for i in range(numEmptyCells):
            newCols[-1].append('')

        # 生成新的列名
        newColNames = ['index{}'.format(i+numCols+1) for i in range(numNewCols)]

        # 扩展DataFrame的索引
        df = df.reindex(columns=df.columns.tolist() + newColNames)

        # 将新的列插入到原始数据的DataFrame中
        for i, col in enumerate(newCols):
            df[newColNames[i]] = col
    else:
        numEmptyCells = df.iloc[:, -1].isnull().sum()
    # numEmptyCells = df.iloc[:, -1].isnull().sum()

        fillCount = min(numEmptyCells, len(missingStrings))

        # 使用missStrings的值填入现有df的最后一列的空值
        df.iloc[n - numEmptyCells :n - numEmptyCells + fillCount, -1] = missingStrings[:fillCount]

        # 计算新增数据的行数
        if len(missingStrings) > fillCount: 
            
            # 获取当前的列数
            numCols = len(df.columns)

            # 计算需要新增的列数
            numNewCols = (len(missingStrings) - fillCount - 1) // n + 1

            # 计算需要新增的空白单元格数
            numEmptyCells = numNewCols * n - len(missingStrings) + fillCount

            # 将新字符串按照每n个字符串的方式写入原始数据的DataFrame
            newCols = [missingStrings[i:i+n] for i in range(fillCount, len(missingStrings), n)]

            # 填充空白单元格
            for i in range(numEmptyCells):
                newCols[-1].append('')

            # 生成新的列名
            newColNames = ['index{}'.format(i+numCols+1) for i in range(numNewCols)]

            # 扩展DataFrame的索引
            df = df.reindex(columns=df.columns.tolist() + newColNames)

            # 将新的列插入到原始数据的DataFrame中
            for i, col in enumerate(newCols):
                df[newColNames[i]] = col

    # 将更新后的DataFrame保存为CSV文件
    df.to_csv(csvFilename, index=False)

def main():
    args = process_arguments()
    # csvFilename = "marker_gene.csv"
    # newStringsFilename = "test.csv"
    # n = 8
    csvFilename = args.indexmarkercsv
    newStringsFilename = args.datasetfile
    n = args.number

    writeNewStringsToCSV(csvFilename, newStringsFilename, n)

if __name__ == '__main__':
    main()