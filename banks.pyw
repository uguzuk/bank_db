#!/usr/bin/python
# -*- coding: utf8 -*-

""" Этот модуль парсит страничку с пунктами обмена валют и сливает некоторые данные в БД """

import requests
from lxml import html
import lxml.etree
import MySQLdb
import schedule
import time

class ParseTable():
	global url
	global bank_table, bank_name, bank_buy, bank_sell, bank_time, bank_info, bank_sum

	bank_name, bank_buy, bank_sell = [], [], []
	bank_time, bank_info, bank_sum = [], [], []

	url = "http://quote.rbc.ru/cash/?sortf=BID&sortd=DESC&city=2&currency=321"

	def __init__(self):
		self.request()

	def request(self):
		"""собираем информацию с банков"""
		count = 0

		page = requests.get(url)
		print "request status code: ", page.status_code

		tree = html.fromstring(page.content)

		global bank_quantity
		bank_quantity = tree.xpath("//tbody[@id='tableBody']/tr[position()>0]") # кол-во банков на сайте

		for each in bank_quantity:
			"""пробегаем по всем банкам"""
			count += 1

			name_path = "//tbody[@id='tableBody']/tr[%s]/td[2]/a/text()" % str(count)
			buy_path = "//tbody[@id='tableBody']/tr[%s]/td[4]/a/text()" % str(count)
			sell_path = "//tbody[@id='tableBody']/tr[%s]/td[5]/a/text()" % str(count)
			time_path = "//tbody[@id='tableBody']/tr[%s]/td[8]/text()" % str(count)
			info_path = "//tbody[@id='tableBody']/tr[%s]/td[9]/span[@id='tel']/text()" % str(count)
			sum_path = "//*[@id='tableBody']/tr[%s]/td[7]/text()" % str(count)

			name_tree = tree.xpath(name_path)
			buy_tree = tree.xpath(buy_path)
			sell_tree = tree.xpath(sell_path)
			time_tree = str(tree.xpath(time_path))
			info_tree = tree.xpath(info_path)
			sum_tree = tree.xpath(sum_path)

			if sum_tree != ['1']: #ищем только те банки, которые принимают сумму от 1
				sum_tree.pop()
			

			if sell_tree == []:
				sell_tree.append('0')

			if buy_tree == []:
				buy_tree.append('0')

			for i in sum_tree:
				bank_sum.append(i)

			for i in name_tree:
				bank_name.append(i)

			for i in buy_tree:
				bank_buy.append(i)

			for i in sell_tree:
				bank_sell.append(i)

			for i in range(0, len(time_tree)):
				bank_time.append(time_tree[2:10])

			for i in info_tree:
				bank_info.append(i)





class DataBase():
	def __init__(self):
		conn = MySQLdb.connect(host = 'localhost', user = 'root', passwd = '', db = 'banks', charset = 'utf8')

		# table gbp_rub: bank_name, buy, sell, kom, Sum, Time, Phone

		cursor = conn.cursor()
		try:
			#cursor.execute("""TRUNCATE TABLE bank_table""")
			cursor.execute("""LOAD DATA INFILE "D:/dev/proj/python/bank_db/bank_table.txt" INTO TABLE bank_table""")
			conn.commit()
		except:
			conn.rollback()

		cursor.execute("""SELECT * FROM bank_table;""")

		conn.close()


class SaveData():
	def __init__(self):
		self.writeFile()

	def writeFile(self):
		"""пишем в файл"""
		f = open('./bank_table.txt', 'w')

		for w in range(0, len(bank_sum)):
			f.write(bank_name[w].encode('utf8') + '\t')
			f.write(bank_buy[w] + '\t')
			f.write(bank_sell[w] + '\t')
			f.write(bank_time[w] + '\t')
			f.write(bank_info[w].encode('utf8') + '\n')

		f.write('\n') # пустая строка в таблице

		f.close()

class Main():
	def __init__(self):
		pt = ParseTable()
		sd = SaveData()
		db = DataBase()

def run():
    run = Main()

schedule.every(1).hours.do(run)

while True:
    schedule.run_pending()
    time.sleep(1)