import sys
sys.path.append("..")
from settings import *
import pandas as pd
from tkinter import *
import requests
from requests_ntlm import HttpNtlmAuth
from bs4 import BeautifulSoup
import os
import pymysql
from PIL import Image, ImageTk
import time


class Authentication:
	def __init__(self, app):

		self.key = False
		self.__app = app

		self.__url = 'https://wehqweb01.winbond.com.tw//d000/tools/tel/querybyalias.asp'
		self.__headers = {
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}
		self.__alias = os.getlogin().lower()
		self.__payload = {"queryalias": self.__alias}

		self.ui()

	def ui(self):
		self.root = Tk()
		self.root.title("Entry")
		screen_width = self.root.winfo_screenwidth()
		screen_height = self.root.winfo_screenheight()
		width = 200
		height = 90
		x = (screen_width - width) // 2
		y = (screen_height - height) // 2
		self.root.geometry(f"{width}x{height}+{x}+{y}")
		self.root.resizable(False, False)

		login_btn = Button(self.root, text="登陸", font=('Ariel', 25, 'bold'), command=self.__login)
		login_btn.grid(row=0, column=0, sticky="nsew")

		register_btn = Button(self.root, text="註冊", font=('Ariel', 25, 'bold'), command=self.__register)
		register_btn.grid(row=0, column=1, sticky="nsew")

		self.root.grid_columnconfigure(0, weight=1)
		self.root.grid_columnconfigure(1, weight=1)

		self.lb = Label(self.root, text="")
		self.lb.place(x=45, y=70)

		self.root.mainloop()

	def __check_apply_before_signup(self):
		db = pymysql.connect(
			host='10.9.205.78',
			port=3306,
			user='ha33',
			passwd='1234',
			database="wl")
		cur = db.cursor()
		sql = "select 1 from apply where alias='{}'".format(self.__alias)
		cur.execute(sql)
		one_row = cur.fetchone()
		cur.close()
		db.close()
		if one_row is None:
			return True # 沒有申請過
		return False # 已經申請過



	def __check_whitelist_before_signup(self):
		db = pymysql.connect(
			host='10.9.205.78',
			port=3306,
			user='ha33',
			passwd='1234',
			database="wl")
		cur = db.cursor()
		sql = "select 1 from whitelist where alias='{}'".format(self.__alias)
		cur.execute(sql)
		one_row = cur.fetchone()
		cur.close()
		db.close()
		if one_row is None:
			return True  # 沒有在白名單
		return False  # 已經在白名單

	def __register(self):

		if not self.__check_whitelist_before_signup():
			self.lb.config(text="您已經在白名單...", fg="red")
			return
		if not self.__check_apply_before_signup():
			self.lb.config(text="您已經在申請中...", fg="red")
			return
		r = requests.post(self.__url, headers=self.__headers, data=self.__payload, auth=HttpNtlmAuth(USER, PASSWD))
		r.encoding = "utf-8"
		soup = BeautifulSoup(r.text, 'html.parser')
		df = pd.read_html(str(soup.find_all("table")[0]))[0]
		df = df[df[3].apply(lambda x: x.lower()) == self.__alias].reset_index(drop=True)
		df = tuple(df.iloc[0])
		db = pymysql.connect(
			host='10.9.205.78',
			port=3306,
			user='ha33',
			passwd='1234',
			database="wl")
		cur = db.cursor()
		sql = "insert into apply (dept, name, ename, alias, title, extension, phone, position) values (%s, %s, %s, %s, %s, %s, %s, %s)"
		cur.execute(sql, df)
		db.commit()
		self.lb.config(text="您已經送出申請...", fg="blue")
		cur.close()
		db.close()
	def __login(self):
		db = pymysql.connect(
			host='10.9.205.78',
			port=3306,
			user='ha33',
			passwd='1234',
			database="wl")
		cur = db.cursor()
		sql = "select dept from whitelist where alias='{}'".format(self.__alias)
		cur.execute(sql)
		one_row = cur.fetchone()
		cur.close()
		db.close()
		if one_row is None:
			self.key = False # 無法登陸
			self.lb.config(text="無法登陸，請註冊", fg="red")
			return
		self.key = True # 登陸
		self.lb.config(text="登陸成功", fg="red")

		# 連結hist
		db = pymysql.connect(
			host='10.9.205.78',
			port=3306,
			user='ha33',
			passwd='1234',
			database="wl")
		cur = db.cursor()
		sql = "insert into hist (datetime, dept, alias, app) values (%s, %s, %s, %s)"
		self.sec = one_row[0]
		data = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), one_row[0], self.__alias, self.__app)
		cur.execute(sql, data)
		db.commit()
		cur.close()
		db.close()


		# 警語
		window = Toplevel(self.root)
		window.title("登陸成功")
		screen_width = self.root.winfo_screenwidth()
		screen_height = self.root.winfo_screenheight()
		width = 1300
		height = 400
		x = (screen_width - width) // 2
		y = (screen_height - height) // 2
		window.geometry(f"{width}x{height}+{x}+{y}")
		window.resizable(False, False)

		canvas = Canvas(window, width=1300, height=400)
		canvas.pack(fill=BOTH, expand=True)

		image = Image.open(r"\\10.9.205.78\hm10_phm\勿刪\warnings\warning.png")  # 替换为你的图片路径
		self.photo = ImageTk.PhotoImage(image)

		# 在 Canvas 上显示图片
		canvas.create_image(650, 200, image=self.photo)

		window.protocol("WM_DELETE_WINDOW", self.__on_window_close)


	def __on_window_close(self):
		self.root.destroy()

# if __name__ == "__main__":
# 	app = "3in1"
# 	obj = Authentication(app)

