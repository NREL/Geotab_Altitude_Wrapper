import polars as pl
import os.path
import json

pl.Config.set_tbl_cols(15)
pl.Config.set_tbl_rows(10)
pl.Config.set_tbl_width_chars(200)
pl.Config.set_fmt_str_lengths(200)

db = 'example_db'

with open(os.path.dirname(__file__) + '/../passwords.txt', 'r') as handle:
  accounts = json.load(handle)

projects_folder = "C:/Projects/"
data_folder = "C:/Data/"
in_path = data_folder + "In/"
out_path = data_folder + "Out/"