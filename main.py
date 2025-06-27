from module import *
from entry import Authentication
app = "Multi_Prog"
e = Authentication(app)

"""
===================================
 輸入 ECR, 撈資料時間, 訓練時間
===================================
"""

equipment = "WKSNSM01"
chamber = "WKSNSM01_CP01"
# recipe = ["MainRecipe/1_Product/D2Y0BSIGE_3_%", "MainRecipe/1_Product/D2Y1BSIGE_0M_B"]
recipe = ["PSP30S_SCL30S_I"]

starttime = "2024-06-01"
endtime = "2024-06-30"

training_starttime = "2024-06-01 00:00:00"
training_endtime = "2024-07-01 00:00:00"


p = Program(equipment, chamber, recipe, starttime, endtime, e)


"""
===================================
 下載資料
===================================
"""

p.query()


"""
===================================
 過濾 recipe step
===================================
"""

p.show_all_recipe_steps()

recipe_step = ['ARC', 'SOC1', 'SOC2', 'ME', 'STRIP', 'TRAN']
p.use_filtered_recipe_steps(recipe_step)


"""
===================================
 使用 all recipe step
===================================
"""

p.use_all_recipe_steps()


"""
===================================
 輸出 or 載入 raw data
===================================
"""

p.output_raw_data()
p.input_raw_data()

"""
===================================
 修改子系統分類
===================================
"""

p.output_category()
p.input_category()


"""
===================================
 進行子系統分析
===================================
"""

p.run(training_starttime, training_endtime)
