from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
import time
import os
from pathlib import Path

driver = webdriver.Chrome(executable_path="./chromedriver")
driver.get("http://plvr.land.moi.gov.tw/DownloadOpenData")
time.sleep(3)
window_after = driver.window_handles[0]
driver.switch_to.window(window_after)
time.sleep(3)
driver.find_element(By.LINK_TEXT, "非本期下載").click()
time.sleep(3)
driver.find_element(By.ID, "historySeason_id").click()
dropdown_selection = Select(driver.find_element(By.ID, "historySeason_id"))
dropdown_selection.select_by_visible_text(u"108年第2季")
time.sleep(1)
driver.find_element(By.ID, "historySeason_id").click()
time.sleep(3)
driver.find_element(By.ID, "fileFormatId").click()
file_selection = Select(driver.find_element(By.ID, "fileFormatId"))
file_selection.select_by_visible_text("CSV 格式")
time.sleep(1)
driver.find_element(By.ID, "fileFormatId").click()
time.sleep(1)
driver.find_element(By.ID, "downloadBtnId").click()
time.sleep(2)
file_dir =os.path.join(Path.home(), "Downloads/lvr_landcsv.zip")
if os.path.exists(str(file_dir)):
    print('data scraping succed')
else:
    print('No file download')
