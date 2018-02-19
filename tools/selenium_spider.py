from selenium import webdriver

# browser = webdriver.Chrome(executable_path="D:\software\download\chromedriver_win32\chromedriver.exe")

# 知乎
# browser.get("https://www.zhihu.com/signup?next=%2F")
# browser.find_element_by_xpath("//*[@id='root']/div/main/div/div/div/div[2]/div[2]/span").click()
# browser.find_element_by_css_selector(".SignContainer-content input[name='username']").send_keys("15075652703")
# browser.find_element_by_css_selector(".SignContainer-content input[name='password']").send_keys("7452525.")
# browser.find_element_by_class_name(".SignContainer-content .SignFlow-submitButton").click()

# 微博
# browser.get("https://weibo.com/")
# import time
# time.sleep(15)
# browser.find_element_by_css_selector(".W_login_form #loginname").send_keys("15075652703")
# browser.find_element_by_css_selector(".W_login_form input[node-type='password']").send_keys("wangyu2525.")
# browser.find_element_by_css_selector(".info_list.login_btn .W_btn_a").click()

# chrome_opt = webdriver.ChromeOptions()
# prefs = {"profile.managed_default_content_settings.images": 2}
# chrome_opt.add_experimental_option("prefs", prefs)
# browser = webdriver.Chrome(executable_path="D:\software\download\chromedriver_win32\chromedriver.exe", chrome_options=chrome_opt)
# browser.get("https://www.taobao.com")

browser = webdriver.PhantomJS(executable_path="H:/pythonlearn/phantomjs/bin/phantomjs.exe")
browser.get("https://item.taobao.com/item.htm?spm=a21bo.2017.201867-rmds-0.3.5af911d9QBGI8B&scm=1007.12807.84406.100200300000004&id=557798281418&pvid=7e313238-6643-4ed4-926a-438b3f7ff861")
print(browser.page_source)
browser.quit()