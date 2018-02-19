import pickle

class Zhihu:
    cookie_dict = [
        '_xsrf',
        '_zap',
        'aliyungf_tc',
        'capsion_ticket',
        'd_c0',
        'q_c1',
        'z_c0',
    ]

    @classmethod
    def set_cookie(cls, cookies):
        for cookie in cookies:
            f = open("../zhihu/" + cookie["name"], 'wb')
            pickle.dump(cookie, f)
            f.close()

    @classmethod
    def get_cookie(cls):
        cookie_dict = {}
        for cookie in cls.cookie_dict:
            with open("H:/pythonlearn/ArticleSpider/cookies/zhihu/" + cookie, 'rb') as f:
                temp = pickle.load(f)
                cookie_dict[cookie] = temp
        return cookie_dict
