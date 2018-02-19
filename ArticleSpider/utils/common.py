import hashlib
import re


def get_md5(url):
    # Python 中的编码是 unicode，需要转成 utf8编码，才能被 hashlib 接受
    if isinstance(url, str):
        url = url.encode("UTF-8")
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()


def get_nums(value):
    """
    提取数字
    :param value:
    :return:
    """
    regex_str = ".*?(\d+).*"
    match = re.match(regex_str, value)
    if match:
        nums = int(match.group(1))
    else:
        nums = 0
    return nums

if __name__ == "__main__":
    print(get_md5("http://blog.jobbole.com/53/"))