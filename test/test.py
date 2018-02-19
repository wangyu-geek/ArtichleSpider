class Kls(object):
    def __init__(self, data):
        self.data = data
        print('aaa')
    def printd(self):
        print(self.data)
    @staticmethod
    def smethod(*arg):
        print('Static:', arg)
    @classmethod
    def cmethod(*arg):
        print('Class:', arg)