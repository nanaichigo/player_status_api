
class MyException(Exception):
    code = 0
    message = ''
    def __init__(self, code, message):
        self.code = code
        self.message = message

class ServerException(MyException):
    def __init__(self, message):
        super().__init__(500, message)

class InputException(MyException):
    def __init__(self, message):
        super().__init__(400, message)

