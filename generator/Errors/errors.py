class LangaugeSupportError(ValueError):
    def __init__(self,msg='Only English and Korean are supported'):
        super().__init__(msg)

class GradeError(ValueError):
    def __init__(self,msg='Check whether you enter the cocrect grade.'):
        super().__init__(msg)

class InputDataError(TypeError):
    def __init__(self,msg='Check your input type.') -> None:
        super().__init__(msg)


class DateTypeError(ValueError):
    def __init__(self,msg='The date format is not compatible to the requirement.'):
        super().__init__(msg)

class SubjectCodeError(ValueError):
    def __init__(self,msg='Subject code is not valid'):
        super().__init__(msg)
        
        
class AgeCodeError(ValueError):
    def __init__(self,msg='Age is out of the scope'):
        super().__init__(msg)
