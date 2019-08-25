from datetime import datetime

types = {'citizen_id': int,
         'town': str,
         'street': str,
         'building': str,
         'apartment': int,
         'name': str,
         'birth_date': str,
         'gender': str,
         'relatives': list
         }


class ValidateError(Exception):
    pass


class Citizen:
    __slots__ = ['citizen_id', 'town', 'street', 'building', 'apartment', 'name', 'birth_date', 'gender', 'relatives']

    def __setattr__(self, key, value):
        """
        Setting values of the keys (adding validation)
        """
        self.validate(key, value)
        super().__setattr__(key, self.normalize_value(key, value))

    @staticmethod
    def validate(key, value):
        """
        Validation of values
        """
        if not isinstance(value, types[key]):
            raise ValidateError('Wrong type')
        if key != 'relatives' and not value:
            raise ValidateError('Wrong value')
        if types[key] == int and value <= 0:
            raise ValidateError('Wrong value')

    @staticmethod
    def normalize_value(key, value):
        """
        Normalizing value before storing
        """
        if key == 'birth_date':
            try:
                return datetime.strptime(value, '%d.%m.%Y')
            except Exception as e:
                raise ValidateError(e)

        if key == 'gender':
            if value not in ('male', 'female'):
                raise ValidateError('Wrong gender')

        if key == 'relatives':
            for relative in value:
                if not isinstance(relative, int):
                    raise ValidateError('Wrong relatives')
        return value


