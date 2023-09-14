import psycopg2


# транслитерация латиницы на кириллицу
def transliteration(string_object: str):
    # словарь замен
    letters = {'a': 'а', 'b': 'в', 'c': 'с', 'd': 'д', 'e': 'е', 'f': 'ф', 'g': 'г', 'h': 'н', 'k': 'к', 'l': 'л',
               'm': 'м', 'n': 'н', "o": "о", 'r': 'р', 'p': 'п', 's': 'с', 't': 'т', 'u': 'у', 'v': 'в', 'x': 'х',
               'z': 'з', 'y': 'у', 'j': 'ю', 'iii': '3', 'ii': '2', 'i': '1', ".": "", " ": "", "-": "", '"': "",
               "№": ""}

    # применение словаря замен к названию
    if type(string_object) is str:
        string_object = string_object.lower()
        for letter in letters.keys():
            string_object = string_object.replace(letter, letters[letter])
        return string_object
    return string_object


class DB:
    def __init__(self):
        self.conn = psycopg2.connect(dbname='geo_atlas_dp_bulat', user='postgres',
                                     password='bulat2002', host='localhost')
        self.cur = self.conn.cursor()

    def close_con(self):
        self.cur.close()
        self.conn.close()

    def add_fluids(self):
        fluids = ["вода",  "нефть", "газ"]
        for fluid in fluids:
            self.cur.execute(
                '''INSERT INTO rest_fluid (name) VALUES ({!r}) 
                   on conflict do nothing'''.format(fluid)
            )
            self.conn.commit()

    def add_company(self, company_name, other_names):
        other_names = '{' + f"{','.join(other_names)}" + '}'
        self.cur.execute(
            '''INSERT INTO users_company (other_names, name) VALUES ({!r}, {!r}) 
               on conflict do nothing'''.format(f'{other_names}', company_name)
        )
        self.conn.commit()

    def link_user_with_company(self):
        self.cur.execute(
            f'''SELECT id FROM users_user''')
        user_id = self.cur.fetchone()[0]
        self.cur.execute(
            f'''SELECT id FROM users_company''')
        company_id = self.cur.fetchone()[0]
        print(user_id, company_id)
        self.cur.execute(
            f'''UPDATE users_user SET company_id = {company_id} WHERE id = {user_id}'''
        )
        self.conn.commit()

    def add_one_field(self, field_name, kod,  company_name, other_names):
        self.cur.execute(
            f'''SELECT id FROM users_company WHERE name = '{company_name}' ''')
        company_id = self.cur.fetchone()[0]
        self.cur.execute(
            '''INSERT INTO rest_field (name, code, company_id, other_names) VALUES ({!r}, {}, {}, {!r}) 
               on conflict do nothing'''.format(field_name, kod, company_id, other_names)
        )
        self.conn.commit()

    def add_one_horizon(self, full_name, other_names, company_name, kod):
        self.cur.execute(
            f'''SELECT id FROM users_company WHERE name = '{company_name}' ''')
        company_id = self.cur.fetchone()[0]
        self.cur.execute(
            '''INSERT INTO rest_horizon (full_name, company_id, other_names, code) 
               VALUES ({!r}, {}, {!r}, {}) on conflict do nothing'''.format(
               full_name, company_id, other_names, kod)
        )
        self.conn.commit()

    def add_one_layer(self, full_name, other_names, company_name, kod):
        self.cur.execute(
            f'''SELECT id FROM users_company WHERE name = '{company_name}' ''')
        company_id = self.cur.fetchone()[0]
        try:
            hor_kod = kod // 100
            self.cur.execute(
                f'''SELECT id FROM rest_horizon WHERE code = {hor_kod}''')
            horizon_id = self.cur.fetchone()[0]
            xy = self.cur.execute(
                '''INSERT INTO rest_layer (full_name, company_id, horizon_id, other_names, code)
                   VALUES ({!r}, {}, {}, {!r}, {}) on conflict do nothing'''.format(
                   full_name, company_id, horizon_id, other_names, kod)
            )
            self.conn.commit()
        except Exception as e:
            pass

    def add_one_area(self, field_name: str, area_name: str, company_name):
        self.cur.execute(
            f'''SELECT id FROM users_company WHERE name = '{company_name}' ''')
        company_id = self.cur.fetchone()[0]
        try:
            field_name = transliteration(field_name)
            self.cur.execute(
                f'''SELECT id FROM rest_field WHERE '{field_name}' = Any(other_names)''')
            field_id = self.cur.fetchone()[0]
            other_names = "{" + transliteration(area_name) + "}"
            xy = self.cur.execute(
                '''INSERT INTO rest_area (name, company_id, field_id, other_names)
                    VALUES ({!r}, {}, {}, {!r}) on conflict do nothing'''.format(
                   area_name, company_id, field_id, other_names)
            )
            self.conn.commit()
        except Exception as e:
            pass

    def add_ngdu_shop(self, kod, ngdu_name: str, company_name, shops: list):
        self.cur.execute(
            f'''SELECT id FROM users_company WHERE name = '{company_name}' ''')
        company_id = self.cur.fetchone()[0]
        try:
            self.cur.execute(
                f'''SELECT id FROM rest_ngdu WHERE code = '{kod}' ''')
            ngdu = self.cur.fetchone()
            if ngdu is None:
                other_names_ngdus = "{" + transliteration(ngdu_name) + "}"
                self.cur.execute(
                    '''INSERT INTO rest_ngdu (name, code, company_id, other_names)
                       VALUES ({!r}, {}, {}, {!r}) on conflict do nothing'''.format(
                       ngdu_name, kod, company_id, other_names_ngdus)
                )

            for shop in shops:
                other_names_shops = "{" + transliteration(shop) + "}"
                self.cur.execute(
                    '''INSERT INTO rest_workshop (name, company_id, ngdu_id, other_names)
                       VALUES ({!r}, {}, {}, {!r}) on conflict do nothing'''.format(
                        shop, company_id, ngdu[0], other_names_shops)
                )
            self.conn.commit()

        except:
            pass
