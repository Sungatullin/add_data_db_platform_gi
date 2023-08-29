import pandas as pd
from db import DB, transliteration
import json

fields = "fields.json"
areas = "areas.json"
horizons = "horizons.json"
layers = "layers.json"
ngdus = "ngdus.json"
company_name = "Татнефть"
other_names_ = ['татнефть', 'tatneft']

database = DB()


def add_data():
    # добавление месторождений
    with open(fields, "r", encoding="utf8") as fields_:
        elems: dict = json.load(fields_)
    for row in elems:
        other_names = "{" + f"{','.join(row['other_names'])}" + "}"
        database.add_one_field(kod=row["kod"], field_name=row["field_name"],
                               other_names=other_names, company_name=company_name)

    # добавление площадей
    with open(areas, "r", encoding="utf8") as areas_:
        elems: dict = json.load(areas_)
    for key, values in elems.items():
        for area in values:
            database.add_one_area(field_name=key, area_name=area, company_name=company_name)

    # добавление нгду и цехов
    with open(ngdus, "r", encoding="utf8") as ngdus_:
        elems: dict = json.load(ngdus_)
    for row in elems:
        for field in row["fields"]:
            database.add_ngdu_shop(field_name=field, kod=row["kod"], ngdu_name=row["name"],
                                   company_name=company_name, shops=row["shop"])

    # добавление горизонтов
    with open(horizons, "r", encoding="utf8") as horizons_:
        elems: dict = json.load(horizons_)
    for row in elems:
        other_names = "{" + f"{','.join(row['other_names'])}" + "}"
        database.add_one_horizon(full_name=row["horizon_name"], other_names=other_names,
                                 company_name=company_name, kod=row["kod"])

    # добавление пластов
    with open(layers, "r", encoding="utf8") as layers_:
        elems: dict = json.load(layers_)
    for row in elems:
        other_names = "{" + f"{','.join(row['other_names'])}" + "}"
        database.add_one_layer(full_name=row["layer_name"], other_names=other_names,
                               company_name=company_name, kod=row["kod"])


if __name__ == '__main__':
    database.add_company(company_name=company_name, other_names=other_names_)
    database.link_user_with_company()
    add_data()
