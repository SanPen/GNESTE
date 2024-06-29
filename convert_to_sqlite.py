import os
from dataclasses import dataclass
from typing import Dict, List
from uuid import uuid4
import pandas as pd
import sqlite3


@dataclass
class Country:
    name: str
    ISO2: str
    ISO3: str
    continent: str

    def __init__(self, name: str, ISO2: str, ISO3: str, continent: str):
        self.name = name
        self.ISO2 = name if pd.isna(ISO2) else ISO2
        self.ISO3 = name if pd.isna(ISO3) else ISO3
        self.continent = continent

    def key(self):
        return self.ISO3


@dataclass
class Technology:
    name: str
    ID: str
    category: str

    def key(self):
        return self.name


@dataclass
class Variable:
    name: str
    short_name: str
    unit: str

    def key(self):
        return self.name


@dataclass
class Entry:
    technology: Technology | str
    country: Country | str
    variable: Variable | str
    power: float
    energy: float
    source: str
    notes: str
    currency: str
    year: int
    price: float
    _key: str = uuid4().hex

    def key(self):
        return self._key


class DB:
    def __init__(self):
        self.countries: Dict[str, Country] = dict()
        self.technologies: Dict[str, Technology] = dict()
        self.variables: Dict[str, Variable] = dict()
        self.entries: List[Entry] = list()

    def add_country(self, country: Country):
        """

        :param country:
        :return:
        """
        self.countries[country.key()] = country

    def add_technology(self, technology: Technology):
        """

        :param technology:
        :return:
        """
        self.technologies[technology.key()] = technology

    def add_variable(self, variable: Variable):
        """

        :param variable:
        :return:
        """
        self.variables[variable.key()] = variable

    def add_entry(self, entry: Entry):
        """

        :param entry:
        :return:
        """
        self.entries.append(entry)

    def add_dataframe(self, df: pd.DataFrame):
        """
        Add Dataframe to the DB
        :param df: DataFrame
        """
        for i, row in df.iterrows():

            tech = Technology(name=row["Technology"], ID=row["ID"], category=row["Category"])
            country = Country(name=row["Country"], ISO2=row["ISO2"], ISO3=row["ISO3"], continent=row["Continent"])
            var = Variable(name=row["Variable"], short_name=row["Code"], unit=row["Unit"])
            self.add_technology(technology=tech)
            self.add_country(country=country)
            self.add_variable(variable=var)

            # power_str = row["Power"].split(" ")[0].strip() if row["Power"] != "" else None
            # energy_str = row["Energy"].split(" ")[0].strip() if row["Energy"] != "" else None
            # power = float(power_str) if power_str is not None else 0.0
            # energy = float(energy_str) if energy_str is not None else 0.0
            years = [int(x) for x in df.columns.values if isinstance(x, (int, float))]

            for year in years:

                price = row[year]

                if not pd.isna(price):
                    entry = Entry(technology=tech,
                                  country=country,
                                  variable=var,
                                  power=0.0,
                                  energy=0.0,
                                  source=row["Source"],
                                  notes=row["Notes"],
                                  currency=row["Currency"],
                                  year=year,
                                  price=price)

                    self.add_entry(entry=entry)

    def add_file(self, file_name: str):
        """
        Add excel or csv file to the database
        :param file_name:
        :return:
        """
        if os.path.exists(file_name):
            if file_name.endswith(".csv"):
                df = pd.read_csv(file_name, skiprows=1, header=1)
            elif file_name.endswith(".xlsx"):
                df = pd.read_excel(file_name, sheet_name="Data")
            else:
                raise Exception("File type not supported")
            self.add_dataframe(df=df)
        else:
            raise Exception("File not exist")

    def save_sqlite(self, file_name: str):
        """
        Save the database in SQLIte relational format
        :param file_name: something that ends up in .sqlite
        """
        if not file_name.endswith(".sqlite"):
            file_name += '.sqlite'

        country_df = pd.DataFrame.from_dict(self.countries, orient='index')
        vars_df = pd.DataFrame.from_dict(self.variables, orient='index')
        tech_df = pd.DataFrame.from_dict(self.technologies, orient='index')

        entries2 = [Entry(technology=e.technology.key(),
                          country=e.country.key(),
                          variable=e.variable.key(),
                          power=e.power,
                          energy=e.energy,
                          source=e.source,
                          notes=e.notes,
                          currency=e.currency,
                          year=e.year,
                          price=e.price) for e in self.entries]
        entries_df = pd.DataFrame(entries2)

        conn = sqlite3.connect(file_name)
        c = conn.cursor()
        country_df.to_sql('country', conn, index=False)
        vars_df.to_sql('vars', conn, index=False)
        tech_df.to_sql('tech', conn, index=False)
        entries_df.to_sql('entries', conn, index=False)
        conn.close()


if __name__ == '__main__':
    files = [
        "GNESTE_Battery_Storage.xlsx",
        "GNESTE_Coal_Power.xlsx",
        "GNESTE_Gas_Power.xlsx",
        "GNESTE_Hydro_Power.xlsx",
        "GNESTE_Nuclear_Power.xlsx",
        "GNESTE_Solar_Power.xlsx",
        "GNESTE_Wind_Power.xlsx",
    ]

    # files = [  # Excel files are less problematic
    #     "GNESTE_Battery_Storage.csv",
    #     "GNESTE_Coal_Power.csv",
    #     "GNESTE_Gas_Power.csv",
    #     "GNESTE_Hydro_Power.csv",
    #     "GNESTE_Nuclear_Power.csv",
    #     "GNESTE_Solar_Power.csv",
    #     "GNESTE_Wind_Power.csv",
    # ]

    db = DB()

    for f_name in files:
        db.add_file(file_name=f_name)

    db.save_sqlite(file_name="GNESTE.sqlite")
