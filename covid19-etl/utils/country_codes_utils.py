import csv
import os


COUNTRY_NAME_MAPPING = {
    # <name in summary_location>: <name in CSV data file>
    "Bosnia and Herzegovina": "Bosnia",
    "Burma": "Myanmar",
    "Cabo Verde": "Cape Verde",
    "Congo (Brazzaville)": "Congo - Brazzaville",
    "Congo (Kinshasa)": "Congo - Kinshasa",
    "Cote d'Ivoire": "Côte d’Ivoire",
    "Eswatini": "Swaziland",
    "Holy See": "Vatican City",
    "Korea, South": "South Korea",
    "North Macedonia": "Macedonia",
    "Saint Vincent and the Grenadines": "St. Vincent & Grenadines",
    "Sao Tome and Principe": "São Tomé & Príncipe",
    "United Kingdom": "UK",
}
ISO_CODES_MAPPING = {
    # ISO codes for countries that are not in the CSV file
    "Kosovo": {"iso2": "XK", "iso3": "XKX"},
    "West Bank and Gaza": {"iso2": "PS", "iso3": "PSE"},
    # JHU has data for boats - we want to include them in total counts
    "Diamond Princess": {"iso2": "Diamond Princess", "iso3": "Diamond Princess"},
    "MS Zaandam": {"iso2": "MS Zaandam", "iso3": "MS Zaandam"},
}

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_codes_dictionary():
    with open(os.path.join(CURRENT_DIR, "country_codes.csv")) as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        headers = next(reader)
        i_name = headers.index("CLDR display name")
        i_iso2 = headers.index("ISO3166-1-Alpha-2")
        i_iso3 = headers.index("ISO3166-1-Alpha-3")

        res = {
            row[i_name]: {"iso2": row[i_iso2], "iso3": row[i_iso3]} for row in reader
        }
    return res


def get_codes_for_country_name(codes_dict, country_name):
    stripped_name = (
        country_name.strip("*").replace("Saint", "St.").replace(" and ", " & ")
    )
    data = codes_dict.get(stripped_name)
    if data:
        return data

    mapped_name = COUNTRY_NAME_MAPPING.get(country_name)
    data = codes_dict.get(mapped_name)
    if data:
        return data

    data = ISO_CODES_MAPPING.get(country_name)
    if data:
        return data

    raise Exception('Cannot find ISO codes data for "{}"'.format(country_name))
