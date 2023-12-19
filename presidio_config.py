import pandas as pd
from unidecode import unidecode
import zipfile
import glob
import re
from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
from presidio_anonymizer import AnonymizerEngine

def process_name_txt_files(folder_path, name_list, file_extension='*.txt', top_n=1000):
    """
    Processes .txt files to append names.

    Parameters:
        folder_path (str): Path to the folder.
        name_list (list): List where the processed names are added.
        file_extension (str): File extension to filter files, default is '*.txt'.
        top_n (int): Only rows where the 'Number' column is larger than top_n are processed.

    Returns:
        list: The updated name_list with names extracted from .txt files.
    """
    for file in glob.glob(f"{folder_path}/{file_extension}"):
        data = pd.read_csv(file, header=None, names=["Name", "Gender", "Number"])
        data = data[data["Number"] > top_n]
        name_list.extend(data["Name"])
    
    return name_list

def process_name_csv_files(csv_folder_path, name_list, top_n=1000):
    """
    Processes .csv files to append names.

    Parameters:
        csv_folder_path (str): Path to the folder containing .csv files.
        name_list(list): List where the processed names are added.
        top_n (int): Only rows where the number is larger than top_n are processed.

    Returns:
        list: The updated name_list with names extracted from .csv files.
    """
    for file in glob.glob(f"{csv_folder_path}/*.csv"):
        data = pd.read_csv(file, header=None, skiprows=1)
        data = data[data[1] > top_n]
        name_list.extend(unidecode(item) for item in data.iloc[:, 0].dropna().tolist())

    return name_list

def get_unique_names(name_list, remove_list):
    """
    A function to make names unique and also remove names in remove_list.

    Parameters:
       name_list (List): The list of names.
       remove_list (List): The list of names to remove.
    Returns:
        List: Unique and filtered list of names.
    """
    # Get unique names
    name_list = sorted(
        list(set([word.title() for name in name_list for word in name.split(" ")]))
    )
    name_list = [name for name in name_list if name not in remove_list]
    
    return name_list

def get_name_recognizer(unique_names):
    """
    This function initializes and configures a name recognizer for Presidio. 

    Parameters:
        unique_names: The list of unique names to recognize.

    Returns:
        name_recognizer(PatternRecognizer): The initialized and configured recognizer.
    """
    name_recognizer = PatternRecognizer(supported_entity="PERSON", deny_list=unique_names)
    return name_recognizer

def get_number_recognizer(confidence=0.5):
    """
    This function initializes and configures a number recognizer for Presidio. 

    Parameters:
        confidence (float): The assigned confidence level for the pattern. Default is 0.5.

    Returns:
        number_recognizer(PatternRecognizer): The initialized and configured recognizer.
    """
    number_pattern = Pattern("NUMBER (\d+)", r"\b\d+\b", confidence)
    number_recognizer = PatternRecognizer(supported_entity="NUMBER", name="number_recognizer", patterns=[number_pattern])
    return number_recognizer

def get_single_char_recognizer(confidence=0.8):
    """
    This function initializes and configures a single character recognizer for Presidio, excluding for A and I, which can be words by itself.

    Parameters:
        confidence (float): The assigned confidence level for the pattern. Default is 0.8.

    Returns:
        single_char_recognizer(PatternRecognizer): The initialized and configured recognizer.
    """
    patterns = [Pattern("SingleCharExcludeAI", r"(?<= )([B-HJ-Z])(?= )", confidence)]
    single_char_recognizer = PatternRecognizer(
        supported_entity="SINGLE_CHAR_EXCLUDE_AI", patterns=patterns
    )
    return single_char_recognizer

def get_datetime_recognizer(month_list, week_list, time_list):
    """
    This function initializes a Presidio datetime recognizer with extended patterns.
    
    Parameters:
        month_list (list): List of month names.
        week_list (list): List of week day names.
        time_list (list): List of specific time-related names.

    Returns:
        datetime_recognizer(PatternRecognizer): The initialized and configured datetime recognizer.
    """
    
    datetime_list = month_list + week_list + time_list

    datetime_recognizer = PatternRecognizer(
        supported_entity="DATE_TIME", deny_list=datetime_list
    )
    
    return datetime_recognizer

def get_email_recognizer(email_domains):
    """
    This function initializes and configures an email domain recognizer for Presidio.
    
    Parameters:
        email_domains (list): List of email domains.

    Returns:
        email_recognizer(PatternRecognizer): The initialized and configured email recognizer.
    """
    email_list = list(set([domain.split(".")[0].title() for domain in email_domains]))
    email_recognizer = PatternRecognizer(supported_entity="EMAIL_DOMAIN", deny_list=email_list)
    return email_recognizer

def generate_location_list(country_names, state_names, city_names, airport_names):
    """
    Function to compile a list of locations from provided lists of country, state, city, and airport names.
    
    Parameters:
        country_names (list): List of country names.
        state_names (list): List of state names.
        city_names (list): List of city names.
        airport_names (list): List of airport names.

    Returns:
        list: Compiled list of location names.
    """

    location_list = sorted(
        list(set(country_names + state_names + city_names + airport_names))
    )
    
    location_list = [
        " ".join(re.sub(r"\(.*?\)", "", item).split()).title()
        for item in location_list
    ]
    
    return location_list

def get_location_recognizer(location_list):
    """
    This function initializes and configures a location recognizer for Presidio.
    
    Parameters:
        location_list (list): List of location names to recognize.

    Returns:
        location_recognizer(PatternRecognizer): The initialized and configured location recognizer.
    """
    location_recognizer = PatternRecognizer(
            supported_entity="LOCATION", deny_list=location_list
        )
    return location_recognizer


def run():
    """
    The main function to organize and control the process of creating recognizers and preprocessing data.
    """
    ## PREPROCESSING FOR NAME RECOGNIZER ##
    # List to hold names
    name_list = []
    remove_list = ["A", "Job", "You", "West", "Don", "Young", "Ma", "Said", "Bye Bye", "Okay", "Perfect"]
    
    # Path to zipped folder
    path_to_zip = "data/names.zip"

    # Extract zipped folder
    with zipfile.ZipFile(path_to_zip, "r") as zip_ref:
        zip_ref.extractall("data/ssa_names")

    # Process names from .txt files
    name_list = process_name_txt_files(folder_path="data/ssa_names", name_list=name_list)

    # Process names from .csv files
    name_list = process_name_csv_files(csv_folder_path="data/hispanic_names", name_list=name_list)

    # Add additional names
    name_list += ["Maryellen", "Chantelle", "Kirkland", "Johnston"]

    # Create a list of unique names
    unique_names = get_unique_names(name_list, remove_list)
    ##

    ## PREPROCESSING FOR LOCATION RECOGNIZER ##
    # Define lists of location categories
    country_names = [
        "United States",
        "America",
        "Canada",
        "Afghanistan",
        "Albania",
        "Algeria",
        "American Samoa",
        "Andorra",
        "Angola",
        "Anguilla",
        "Antarctica",
        "Antigua and Barbuda",
        "Argentina",
        "Armenia",
        "Aruba",
        "Australia",
        "Austria",
        "Azerbaijan",
        "Bahamas",
        "Bahrain",
        "Bangladesh",
        "Barbados",
        "Belarus",
        "Belgium",
        "Belize",
        "Benin",
        "Bermuda",
        "Bhutan",
        "Bolivia",
        "Bosnia and Herzegovina",
        "Botswana",
        "Bouvet Island",
        "Brazil",
        "British Indian Ocean Territory",
        "Brunei Darussalam",
        "Bulgaria",
        "Burkina Faso",
        "Burundi",
        "Cambodia",
        "Cameroon",
        "Cape Verde",
        "Cayman Islands",
        "Central African Republic",
        "Chad",
        "Chile",
        "China",
        "Christmas Island",
        "Cocos (Keeling) Islands",
        "Colombia",
        "Comoros",
        "Congo",
        "Cook Islands",
        "Costa Rica",
        "Croatia (Hrvatska)",
        "Cuba",
        "Cyprus",
        "Czech Republic",
        "Denmark",
        "Djibouti",
        "Dominica",
        "Dominican Republic",
        "East Timor",
        "Ecudaor",
        "Egypt",
        "El Salvador",
        "Equatorial Guinea",
        "Eritrea",
        "Estonia",
        "Ethiopia",
        "Falkland Islands",
        "Faroe Islands",
        "Fiji",
        "Finland",
        "France",
        "France, Metropolitan",
        "French Guiana",
        "French Polynesia",
        "French Southern Territories",
        "Gabon",
        "Gambia",
        "Georgia",
        "Germany",
        "Ghana",
        "Gibraltar",
        "Greece",
        "Greenland",
        "Grenada",
        "Guadeloupe",
        "Guam",
        "Guatemala",
        "Guinea",
        "Guinea-Bissau",
        "Guyana",
        "Haiti",
        "Heard and Mc Donald Islands",
        "Honduras",
        "Hong Kong",
        "Hungary",
        "Iceland",
        "India",
        "Indonesia",
        "Iran",
        "Iraq",
        "Ireland",
        "Israel",
        "Italy",
        "Ivory Coast",
        "Jamaica",
        "Japan",
        "Jordan",
        "Kazakhstan",
        "Kenya",
        "Kiribati",
        "Korea",
        "Kosovo",
        "Kuwait",
        "Kyrgyzstan",
        "Laos",
        "Latvia",
        "Lebanon",
        "Lesotho",
        "Liberia",
        "Libya",
        "Liechtenstein",
        "Lithuania",
        "Luxembourg",
        "Macau",
        "Macedonia",
        "Madagascar",
        "Malawi",
        "Malaysia",
        "Maldives",
        "Mali",
        "Malta",
        "Marshall Islands",
        "Martinique",
        "Mauritania",
        "Mauritius",
        "Mayotte",
        "Mexico",
        "Micronesia",
        "Moldova",
        "Monaco",
        "Mongolia",
        "Montserrat",
        "Morocco",
        "Mozambique",
        "Myanmar",
        "Namibia",
        "Nauru",
        "Nepal",
        "Netherlands",
        "Netherlands Antilles",
        "New Caledonia",
        "New Zealand",
        "Nicaragua",
        "Niger",
        "Nigeria",
        "Niue",
        "Norfork Island",
        "Northern Mariana Islands",
        "Norway",
        "Oman",
        "Pakistan",
        "Palau",
        "Panama",
        "Papua New Guinea",
        "Paraguay",
        "Peru",
        "Philippines",
        "Pitcairn",
        "Poland",
        "Portugal",
        "Puerto Rico",
        "Qatar",
        "Reunion",
        "Romania",
        "Russian Federation",
        "Rwanda",
        "Saint Kitts and Nevis",
        "Saint Lucia",
        "Saint Vincent and the Grenadines",
        "Samoa",
        "San Marino",
        "Sao Tome and Principe",
        "Saudi Arabia",
        "Senegal",
        "Seychelles",
        "Sierra Leone",
        "Singapore",
        "Slovakia",
        "Slovenia",
        "Solomon Islands",
        "Somalia",
        "South Africa",
        "South Georgia South Sandwich Islands",
        "South Sudan",
        "Spain",
        "Sri Lanka",
        "St. Helena",
        "St. Pierre and Miquelon",
        "Sudan",
        "Suriname",
        "Svalbarn and Jan Mayen Islands",
        "Swaziland",
        "Sweden",
        "Switzerland",
        "Syrian Arab Republic",
        "Taiwan",
        "Tajikistan",
        "Tanzania, United Republic of",
        "Thailand",
        "Togo",
        "Tokelau",
        "Tonga",
        "Trinidad and Tobago",
        "Tunisia",
        "Turkey",
        "Turkmenistan",
        "Turks and Caicos Islands",
        "Tuvalu",
        "Uganda",
        "Ukraine",
        "United Arab Emirates",
        "United Kingdom",
        "United States minor outlying islands",
        "Uruguay",
        "Uzbekistan",
        "Vanuatu",
        "Vatican City State",
        "Venezuela",
        "Vietnam",
        "Virigan Islands",
        "Wallis and Futuna Islands",
        "Western Sahara",
        "Yemen",
        "Yugoslavia",
        "Zaire",
        "Zambia",
        "Zimbabwe",
    ]
    state_names = [
        "Alaska",
        "Alabama",
        "Arkansas",
        "American Samoa",
        "Arizona",
        "California",
        "Colorado",
        "Connecticut",
        "District Of Columbia",
        "Delaware",
        "Florida",
        "Georgia",
        "Guam",
        "Hawaii",
        "Iowa",
        "Idaho",
        "Illinois",
        "Indiana",
        "Kansas",
        "Kentucky",
        "Louisiana",
        "Massachusetts",
        "Maryland",
        "Maine",
        "Michigan",
        "Minnesota",
        "Missouri",
        "Mississippi",
        "Montana",
        "North Carolina",
        "North Dakota",
        "Nebraska",
        "New Hampshire",
        "New Jersey",
        "New Mexico",
        "Nevada",
        "New York",
        "Ohio",
        "Oklahoma",
        "Oregon",
        "Pennsylvania",
        "Puerto Rico",
        "Rhode Island",
        "South Carolina",
        "South Dakota",
        "Tennessee",
        "Texas",
        "Utah",
        "Virginia",
        "Virgin Islands",
        "Vermont",
        "Washington",
        "Wisconsin",
        "West Virginia",
        "Wyoming",
    ]
    city_names = [
        "Aberdeen",
        "Abilene",
        "Akron",
        "Albany",
        "Albuquerque",
        "Alexandria",
        "Allentown",
        "Amarillo",
        "Anaheim",
        "Anchorage",
        "Ann Arbor",
        "Antioch",
        "Apple Valley",
        "Appleton",
        "Arlington",
        "Arvada",
        "Asheville",
        "Athens",
        "Atlanta",
        "Atlantic City",
        "Augusta",
        "Aurora",
        "Austin",
        "Bakersfield",
        "Baltimore",
        "Barnstable",
        "Baton Rouge",
        "Beaumont",
        "Bel Air",
        "Bellevue",
        "Berkeley",
        "Bethlehem",
        "Billings",
        "Birmingham",
        "Bloomington",
        "Boise",
        "Boise City",
        "Bonita Springs",
        "Boston",
        "Boulder",
        "Bradenton",
        "Bremerton",
        "Bridgeport",
        "Brighton",
        "Brownsville",
        "Bryan",
        "Buffalo",
        "Burbank",
        "Burlington",
        "Cambridge",
        "Canton",
        "Cape Coral",
        "Carrollton",
        "Cary",
        "Cathedral City",
        "Cedar Rapids",
        "Champaign",
        "Chandler",
        "Charleston",
        "Charlotte",
        "Chattanooga",
        "Chesapeake",
        "Chicago",
        "Chula Vista",
        "Cincinnati",
        "Clarke County",
        "Clarksville",
        "Clearwater",
        "Cleveland",
        "College Station",
        "Colorado Springs",
        "Columbia",
        "Columbus",
        "Concord",
        "Coral Springs",
        "Corona",
        "Corpus Christi",
        "Costa Mesa",
        "Dallas",
        "Daly City",
        "Danbury",
        "Davenport",
        "Davidson County",
        "Dayton",
        "Daytona Beach",
        "Deltona",
        "Denton",
        "Denver",
        "Des Moines",
        "Detroit",
        "Downey",
        "Duluth",
        "Durham",
        "El Monte",
        "El Paso",
        "Elizabeth",
        "Elk Grove",
        "Elkhart",
        "Erie",
        "Escondido",
        "Eugene",
        "Evansville",
        "Fairfield",
        "Fargo",
        "Fayetteville",
        "Fitchburg",
        "Flint",
        "Fontana",
        "Fort Collins",
        "Fort Lauderdale",
        "Fort Smith",
        "Fort Walton Beach",
        "Fort Wayne",
        "Fort Worth",
        "Frederick",
        "Fremont",
        "Fresno",
        "Fullerton",
        "Gainesville",
        "Garden Grove",
        "Garland",
        "Gastonia",
        "Gilbert",
        "Glendale",
        "Grand Prairie",
        "Grand Rapids",
        "Grayslake",
        "Green Bay",
        "GreenBay",
        "Greensboro",
        "Greenville",
        "Gulfport-Biloxi",
        "Hagerstown",
        "Hampton",
        "Harlingen",
        "Harrisburg",
        "Hartford",
        "Havre de Grace",
        "Hayward",
        "Hemet",
        "Henderson",
        "Hesperia",
        "Hialeah",
        "Hickory",
        "High Point",
        "Hollywood",
        "Honolulu",
        "Houma",
        "Houston",
        "Howell",
        "Huntington",
        "Huntington Beach",
        "Huntsville",
        "Independence",
        "Indianapolis",
        "Inglewood",
        "Irvine",
        "Irving",
        "Jackson",
        "Jacksonville",
        "Jefferson",
        "Jersey City",
        "Johnson City",
        "Joliet",
        "Kailua",
        "Kalamazoo",
        "Kaneohe",
        "Kansas City",
        "Kennewick",
        "Kenosha",
        "Killeen",
        "Kissimmee",
        "Knoxville",
        "Lacey",
        "Lafayette",
        "Lake Charles",
        "Lakeland",
        "Lakewood",
        "Lancaster",
        "Lansing",
        "Laredo",
        "Las Cruces",
        "Las Vegas",
        "Layton",
        "Leominster",
        "Lewisville",
        "Lexington",
        "Lincoln",
        "Little Rock",
        "Long Beach",
        "Lorain",
        "Los Angeles",
        "Louisville",
        "Lowell",
        "Lubbock",
        "Macon",
        "Madison",
        "Manchester",
        "Marina",
        "Marysville",
        "McAllen",
        "McHenry",
        "Medford",
        "Melbourne",
        "Memphis",
        "Merced",
        "Mesa",
        "Mesquite",
        "Miami",
        "Milwaukee",
        "Minneapolis",
        "Miramar",
        "Mission Viejo",
        "Mobile",
        "Modesto",
        "Monroe",
        "Monterey",
        "Montgomery",
        "Moreno Valley",
        "Murfreesboro",
        "Murrieta",
        "Muskegon",
        "Myrtle Beach",
        "Naperville",
        "Naples",
        "Nashua",
        "Nashville",
        "New Bedford",
        "New Haven",
        "New London",
        "New Orleans",
        "New York",
        "New York City",
        "Newark",
        "Newburgh",
        "Newport News",
        "Norfolk",
        "Normal",
        "Norman",
        "North Charleston",
        "North Las Vegas",
        "North Port",
        "Norwalk",
        "Norwich",
        "Oakland",
        "Ocala",
        "Oceanside",
        "Odessa",
        "Ogden",
        "Oklahoma City",
        "Olathe",
        "Olympia",
        "Omaha",
        "Ontario",
        "Orange",
        "Orem",
        "Orlando",
        "Overland Park",
        "Oxnard",
        "Palm Bay",
        "Palm Springs",
        "Palmdale",
        "Panama City",
        "Pasadena",
        "Paterson",
        "Pembroke Pines",
        "Pensacola",
        "Peoria",
        "Philadelphia",
        "Phoenix",
        "Pittsburgh",
        "Plano",
        "Pomona",
        "Pompano Beach",
        "Port Arthur",
        "Port Orange",
        "Port Saint Lucie",
        "Port St. Lucie",
        "Portland",
        "Portsmouth",
        "Poughkeepsie",
        "Providence",
        "Provo",
        "Pueblo",
        "Punta Gorda",
        "Racine",
        "Raleigh",
        "Rancho Cucamonga",
        "Reading",
        "Redding",
        "Reno",
        "Richland",
        "Richmond",
        "Richmond County",
        "Riverside",
        "Roanoke",
        "Rochester",
        "Rockford",
        "Roseville",
        "Round Lake Beach",
        "Sacramento",
        "Saginaw",
        "Saint Louis",
        "Saint Paul",
        "Saint Petersburg",
        "Salem",
        "Salinas",
        "Salt Lake City",
        "San Antonio",
        "San Bernardino",
        "San Buenaventura",
        "San Diego",
        "San Francisco",
        "San Jose",
        "Santa Ana",
        "Santa Barbara",
        "Santa Clara",
        "Santa Clarita",
        "Santa Cruz",
        "Santa Maria",
        "Santa Rosa",
        "Sarasota",
        "Savannah",
        "Scottsdale",
        "Scranton",
        "Seaside",
        "Seattle",
        "Sebastian",
        "Shreveport",
        "Simi Valley",
        "Sioux City",
        "Sioux Falls",
        "South Bend",
        "South Lyon",
        "Spartanburg",
        "Spokane",
        "Springdale",
        "Springfield",
        "St. Louis",
        "St. Paul",
        "St. Petersburg",
        "Stamford",
        "Sterling Heights",
        "Stockton",
        "Sunnyvale",
        "Syracuse",
        "Tacoma",
        "Tallahassee",
        "Tampa",
        "Temecula",
        "Tempe",
        "Thornton",
        "Thousand Oaks",
        "Toledo",
        "Topeka",
        "Torrance",
        "Trenton",
        "Tucson",
        "Tulsa",
        "Tuscaloosa",
        "Tyler",
        "Utica",
        "Vallejo",
        "Vancouver",
        "Vero Beach",
        "Victorville",
        "Virginia Beach",
        "Visalia",
        "Waco",
        "Warren",
        "Washington",
        "Waterbury",
        "Waterloo",
        "West Covina",
        "West Valley City",
        "Westminster",
        "Wichita",
        "Wilmington",
        "Winston",
        "Winter Haven",
        "Worcester",
        "Yakima",
        "Yonkers",
        "York",
        "Youngstown",
    ]
    airport_names = [
        "London Heathrow",
        "Amsterdam",
        "Kuala Lumpur",
        "Tokyo Haneda",
        "Istanbul",
        "Seoul Incheon",
        "Paris",
        "Toronto",
        "Singapore",
        "Manila",
        "Dubai",
        "Soekarno Hatta",
        "Munich",
        "Sydney",
        "Delhi",
        "Madrid",
        "Bogota",
        "Fukuoka",
        "Shanghai Pudong",
        "Doha",
        "Mumbai",
        "Johannesburg",
        "Rome Fiumicino",
        "Athens",
        "Barcelona",
        "Taipei",
        "Taoyuan",
        "Guangzhou Baiyun",
        "Hanoi",
        "Riyadh",
        "Auckland",
        "Wellington",
        "Melbourne",
    ]

    # Generate list of location names
    location_list = generate_location_list(country_names, state_names, city_names, airport_names)
    ##


    ## PROPROCESSING FOR DATETIME RECOGNIZER ##
    # Define datetime word lists
    month_list = [
        "January",
        "February",
        "March",
        "April",
        # "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    week_list = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    time_list = ["Noon", "Midnight"]
    ##

    ## PREPROCESSING FOR EMAIL DOMAIN RECOGNIZER ##
    email_domains = [
    "aol.com",
    "att.net",
    "comcast.net",
    "facebook.com",
    "gmail.com",
    "gmx.com",
    "googlemail.com",
    "google.com",
    "hotmail.com",
    "mac.com",
    "microsoft.com",
    "msn.com",
    "verizon.net",
    "yahoo.com",
]
    ##

    # Create recognizers
    name_recognizer = get_name_recognizer(unique_names)
    location_recognizer = get_location_recognizer(location_list)
    datetime_recognizer = get_datetime_recognizer(month_list, week_list, time_list)
    number_recognizer = get_number_recognizer()
    single_char_recognizer = get_single_char_recognizer()
    email_recognizer = get_email_recognizer(email_domains)

    # Initialize the AnalyzerEngine
    analyzer = AnalyzerEngine()
    analyzer.registry.add_recognizer(name_recognizer)
    analyzer.registry.add_recognizer(number_recognizer)
    analyzer.registry.add_recognizer(single_char_recognizer)
    analyzer.registry.add_recognizer(datetime_recognizer)
    analyzer.registry.add_recognizer(location_recognizer)
    analyzer.registry.add_recognizer(email_recognizer)

    # Initialize the AnonymizerEngine
    anonymizer = AnonymizerEngine()

    return analyzer, anonymizer

if __name__ == '__main__':
    analyzer, anonymizer = run()

    ## Testing anonymization pipeline

    # Define your text
    text = "Hello, I'm Bob and I live in Vancouver, WA 90101 and my email is bobby@hotmail.com"  # 

    # Analyzer and Anonymizer
    results = analyzer.analyze(text=text, language="en")
    anonymized_text = anonymizer.anonymize(text=text, analyzer_results=results)
    print(anonymized_text.text)

