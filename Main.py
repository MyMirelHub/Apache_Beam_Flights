# Import beam libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# Initialise with default Options
p = beam.Pipeline(options=PipelineOptions())


# Function Definitions----------------------------------------------------------------------

# Import Geohash library, and apply function to lon,lat.
def geosh(lon, lat):
    import Geohash
    hsh = Geohash.encode(float(lon), float(lat))
    return hsh


# #String padding with trailing zeroes
def string_pad(path_order):
    pad = str(path_order).ljust(11, '0')
    return pad


# Key Value Mappings ---------------------------------------------------------

# <K><V> tuple with <Date,Departure_Schedule,Departure_Airport><Data>
class FlightKeys(beam.DoFn):
    def process(self, element):
        flightskey = element[0], element[12], element[5]
        geo_hash = geosh(element[-4], element[-3])
        padded = string_pad(element[-1])
        concat = element[5] + element[3]
        return [
            (flightskey, (element[:18], geo_hash, padded, concat))
        ]


# <K><V> Tuple, with <Airline><Date,Departure_Schedule,Departure_Airport>
class CollectFlights(beam.DoFn):
    def process(self, element):
        return [
            (element[1], (element[0], element[12], element[5]))
        ]


# <K><V> Tuple, with <Date,Time,Aiport><WeatherData>
class SplitWeather(beam.DoFn):
    def process(self, element):
        WeatherKey = element[0], element[2], element[1]
        return [
            (WeatherKey, element[3:])
        ]


# <K><V> Tuple, with <Airline,Date><Departure_Schedule,Departure_Airport>
class CollectFlightsDay(beam.DoFn):

    def process(self, element):
        key = element[1], element[0]
        value = (element[12], element[5])
        return [
            (key, (value))
        ]


# Arithmetic Operator
class GetTotal(beam.DoFn):
    def process(self, element):
        # get the total transactions for one item
        return [(str(element[0]), sum(element[1]))]


# Print function, for printing output in command terminal- useful for debugging
class Printer(beam.DoFn):
    def process(self, data_item):
        print data_item


# Pipeline

# Read the flights data
raw_flights = (p
               | "flights:read" >> ReadFromText("C:/Users/mirel/Desktop/flights_small.csv", skip_header_lines=1)
               | beam.Map(lambda record: (record.split(',')))
               )
# Turn it into a KV pair
flights_data = (raw_flights
                | beam.ParDo(FlightKeys())
                )
# Read the Weather Data, and turn it into KV pair
weather = (p
           | "readweather" >> ReadFromText("C:/Users/mirel/Desktop/weather.csv", skip_header_lines=1)
           | beam.Map(lambda record: (record.split(',')))
           | beam.ParDo(SplitWeather())
           )
"""
Turn Flights and weather data into a dictionary, Group them by their common key, Filter out values that don't match,
and extract values which are a match
"""
results = ((weather, flights_data)
           | beam.CoGroupByKey()
           | 'Filter' >> beam.Filter(lambda x: (len(x[1][0]) > 0 and len(x[1][1]) > 0))
           | beam.Values()
           | beam.Map(lambda (airport, data): '{},{}'.format(airport, data))
           )

# Total flights per airline
Total_flights = (raw_flights
                 | beam.ParDo(CollectFlights())
                 | beam.Map(lambda record: (record[0], 1))
                 | 'c' >> beam.GroupByKey()
                 | 'Get the total' >> beam.ParDo(GetTotal())
                 )
# Flights per Airline per day
Flights_Per_day = (raw_flights
                   | beam.ParDo(CollectFlightsDay())
                   | beam.Map(lambda record: (record[0], 1))
                   | beam.GroupByKey()
                   | 'Get the total in each day' >> beam.ParDo(GetTotal())
                   | beam.Map(lambda (airport, data): '{},{}'.format(airport, data))
                   )

# Write Output files, define custom header
results | WriteToText("C:/Users/mirel/Desktop/Flights_Weather", file_name_suffix='.csv',
                      header='temp,snow,wind,Date,Airline,Airline_code,Arrival_airport,Arrival_state,'
                             'Departure_Airport,Departure_State,Departure_actual,Departure_delay,Arrival_'
                             'actual,Arrival_delay,Arrival_schedule,Departure_schedule,DC_Longitude,DC_Latitude,'
                             'Longitude,Latitude,Route,geohash,Path_order,DepArr')

Flights_Per_day | "Fday" >> WriteToText(
    "C:/Users/mirel/Desktop/Flights_Per_Day", file_name_suffix='.csv', header='Airline,Date,Flight_Count')

Total_flights | "Total" >> WriteToText(
    "C:/Users/mirel/Desktop/Total_Flights", file_name_suffix='.csv', header='Airline,Flights')

p.run()
