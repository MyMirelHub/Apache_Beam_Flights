# Apache Beam Flights Challenge

## Design Choices

### Choosing a SDK

The choice was made to write the program using the  python SDK, due to
the ease of setting up the initial environment and setting installing the dependencies. Java was considered but it was more difficult to set up the IDE as it required setting up the dependencies through maven and creating a pom.xml file. After discovering Beam more, it was evident that the Java SDK was much more complete and contained abilities to do more complex joins, manipulate keys and had a pseudo SQL connector. Considering the size and the relational nature
of the Datasets it would have been a better choice.

#### Dependencies

```shell
#!/bin/bash
pip install Geohash
```

### ETL Process

The diagram shows the transformations which are going to take place. This will be to read the two files, transform the data by mapping key values and loading it into three outputs; Flights data with weather, total flights per airline and per airline/day.

![ETL][logo]

[logo]: https://github.com/MyMirelHub/Apache_Beam_Flights/blob/master/ELT%20Beam.png?raw=true "etl_beam"

## Step 1 - Combining The Weather Data 

### Data Reference

This is given as reference as after the data is split, the index values are used.

*Flights*

```
Date,Airline,Airline_code,Arrival_airport,Arrival_state,Departure_Airport,Departure_State,Departure_actual,Departure_delay,Arrival_actual,Arrival_delay,Arrival_schedule,Departure_schedule,DC_Longitude,DC_Latitude,Longitude,Latitude,Route,Path_order
```

*Weather*

```
Date,airport,time,temperature,snow,wind
```

## Parsing the input data

The built-in transform `apache_beam.io.textio.ReadFromText` reads the contents of the file into `PCollection` (beams immutable object file). A split is then applied to process the data as a dictionary.

```python
# Read the flights data
raw_flights = (p
               | "flights:read" >> ReadFromText("C:/Users/mirel/Desktop/flights_small.csv", skip_header_lines=1)
               | beam.Map(lambda record: (record.split(',')))
               )
```

We then want to map the information under relevant keys.

```python
# Turn it into a KV pair
flights_data = (raw_flights
                | beam.ParDo(FlightKeys())
                )
```

Where `FlightKeys` is function which extracts tuples and forms them into a KV pair. In this case we are creating a common key for merging the weather data(Date,Time,Airport) with the value being the rest of the flight data. 

```python
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
```

Within this `DoFn` process, The departure and arrival airports are concatenated, string padding is applied to the path order, and geohash is formed from latitude, longitude.

```python
# Import Geohash library, and apply function to lon,lat.
def geosh(lon, lat):
    import Geohash
    hsh = Geohash.encode(float(lon), float(lat))
    return hsh

# #String padding with trailing zeroes
def string_pad(path_order):
    pad = str(path_order).ljust(11, '0')
    return pad
```

### Transforming the Data 
A similar KV mapping is done with the weather, as documented in the Main.py. And the two datasets are ready for grouping. CoGrouByKey takes the two separate Key collections and groups them by the common key mapped earlier. In other big data systems, it’s called MapReduce, and ideally you want to avoid doing this operation too many times as all the information for a single key is gathered into the same machine, and different values for this key may be processed by other machines, leading to too many IO from the workers and not the memory.

```python
results = ((weather, flights_data)
           | beam.CoGroupByKey()
           | 'Filter' >> beam.Filter(lambda x: (len(x[1][0]) > 0 and len(x[1][1]) > 0))
           | beam.Values()
           | beam.Map(lambda (airport, data): '{},{}'.format(airport, data))
           )
```
### Writing the Data

Last step was writing the data to CSV, and manually reattaching the headers. Due to direct runner being slow, the dataset for this process was significantly reduced and a couple of rows were changed to match the weather data, as a proof of concept to whether the grouping works. 

```Python
# Write Output files, define custom header
results | WriteToText("C:/Users/mirel/Desktop/Flights_Weather", file_name_suffix='.csv',
                      header='temp,snow,wind,Date,Airline,Airline_code,Arrival_airport,Arrival_state,'
                             'Departure_Airport,Departure_State,Departure_actual,Departure_delay,Arrival_'
                             'actual,Arrival_delay,Arrival_schedule,Departure_schedule,DC_Longitude,DC_Latitude,'
                             'Longitude,Latitude,Route,geohash,Path_order,DepArr')
```

## Step 2 - Calculating Totals

A similar process was employed for calculating the totals. Key values were grouped, and `GroupBykey` was run on the Pcollection.

```python
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
```

The class. `CollectFlights` is to calculate the total number of flights per airline, and the criteria for a unique flight is assumed to have a have a distinct *date, departure time, Departure Airport*. Although this might not always be the case, including and additional data like the route could increase accuracy. Or if a flight number was provided would increase the likelihood than it is a distinct flight taking place. 

The code Maps `record[0]` - the key, attaches a value `1` to it, and after running `GroupByKey`, it combines similar keys and the `GetTotal` class is called to sum the 1's per group. 

```python
class GetTotal(beam.DoFn):
    def process(self, element):
        # get the total transactions for one item
        return [(str(element[0]), sum(element[1]))]
```

The exact same step is followed with `CollectFlightsDay` which calculates the flights per airline per day, but the keys are mapped differently. 

``` Python
# <K><V> Tuple, with <Airline,Date><Departure_Schedule,Departure_Airport>
class CollectFlightsDay(beam.DoFn):

    def process(self, element):
        key = element[1], element[0]
        value = (element[12], element[5])
        return [
            (key, (value))
        ]
```

## Next Steps/ Limitations 

- Add Arrival Weather. This was very difficult in beam, as the common key was for departure weather, and new KV pairs would need to be generated for the arrival weather. Could look into windowing, or Java which supports more join functionalities. 

- Cleaning the output data. As noticed, the output datasets from being converted into dictionaries and grouped by keys are messy with outer brackets, square brackets and the "u-" dictionary character bind. I tried running regex, but could not to it on the immutable grouped Pcollection, and different IO connectors  may have different final parsing, so would need to be investigated further. 
