#!/usr/bin/python
# This is a python application for exploring the pandas data analysis
# library using 311 Service Request data from Open Data NYC

def guess_borough(row):
    return row["park_borough"] if ("Unspecified" in row["borough"] and "Unspecified" not in row["park_borough"]) else row["borough"]

def main():

    try:

        # Dependencies
        import os
        import pandas as pd

        # Variables
        app_token = 'eQu3kE699WTRtnKO0535uG1cT'
        data_set_endpoint = "https://data.cityofnewyork.us/resource/fhrw-4uyv"
        data_set_file_extension = ".json"
        debugging = 1
        max_num_largest = 10
        max_num_query_results = 200000
        script_path_root_dir = os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))
        script_path_src_dir = os.path.dirname(os.path.abspath(__file__))
        zip_code_data_file_extension = ".csv"
        zip_code_data_file_name = "2010_Census_Population_By_Zipcode_ZCTA"
        zip_code_data_file_path = os.path.join(
            script_path_root_dir,
            "data",
            zip_code_data_file_name + zip_code_data_file_extension
        )

        # Check for sanity
        if debugging:
            print("Start executing")
            print("\nscript_path_root_dir : " + script_path_root_dir)
            print("script_path_src_dir : " + script_path_src_dir)
            print("zip_code_data_file_path : " +
                  zip_code_data_file_path + "\n")

        # Check for the zip code data file
        # This data will be used later to get the top n most
        # populated zip codes
        if not os.path.isfile(zip_code_data_file_path):
            raise IOError("Zip Code Data File Not Found")

        # Read and sort the zip code data file in descending order
        # via pandas based on the 2010 US Census population data
        zip_codes_df = pd.read_csv(zip_code_data_file_path)
        zip_codes_df = zip_codes_df.sort_values(
            by=["2010 Census Population"],
            ascending=False
        )

        # Get the top n most populated zip codes
        top_populated_zip_codes_df = zip_codes_df.head(max_num_largest)
        top_populated_zip_codes = top_populated_zip_codes_df["Zip Code ZCTA"].values.tolist(
        )
        top_populated_zip_codes = [str(i) for i in top_populated_zip_codes]

        # Check for sanity
        if debugging:
            print("Print zip_codes_df type : " + str(type(zip_codes_df)))
            print("Print len(zip_codes_df) : " + repr(len(zip_codes_df.index)))
            print("\nPrint Top " + repr(max_num_largest) +
                  " Most Populated Zip Codes\n")
            print(top_populated_zip_codes_df)
            print("\nPrint List of Top " +
                  repr(max_num_largest) + " Most Populated Zip Codes\n")
            print(top_populated_zip_codes)

        # Set the Socrata Open Data API (SODA) resource URL with query parameters
        data_set_final_url = data_set_endpoint + data_set_file_extension + "?$$app_token=" + \
            app_token + "&$select=*&$order=unique_key&$limit=" + \
            repr(max_num_query_results)
        print("\nPrint Data Set Final URL : " + data_set_final_url + "\n")

        # Access the NYC 311 Service Requests from 2010 to Present data set
        service_requests_df = pd.read_json(data_set_final_url, orient="records")

        # Guess the appropriate borough if borough is listed as 'Unspecified'
        # Unsuccessful attempts will keep borough listed as 'Unspecified'
        service_requests_df["borough"] = service_requests_df.apply(guess_borough, axis=1)

        # Check for sanity
        if debugging:
            print("Print service_requests_df type : " + str(type(service_requests_df)))
            print("Print len(service_requests_df) : " +
                  repr(len(service_requests_df.index)))
            print("Print service_requests_df.head()")
            print(service_requests_df.head())

        # Find the total number of complaints for each complaint type
        complaint_type_totals = service_requests_df["complaint_type"].value_counts()

        # Check for sanity
        if debugging:
            print("\nPrint Top " + repr(max_num_largest) +
                  " Types of Complaints\n")
            print(complaint_type_totals.nlargest(max_num_largest))
            print()

        # Find and list the top n types of complaints
        top_complaint_type_names = complaint_type_totals.nlargest(
            max_num_largest).keys().tolist()
        print("\nPrint List of Top " +
              repr(max_num_largest) + " Types of Complaints\n")
        for ct in top_complaint_type_names:
            print(ct)
        print()

        # Find the total number of complaints for each borough
        complaint_totals_per_borough = service_requests_df["borough"].value_counts()
        print("\nPrint Total Number of Complaints per Borough\n")
        print(complaint_totals_per_borough)
        print()

        # Get all the boroughs found in the data set
        borough_names = complaint_totals_per_borough.keys().tolist()

        # Check for sanity
        if debugging:
            print("\nPrint List of Boroughs\n")
            for b in borough_names:
                print(b)
            print()

        # Keep only the service requests that are of the top n types of complaints
        top_complaint_types_boroughs_df = service_requests_df[service_requests_df.complaint_type.isin(
            top_complaint_type_names)]
        top_complaint_types_zips_df = top_complaint_types_boroughs_df.copy(
            deep=True)

        # Group the service requests by the types of complaints
        # Count each complaint occurrence for each borough
        top_complaint_types_per_borough = top_complaint_types_boroughs_df.groupby(
            ["complaint_type", "borough"]).size().to_frame(name="number_of_complaints").reset_index()

        # Output the top n types of complaints for each borough in descending order
        print("\nPrint Number of Top " + repr(max_num_largest) +
              " Types of Complaints per Borough\n")
        print(top_complaint_types_per_borough.sort_values(
            by=["borough", "number_of_complaints", "complaint_type"], ascending=False))

        # Keep only the service requests that are reported from the top n most populated zip codes
        top_complaint_types_top_zip_grouped = top_complaint_types_zips_df[top_complaint_types_zips_df.incident_zip.isin(
            top_populated_zip_codes)]

        # Group the service requests by the types of complaints
        # Count each complaint occurrence in the top n most populated zip codes
        top_complaint_types_per_top_zip = top_complaint_types_top_zip_grouped.groupby(
            ["complaint_type", "incident_zip"]).size().to_frame(name="number_of_complaints").reset_index()

        # Output the top n types of complaints the top n most populated zip codes in descending order
        print("\nPrint Number of Top " + repr(max_num_largest) +
              " Types of Complaints per Top " + repr(max_num_largest) + " Most Populated Zip Codes\n")
        print(top_complaint_types_per_top_zip.sort_values(
            by=["incident_zip", "number_of_complaints", "complaint_type"], ascending=False))
        print("\n")

        # Check for sanity
        if debugging:
            print("End executing")

    except ImportError:
        print("Missing dependencies. Try running 'pip install -r requirements.txt' at the top-most directory in this project.")
    except (OSError, IOError) as ose:
        print(ose)
    except Exception as e:
        print(e)


main()
