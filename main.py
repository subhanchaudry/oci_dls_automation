# OCI Data Labeling Service Automation
# This script allows a user to automatically label images in the OCI Data Labeling Service from a CSV file containing the image file names and labels
# Authored by Subhan Chaudry (subhan.chaudry@oracle.com) and Daniel Bates (daniel.bates@oracle.com)

import oci
import csv
import threading

# This should be replaced with the OCID of the dataset in the OCI Data Labeling Service
dataset_ocid = 'ocid1.datalabelingdataset.oc1.iad.amaaaaaaytsgwayay7er7ljzyk3abfqvgut55u4bmh7rdxre3rbegz4upvwq'

# This should be replaced with the OCID of the compartment where your dataset is located
compartment_ocid = 'ocid1.compartment.oc1..aaaaaaaairmqqmhnovuqsbsgda7x3pvv5mwhkfcmhorlreilzsdkc5kuim2a'

def extract_data():
    # Make sure you have your OCI config file: https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm#SDK_and_CLI_Configuration_File
    config = oci.config.from_file()
    hm = {}
    
    # Replace the file name with the name of your CSV containing the image file names and appropriate labels
    with open('trainLabels.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in spamreader:
            file_name, value = row[0].split(',')
            hm[file_name] = value
    
    # The Data Labeling API can only handle a maximum of 1,000 records at a time so repeat it multiple times
    # Change num_calls depending on the size of your dataset
    num_calls = 7
    for x in range(num_calls):
        record_set(hm, config)

    print("All annotations are completed! :)")

def record_set(hm, config):
    data_labeling_service_dataplane_client = oci.data_labeling_service_dataplane.DataLabelingClient(
        config)
    list_records_response = data_labeling_service_dataplane_client.list_records(
        compartment_id=compartment_ocid,
        dataset_id=dataset_ocid,
        is_labeled=False,
        limit=1000)

    # Check the records returned that are unlabelled
    # print(list_records_response.data.items)

    # Multi-threading to speed up record processing
    # Change num_threads to how many threads you want
    num_threads = 25
    split_records = [list_records_response.data.items[i::num_threads] for i in range(num_threads)]

    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=record_labeling, args=(hm, config, split_records[i]))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

def record_labeling(hm, config, record_items):
    # We are splitting the name to remove ".jpeg" but it may not be necessary depending on how you name files in the CSV
    for entry in record_items:
        update_label(config, entry, hm[entry.name.split('.')[0]])
 
def update_label(config, entry, value):
    data_labeling_service_dataplane_client = oci.data_labeling_service_dataplane.DataLabelingClient(
        config, timeout=240)

    # Feel free to format the labels to be more readable
    # Comment this out if you don't need it
    # if value == '0':
    #     value = '0 - No DR'
    # elif value == '1':
    #     value = '1 - Mild'
    # elif value == '2':
    #     value = '2 - Moderate'
    # elif value == '3':
    #     value = '3 - Severe'
    # else:
    #     value = '4 - Proliferative DR'

    create_annotation_response = data_labeling_service_dataplane_client.create_annotation(
        create_annotation_details=oci.data_labeling_service_dataplane.models.CreateAnnotationDetails(
            record_id=entry.id,
            compartment_id=compartment_ocid,
            entities=[
                oci.data_labeling_service_dataplane.models.ImageObjectSelectionEntity(
                    entity_type="GENERIC",
                    labels=[
                        oci.data_labeling_service_dataplane.models.Label(
                            label=value)],)]))
    
    # Check the data from the response
    # print(create_annotation_response.data)
    
    print("Annotation for " + entry.name + " is completed")

if __name__ == '__main__':
    extract_data()
