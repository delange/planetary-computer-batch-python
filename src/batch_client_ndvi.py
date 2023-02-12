import sys
import os
import argparse
import datetime
from datetime import date
import uuid
import re
import pystac
from pystac_client import Client
from pystac.extensions.eo import EOExtension as eo
from shapely.geometry import Point, Polygon, mapping

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
import azure.storage.blob as azureblob
import azure.batch as azurebatch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

sys.path.append(os.getcwd())
import config_local


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.
    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')

def get_container_sas_token(account_name,
                            container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.
    :param str account_name: The name of the Azure Blob storage account
    :param str container_name: The name of the Azure Blob storage container.
    :param blob_permissions: A blob SAS permission object
    :type blob_permissions: blob_permissions: `azure.storage.blob.blobsaspermissions`
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately. Expiration is in 2 hours.

    container_sas_token = azureblob.generate_container_sas( \
        account_name,
        container_name,
        account_key=_STORAGE_ACCOUNT_KEY,
        permission=blob_permissions,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=24))

    return container_sas_token


def get_container_sas_url(blob_service_client, account_name,
                          container_name, blob_permissions):
    """
    Obtains a shared access signature URL that provides write access to the
    output container to which the tasks will upload their output.

    :param blob_service_client: A blob service client.
    :type blob_service_client: `azure.storage.blob.BlobServiceClient`
    :param str account_name: The name fo the Azure Blob storage account
    :param str container_name: The name of the Azure Blob storage container.
    :param blob_permissions: A blob SAS permission object
    :type blob_permissions: blob_permissions: `azure.storage.blob.blobsaspermissions`
    :rtype: str
    :return: A SAS URL granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container.
    sas_token = get_container_sas_token(account_name,
                                        container_name, blob_permissions)

    # Construct SAS URL for the container
    print(account_name, container_name, sas_token)
    container_sas_url = "https://{}.blob.core.windows.net/{}?{}".format(account_name, container_name, sas_token)

    return container_sas_url


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = azurebatch.models.JobAddParameter(
        id=job_id,
        pool_info=azurebatch.models.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)



def add_tasks_ndvi(batch_service_client, job_id, input_files, output_container_sas_url, name, satellite, post_processing):
    """
    Adds a task for each input file in the collection to the specified job.
    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_url: A SAS token granting write access to
    the specified Azure Blob storage container.
    :param str name: a string with user name
    :param str satellite: a string with satellite name.
    :param str post_processing: a string with post-processing name, like NDVI.
    """
  
    print('Adding {} tasks to job [{}]...'.format(len(input_files), job_id))

 
    task_container_settings = azurebatch.models.TaskContainerSettings(
        image_name='mcr.microsoft.com/planetary-computer/python',
        container_run_options='--rm')

    for idx, input_file in enumerate(input_files):
        pc_id = input_file.id
        footprint = input_file.geometry
        red_href = input_file.assets["B04"].href
        nir_href = input_file.assets["B08"].href
        geometry = re.sub("'", '"', str(footprint))
        output_file_name = "ndvi__{}".format(pc_id)
        output_file_name_tif = "ndvi__{}.tif".format(pc_id)
        output_file_name_footprint = "ndvi__{}.geojson".format(pc_id)
        with open(output_file_name_footprint, 'w') as writer:
            writer.write(geometry)
        current_datetime = datetime.datetime.now()
        current_datetime_str = current_datetime.strftime("%Y%m%d-%H%M%S")
        random_hash = uuid.uuid4().hex
        task_id = name + "_ndvi_" + current_datetime_str + "_" + random_hash
        command = '/bin/bash -c "cp -r $AZ_BATCH_NODE_STARTUP_DIR/wd $AZ_BATCH_TASK_WORKING_DIR && python3 $AZ_BATCH_TASK_WORKING_DIR/wd/batch_task_ndvi.py --red {} --nir {} --output {}"'.format(red_href, nir_href, output_file_name)
       

        batchtask = azurebatch.models.TaskAddParameter(
            id=task_id,
            command_line=command,
            container_settings=task_container_settings,
            constraints=batchmodels.TaskConstraints(retention_time=datetime.timedelta(minutes=60)),
            output_files=[batchmodels.OutputFile(
                file_pattern= output_file_name_tif,
                destination=batchmodels.OutputFileDestination(
                    container=batchmodels.OutputFileBlobContainerDestination(
                        container_url=output_container_sas_url)),
                upload_options=batchmodels.OutputFileUploadOptions(
                    upload_condition=batchmodels.OutputFileUploadCondition.task_completion))]
        )
        batch_service_client.task.add(job_id, batchtask)


def do_stac_search(satellite, post_processing, coordinates, date_range_start, date_range_end, cloud_cover):
    """
    Returns a list of stac items fullfill the search query.
    :param str satellite: Satellite name [Sentinel2], string
    :param str post_processing: PostProcessing [NDVI], string
    :param str coordinates: Coordinates of area of interest, string format: lowerleft_lat,lowerleft_long,upperright_lat,upperright_long
    :param str date_range_start: Start date for the period of interest [YYYY-mm-dd], string
    :param str date_range_end: End date for the period of interest [YYYY-mm-dd], string
    :param str cloud_cover: Max. accepted cloud coverage in percent [1-100], string
    """

    # Populate time range
    if validate_date(date_range_start) and validate_date(date_range_end) \
        and datetime.datetime.strptime(date_range_start, '%Y-%m-%d') < datetime.datetime.strptime(date_range_end, '%Y-%m-%d'):
        time_range = date_range_start + "/" + date_range_end
    else :
        # dummy time range, ranging from 1 February 2023 til day of invoking:
        start_time = "2023-01-01"
        end_time = date.today()
        end_formatted = end_time.strftime("%Y-%m-%d")
        time_range = str(start_time) + "/" + str(end_time)
    print("Actual processing occur for the period: " + time_range)
    

    # Populate area of interest
    try:
        coords = re.split(',', coordinates, maxsplit=3)
        ll_lat = float(coords[0])
        ll_lon = float(coords[1])
        ur_lat = float(coords[2])
        ur_lon = float(coords[3])
        lower_left = validate_coordinate(ll_lat, ll_lon)
        upper_right = validate_coordinate(ur_lat, ur_lon)
        if lower_left and upper_right:
            aoi_polygon = Polygon([(ll_lon, ll_lat), (ur_lon, ll_lat), (ur_lon, ur_lat), (ll_lon, ur_lat), (ll_lon, ll_lat)])
            check_aio_poly = aoi_polygon.is_valid
            area_of_interest = mapping(aoi_polygon)
            print("area of interest = from input")
    except:
        # the input coordinates where false, they will be replaced with corners for the Netherlands (default)
        # other bounding boxes can also be used from: https://gist.github.com/botzill/fc2a1581873200739f6dc5c1daf85a7d
        # reference is from discussion: https://gist.github.com/graydon/11198540
        area_of_interest = {
            "type": "Polygon",
            "coordinates": [
                [
                    [7.09205325687, 50.803721015],
                    [3.31497114423, 50.803721015],
                    [3.31497114423, 53.5104033474],
                    [7.09205325687, 53.5104033474],
                    [7.09205325687, 50.803721015],
                ]
             ],
            }
        print("area of interest = from default setting (bounding box for The Netherlands)")


    # Populate satellite / sensor
    # only sentinel2 satellite implemented yet:
    if satellite == "Sentinel2":
        satellite = "sentinel-2-l2a"
    else:
        satellite = "sentinel-2-l2a"


    # Populate cloud coverage
    # only sentinel2 satellite implemented yet:
    try:
        cloud_cover = float(cloud_cover)
        if cloud_cover >= 0 and cloud_cover <= 100:
            cloud_cover = cloud_cover
        elif cloud_cover > 100:
            cloud_cover = 100
        else: cloud_cover = 0
    except:
        # without meaningfull cloud cover setting, it default to the following percentage
        cloud_cover = 100
    print("Max cloud cover used: " + str(cloud_cover))


    # Populate pre processing
    # only NDVI implemented yet:
    if post_processing == "NDVI":
        post_processing = "NDVI"
    else:
        post_processing == "NDVI"


    # Planetary Computer search
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    collections = catalog.get_children()

    search = catalog.search(
        collections = [satellite],
        intersects = area_of_interest, 
        datetime = time_range, 
        query={"eo:cloud_cover": {"lt": cloud_cover}}
    )

    test_items = list(search.get_items())
    for item in test_items:
        print(f"{item.id}: {item.datetime}")

    return list(search.get_items())



def validate_date(date_text):
    """
    Returns True when input date fullfill the required string format of YYYY-MM-DD, otherwise False.
    :param str date_txt: An input data string with format: YYYY-MM-DD.
    """
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except:
        print("Incorrect data format, should be YYYY-MM-DD")
        return False



def validate_coordinate(lat, lon):
    """
    Returns True when input date fullfill the required string format of lat [-90:90] and lon [-180:180], otherwise False.
    :param str lat: An input latitude string with format 'values' ranging between -90 and 90.
    :param str lon: An input longitude string with format 'values' ranging between -180 and 180.
    """
    try:
        s = Point(lon, lat)
        if s.is_valid:
            return True
        else: 
            return False
            print("Coordinate True (" + str(lon) + "," + str(lat) + ") is: incorrect")
    except:
        print("Coordinate False (" + str(lon) + "," + str(lat) + ") is: incorrect")
        return False



if __name__ == '__main__':
 
    name = 'Rem' # name, string
    satellite = 'Sentinel2' # Satellite name [Sentinel2], string
    post_processing = 'NDVI' # PostProcessing [NDVI], string
    coordinates = '' # Coordinates of area of interest, string format: lowerleft_lat,lowerleft_long,upperright_lat,upperright_long
    date_range_start = '' # Start date for the period of interest [YYYY-mm-dd], string
    date_range_end = '' # End date for the period of interest [YYYY-mm-dd], string
    cloud_cover = '10' # Max. accepted cloud coverage in percent [1-100], string
 
    items = do_stac_search(satellite, post_processing, coordinates, date_range_start, date_range_end, cloud_cover)

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    
    blob_service_client = azureblob.BlobServiceClient(
           _STORAGE_URL, 
           _STORAGE_ACCOUNT_KEY)

    output_container_name = 'batch-output'
    
    sas_permission = azureblob.BlobSasPermissions(read=True, create=True, add=True, write=True)

    # Obtain a shared access signature URL that provides write access to the output
    # container to which the tasks will upload their output.

    output_container_sas_url = get_container_sas_url(
        blob_service_client,
        _STORAGE_ACCOUNT_NAME,
        output_container_name,
        sas_permission)
    
    credentials = batchauth.SharedKeyCredentials(_BATCH_ACCOUNT_NAME,
                                                 _BATCH_ACCOUNT_KEY)


    # Create a Batch service client.
    batch_client = azurebatch.BatchServiceClient(
        credentials,
        batch_url=_BATCH_ACCOUNT_URL)


    try:

        # Create the job that will run the tasks.
        create_job(batch_client, _JOB_ID, _POOL_ID)

        # Add the tasks to the job. Pass the input files and a SAS URL
        # to the storage container for output files.
        add_tasks_ndvi(batch_client, _JOB_ID, items, output_container_sas_url, name, satellite, post_processing)
    
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

