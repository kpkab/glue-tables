from fastapi import FastAPI
from dotenv import dotenv_values
import boto3
import botocore
from model import Glue_table
from model import SuccessResponse, ErrorResponse, ExceptionResponse

config = dotenv_values(".env")
ACCESS_ID = config.get("aws_access_key_id")
ACCESS_KEY = config.get("aws_secret_access_key")
REGION = config.get("aws_region")


client = boto3.client('glue', aws_access_key_id=ACCESS_ID,
                      aws_secret_access_key=ACCESS_KEY, region_name=REGION)

app = FastAPI(openapi_url="/table/openapi.json",docs_url="/table/docs")

@app.post("/table/create_table")
async def create_table(glue:Glue_table):
    """
    This endpoint creates a Glue table for glue databse.
    """
    try:
        response = client.create_table(
            DatabaseName =glue.DatabaseName,         
            TableInput={
                'Name':glue.Name,   
                'StorageDescriptor':{
                    'Location':glue.Location
            }
            },
            
           )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'AlreadyExistsException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ConcurrentModificationException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNotReadyException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.get("/table/get_tables/{DatabaseName}")
async def get_tables(DatabaseName: str):
    """
    This endpoint return the Glue table for glue databse.
    """
    try:
        response = client.get_tables(
            DatabaseName=DatabaseName
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'],data=response['TableList'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.get("/table/get_table/{Name}/{DatabaseName}")
async def get_table(Name: str, DatabaseName: str):
    """
    This endpoint returns specific Glue table for glue databse.
    """
    try:
        response = client.get_table(
            DatabaseName=DatabaseName,
            Name=Name
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response['Table'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNotReadyException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.get("/table/search_table/{SearchText}")
async def search_table(SearchText: str):
    """
    This endpoint search the Glue tables.
    """
    try:
        response = client.search_tables(
            SearchText=SearchText
        )

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response['TableList'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()

@app.delete("/table/delete_table/{Name}/{DatabaseName}")
async def delete_table(Name: str, DatabaseName: str): 
    """
    This endpoint delete Glue table from glue databse.
    """
    try:
        response = client.delete_table(
            DatabaseName=DatabaseName,
            Name=Name
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ConcurrentModificationException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNotReadyException':
                return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
                return ExceptionResponse()
    except Exception as e:
        return ExceptionResponse()
    
@app.put("/table/update_table")
async def update_table(glue:Glue_table):
    """
    This endpoint Update the existing Glue table.
    """
    try:    
        response = client.update_table(
            DatabaseName =glue.DatabaseName,         
            TableInput={
                'Name':glue.Name,   
                'StorageDescriptor':{
                    'Location':glue.Location
            }
            }
           )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return SuccessResponse(status=response['ResponseMetadata']['HTTPStatusCode'])
        else:
            return ErrorResponse(status=response['ResponseMetadata']['HTTPStatusCode'], data=response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InvalidInputException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'InternalServiceException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'OperationTimeoutException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ConcurrentModificationException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNumberLimitExceededException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'GlueEncryptionException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        if error.response['Error']['Code'] == 'ResourceNotReadyException':
            return ExceptionResponse(status=error.response['ResponseMetadata']['HTTPStatusCode'], message=error.response['Error'])
        else:
            return ExceptionResponse()
    except Exception as e:
        print('error', e)
        return ExceptionResponse()
