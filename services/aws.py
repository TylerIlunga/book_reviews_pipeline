import pandas as pd


class AWSService:
    def __init__(self):
        print("Instantiating AWSS3Service")

    def pull_data_from_s3_bucket(self, bucket, data_key, file):
        print("pull_data_from_bucket()")
        print("bucket, data_key", bucket, data_key)
        data_location = self._get_s3_data_location_uri(bucket, data_key, file)
        print("data_location:", data_location)
        return pd.read_csv(data_location, error_bad_lines=False, encoding='latin1', delimiter=";")

    def _get_s3_data_location_uri(self, bucket, data_key, file):
        print("_get_data_location_uri()")
        # return './csv/{}'.format(str(file))  # LOCAL
        return 's3://{}/{}'.format(bucket, data_key) # PROD
