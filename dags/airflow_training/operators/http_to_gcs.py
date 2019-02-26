from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import io


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ('endpoint',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 gcs_path,
                 bucket,
                 http_conn_id="currency_con",
                 google_cloud_storage_conn_id="google_cloud_default",
                 delegate_to=None,
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.gcs_path = gcs_path
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.bucket = bucket
        self.delegate_to = delegate_to

    def execute(self, context):
        http = HttpHook(
            method="GET",
            http_conn_id=self.http_conn_id
        )
        conversion = http.run(
            endpoint=self.endpoint
        )
        self._upload_to_gcs(conversion)

    def _upload_to_gcs(self, file_to_upload):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )
        hook.upload(self.bucket, self.gcs_path + io.BytesIO(str(file_to_upload.content)), \
                    "application/json")
