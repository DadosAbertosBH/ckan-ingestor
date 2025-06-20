# Pedalin
# Copyright (C) 2025  Pedalin

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import json
from io import BytesIO

import requests
from minio import Minio

from ckan_ingestor.config.s3_settings import S3Settings


class S3DocumentIngestor:
    """
    Ingestor for PDF documents into an S3 bucket using Minio.
    """
    minio: Minio
    bucket: str
    public_url: str

    def __init__(self, s3_settings: S3Settings):
        protocol = "https" if s3_settings.use_ssl else "http"
        self.public_url = f"{protocol}://{s3_settings.endpoint}/{s3_settings.bucket}"
        self.bucket = s3_settings.bucket
        self.minio = Minio(
            endpoint=s3_settings.endpoint,
            access_key=s3_settings.access_key_id,
            secret_key=s3_settings.secret_access_key,
            secure=s3_settings.use_ssl,
        )
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                    ],
                    "Resource": f"arn:aws:s3:::{self.bucket}/docs/*",
                },
            ],
        }
        self.minio.set_bucket_policy(self.bucket, json.dumps(policy))

    def ingest(self, filename: str, download_url: str, content_type):
        with requests.get(
            download_url,
            stream=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0"
            },
        ) as response:
            response.raise_for_status()
            pdf_bytes = response.content
            object_name = f"/docs/{filename}"
            data = BytesIO(response.content)
            # Upload the object to Minio
            self.minio.put_object(
                bucket_name=self.bucket,
                object_name=object_name,
                data=data,
                length=len(pdf_bytes),
                content_type=content_type,
            )

            return self.public_url + object_name
